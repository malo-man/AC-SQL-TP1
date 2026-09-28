"""Étape E : chargement des données propres dans le schéma mart de PostgreSQL.

Le job lit le dernier instantané de la zone « aggregated », éclate les
structures imbriquées en tables (le miroir du modèle du TP1) puis charge le
tout en deux temps :

1. Spark écrit chaque table dans sa table de transit `mart._stg_<table>`, en
   mode overwrite + truncate : la table est vidée, jamais supprimée, donc ses
   types restent ceux définis dans la migration ;
2. un UPSERT SQL recopie le transit vers la table cible, dans une seule
   transaction et dans l'ordre des clés étrangères ; dans la même transaction,
   les liaisons qu'un film a perdues chez TMDB sont retirées, puis les
   référentiels et personnes qui ne sont plus reliés à aucun film.

Pourquoi pas un simple `write.jdbc(mode="overwrite")` : il supprime puis
recrée la table, et emporte avec elle clés primaires, clés étrangères, index
et contraintes. L'UPSERT préserve tout cela et rend le chargement rejouable.

Lancement : `python load_mart.py`.
"""

import sys

import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (
    LoadRun,
    Settings,
    blank_to_null,
    build_session,
    latest_partition,
    load_settings,
    log,
    setup_logging,
    pipeline_lock,
)

JOB = "load_mart"

# Ordre de chargement : les référentiels d'abord, puis les films, puis les
# tables de liaison — sinon les clés étrangères refusent l'insertion.
# (table, colonnes écrites, clé primaire)
TABLES: list[tuple[str, list[str], list[str]]] = [
    ("genres", ["id", "name"], ["id"]),
    ("collections", ["id", "name", "poster_path", "backdrop_path"], ["id"]),
    ("production_companies", ["id", "name", "origin_country", "logo_path"], ["id"]),
    ("countries", ["iso_3166_1", "name"], ["iso_3166_1"]),
    ("languages", ["iso_639_1", "name", "english_name"], ["iso_639_1"]),
    (
        "people",
        [
            "id", "name", "original_name", "gender", "known_for_department",
            "popularity", "profile_path",
        ],
        ["id"],
    ),
    (
        "movies",
        [
            "id", "imdb_id", "title", "original_title", "original_language", "overview",
            "tagline", "status", "release_date", "runtime", "budget", "revenue", "popularity",
            "vote_average", "vote_count", "adult", "homepage", "poster_path", "backdrop_path",
            "collection_id", "imdb_average_rating", "imdb_num_votes", "imdb_start_year",
            "imdb_runtime_minutes", "imdb_genres", "has_imdb_match", "rating_gap",
            "votes_ratio", "source_fetched_at", "ingest_date",
        ],
        ["id"],
    ),
    ("movie_genres", ["movie_id", "genre_id"], ["movie_id", "genre_id"]),
    ("movie_production_companies", ["movie_id", "company_id"], ["movie_id", "company_id"]),
    ("movie_production_countries", ["movie_id", "country_id"], ["movie_id", "country_id"]),
    ("movie_spoken_languages", ["movie_id", "language_id"], ["movie_id", "language_id"]),
    (
        "movie_cast",
        ["credit_id", "movie_id", "person_id", "character", "cast_order"],
        ["credit_id"],
    ),
    ("movie_crew", ["credit_id", "movie_id", "person_id", "department", "job"], ["credit_id"]),
]


def read_snapshot(spark: SparkSession, settings: Settings) -> DataFrame | None:
    """Lit le dernier instantané produit par l'agrégation."""
    partition = latest_partition(settings.aggregated)
    if partition is None:
        return None
    log.info("Instantané lu depuis %s", partition.name)
    # La colonne de partition n'existe pas dans les fichiers : on la rétablit
    # depuis le nom du répertoire, pour tracer d'où vient chaque ligne.
    ingest_date = partition.name.split("=", 1)[1]
    return spark.read.parquet(str(partition)).withColumn(
        "ingest_date", F.lit(ingest_date).cast("date")
    )


# Tables de liaison et relations porteuses : l'état d'un film rechargé est
# complet dans le transit, ce qui n'y figure plus a disparu chez TMDB.
LINK_TABLES = [name for name, _, _ in TABLES if name.startswith("movie_")]

# Référentiels et personnes n'existent que par un film (cardinalité (1,N) du
# MCD) : sans liaison, ils sont orphelins. (table, condition d'orphelin)
ORPHANS: list[tuple[str, str]] = [
    ("genres", "NOT EXISTS (SELECT 1 FROM mart.movie_genres l WHERE l.genre_id = t.id)"),
    ("collections", "NOT EXISTS (SELECT 1 FROM mart.movies m WHERE m.collection_id = t.id)"),
    (
        "production_companies",
        "NOT EXISTS (SELECT 1 FROM mart.movie_production_companies l WHERE l.company_id = t.id)",
    ),
    (
        "countries",
        "NOT EXISTS (SELECT 1 FROM mart.movie_production_countries l"
        " WHERE l.country_id = t.iso_3166_1)",
    ),
    (
        "languages",
        "NOT EXISTS (SELECT 1 FROM mart.movie_spoken_languages l WHERE l.language_id = t.iso_639_1)",
    ),
    (
        "people",
        "NOT EXISTS (SELECT 1 FROM mart.movie_cast c WHERE c.person_id = t.id)"
        " AND NOT EXISTS (SELECT 1 FROM mart.movie_crew c WHERE c.person_id = t.id)",
    ),
]


def code(column: str, length: int = 2) -> F.Column:
    """Normalise un code ISO : majuscules et longueur fixe attendue par la base.

    Un code vide devient NULL : stocké tel quel en CHAR(2), il passerait pour
    renseigné (contrôle VAL-09 du TP3).
    """
    return blank_to_null(F.substring(F.upper(F.trim(F.col(column))), 1, length))


def first_per(frame: DataFrame, keys: list[str], order: list[F.Column]) -> DataFrame:
    """Garde une ligne par valeur de `keys`, la première selon `order`."""
    window = Window.partitionBy(*keys).orderBy(*order)
    return frame.withColumn("_rank", F.row_number().over(window)).where("_rank = 1").drop("_rank")


def build_tables(snapshot: DataFrame) -> dict[str, DataFrame]:
    """Éclate l'instantané en une table par entité et par relation du modèle."""
    movies = snapshot.select(
        "id", "imdb_id", "title", "original_title",
        F.substring(F.col("original_language"), 1, 2).alias("original_language"),
        "overview", "tagline", "status", "release_date", "runtime", "budget", "revenue",
        "popularity", "vote_average", "vote_count", "adult", "homepage", "poster_path",
        "backdrop_path",
        F.col("collection.id").alias("collection_id"),
        "imdb_average_rating", "imdb_num_votes", "imdb_start_year", "imdb_runtime_minutes",
        "imdb_genres", "has_imdb_match", "rating_gap", "votes_ratio",
        "source_fetched_at", "ingest_date",
    )

    genres = snapshot.select(F.explode("genres").alias("g")).select(
        F.col("g.id").alias("id"), F.trim(F.col("g.name")).alias("name")
    )
    companies = snapshot.select(F.explode("production_companies").alias("c")).select(
        F.col("c.id").alias("id"),
        F.trim(F.col("c.name")).alias("name"),
        code("c.origin_country").alias("origin_country"),
        F.col("c.logo_path").alias("logo_path"),
    )
    countries = snapshot.select(F.explode("production_countries").alias("c")).select(
        code("c.iso_3166_1").alias("iso_3166_1"),
        F.trim(F.col("c.name")).alias("name"),
    )
    languages = snapshot.select(F.explode("spoken_languages").alias("l")).select(
        F.substring(F.lower(F.trim(F.col("l.iso_639_1"))), 1, 2).alias("iso_639_1"),
        blank_to_null(F.col("l.name")).alias("name"),
        blank_to_null(F.col("l.english_name")).alias("english_name"),
    )
    collections = snapshot.where(F.col("collection.id").isNotNull()).select(
        F.col("collection.id").alias("id"),
        F.trim(F.col("collection.name")).alias("name"),
        F.col("collection.poster_path").alias("poster_path"),
        F.col("collection.backdrop_path").alias("backdrop_path"),
    )

    # Casting et équipe technique : mêmes personnes, attributs de relation différents
    cast = snapshot.select("id", "source_fetched_at", F.explode("credits.cast").alias("p"))
    crew = snapshot.select("id", "source_fetched_at", F.explode("credits.crew").alias("p"))

    def person_columns(frame: DataFrame) -> DataFrame:
        return frame.select(
            F.col("p.id").alias("id"),
            F.trim(F.col("p.name")).alias("name"),
            blank_to_null(F.col("p.original_name")).alias("original_name"),
            # 0 est la valeur TMDB de « non renseigné » : une fausse catégorie
            # dans toute répartition par genre (contrôle VAL-13 du TP3)
            F.when(F.col("p.gender").isin(1, 2, 3), F.col("p.gender").cast("short")).alias("gender"),
            blank_to_null(F.col("p.known_for_department")).alias("known_for_department"),
            F.col("p.popularity").alias("popularity"),
            F.col("p.profile_path").alias("profile_path"),
            F.col("source_fetched_at"),
        )

    # Une même personne apparaît dans plusieurs films : on garde la version
    # issue de la collecte la plus récente (une seule entité PERSONNE, comme au TP1).
    freshest = Window.partitionBy("id").orderBy(
        F.col("source_fetched_at").desc(), F.col("popularity").desc_nulls_last()
    )
    people = (
        person_columns(cast)
        .unionByName(person_columns(crew))
        .where(F.col("id").isNotNull() & F.col("name").isNotNull())
        .withColumn("_rank", F.row_number().over(freshest))
        .where(F.col("_rank") == 1)
        .drop("_rank", "source_fetched_at")
    )

    tables = {
        "genres": genres,
        "collections": collections,
        "production_companies": companies,
        "countries": countries,
        "languages": languages,
        "people": people,
        "movies": movies,
        "movie_genres": snapshot.select(
            F.col("id").alias("movie_id"), F.explode("genres.id").alias("genre_id")
        ),
        "movie_production_companies": snapshot.select(
            F.col("id").alias("movie_id"), F.explode("production_companies.id").alias("company_id")
        ),
        "movie_production_countries": snapshot.select(
            F.col("id").alias("movie_id"),
            F.explode("production_countries.iso_3166_1").alias("country_id"),
        ).withColumn("country_id", code("country_id")),
        "movie_spoken_languages": snapshot.select(
            F.col("id").alias("movie_id"),
            F.explode("spoken_languages.iso_639_1").alias("language_id"),
        ).withColumn("language_id", F.substring(F.lower(F.col("language_id")), 1, 2)),
        # Un même rôle ou un même poste peut être crédité deux fois sous deux
        # credit_id : on n'en garde qu'un (contrôles UNI-04 et UNI-05 du TP3).
        "movie_cast": first_per(
            cast.select(
                F.col("p.credit_id").alias("credit_id"),
                F.col("id").alias("movie_id"),
                F.col("p.id").alias("person_id"),
                blank_to_null(F.col("p.character")).alias("character"),
                F.col("p.order").alias("cast_order"),
            ),
            ["movie_id", "person_id", "character"],
            [F.col("cast_order").asc_nulls_last(), F.col("credit_id")],
        ),
        "movie_crew": first_per(
            crew.select(
                F.col("p.credit_id").alias("credit_id"),
                F.col("id").alias("movie_id"),
                F.col("p.id").alias("person_id"),
                blank_to_null(F.col("p.department")).alias("department"),
                blank_to_null(F.col("p.job")).alias("job"),
            ),
            ["movie_id", "person_id", "job"],
            [F.col("credit_id")],
        ),
    }

    # Chaque table est dédoublonnée sur sa clé : un genre revient à chaque film,
    # un crédit peut être relivré par la source.
    deduplicated = {}
    for name, columns, primary_key in TABLES:
        frame = tables[name]
        for key in primary_key:
            frame = frame.where(F.col(key).isNotNull())
        deduplicated[name] = frame.select(*columns).dropDuplicates(primary_key)
    return deduplicated


def write_staging(frame: DataFrame, table: str, settings: Settings) -> None:
    """Vide puis remplit la table de transit correspondante."""
    (
        frame.write.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", f"mart._stg_{table}")
        # truncate : la table est vidée, pas supprimée, donc ses types survivent
        .option("truncate", "true")
        .options(**settings.jdbc_properties)
        .mode("overwrite")
        .save()
    )


def upsert_sql(table: str, columns: list[str], primary_key: list[str]) -> str:
    """Construit l'UPSERT du transit vers la table cible.

    Les tables de liaison n'ont que des colonnes de clé : il n'y a rien à mettre
    à jour quand le lien existe déjà, d'où le DO NOTHING et un compte de lignes
    écrites nul aux exécutions suivantes.
    """
    quoted = ", ".join(f'"{c}"' for c in columns)
    conflict = ", ".join(f'"{c}"' for c in primary_key)
    updates = [f'"{c}" = EXCLUDED."{c}"' for c in columns if c not in primary_key]
    if table == "movies":
        # Trace de la dernière écriture, utile pour la fraîcheur du dashboard
        updates.append('"loaded_at" = now()')
    action = f"DO UPDATE SET {', '.join(updates)}" if updates else "DO NOTHING"
    return (
        f"INSERT INTO mart.{table} ({quoted}) SELECT {quoted} FROM mart._stg_{table} "
        f"ON CONFLICT ({conflict}) {action}"
    )


def purge_stale_links_sql(table: str, primary_key: list[str]) -> str:
    """Retire les liaisons des films rechargés qui ont disparu de la source.

    L'UPSERT ne fait qu'ajouter : un genre reclassé ou un crédit supprimé chez
    TMDB resterait en base (contrôle COH-09 du TP3). Seuls les films présents
    dans le transit sont concernés, leur état y étant complet.
    """
    match = " AND ".join(f's."{c}" = t."{c}"' for c in primary_key)
    return (
        f"DELETE FROM mart.{table} AS t "
        f"WHERE t.movie_id IN (SELECT id FROM mart._stg_movies) "
        f"AND NOT EXISTS (SELECT 1 FROM mart._stg_{table} AS s WHERE {match})"
    )


def apply_upserts(settings: Settings) -> tuple[dict[str, int], dict[str, int]]:
    """Recopie toutes les tables de transit vers les tables cibles, en une transaction.

    Renvoie les lignes écrites par table, puis les lignes retirées : liaisons
    obsolètes, puis référentiels et personnes devenus orphelins.
    """
    written: dict[str, int] = {}
    removed: dict[str, int] = {}
    keys = {table: primary_key for table, _, primary_key in TABLES}
    with psycopg.connect(settings.conninfo) as conn, conn.cursor() as cur:
        for table, columns, primary_key in TABLES:
            cur.execute(upsert_sql(table, columns, primary_key))
            written[table] = cur.rowcount
        for table in LINK_TABLES:
            cur.execute(purge_stale_links_sql(table, keys[table]))
            removed[table] = cur.rowcount
        # Après les liaisons : leur retrait est ce qui rend un référentiel orphelin
        for table, orphan in ORPHANS:
            cur.execute(f"DELETE FROM mart.{table} AS t WHERE {orphan}")
            removed[table] = cur.rowcount
        conn.commit()
    return written, {table: count for table, count in removed.items() if count}


def main() -> int:
    setup_logging()
    settings = load_settings()

    with LoadRun(settings, JOB) as run, pipeline_lock(settings, JOB) as acquired:
        if not acquired:
            log.warning("Un autre job du pipeline est en cours, %s est ignoré", JOB)
            run.skip("exécution concurrente")
            return 0

        spark = build_session(f"tp2-{JOB}")
        try:
            snapshot = read_snapshot(spark, settings)
            if snapshot is None:
                log.warning("Aucun instantané dans %s, rien à charger", settings.aggregated)
                run.skip("zone aggregated vide")
                return 0

            snapshot.cache()
            rows_in = snapshot.count()
            run.ingest_date = snapshot.select("ingest_date").first()[0].isoformat()

            tables = build_tables(snapshot)
            for table, _, _ in TABLES:
                write_staging(tables[table], table, settings)
            log.info("%d tables de transit remplies", len(TABLES))

            written, removed = apply_upserts(settings)
            detail = ", ".join(f"{name}={count}" for name, count in written.items())
            if removed:
                detail += " ; retirées : " + ", ".join(f"{name}={n}" for name, n in removed.items())
            log.info("Chargement terminé, lignes insérées ou mises à jour : %s", detail)
            run.succeed(
                message=detail[:500],
                raw_rows_in=rows_in,
                distinct_movies_in=rows_in,
                clean_rows_out=written["movies"],
                rejected_rows=0,
                imdb_matched=snapshot.where(F.col("has_imdb_match")).count(),
            )
        finally:
            spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
