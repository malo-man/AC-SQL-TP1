"""Étape B/C/D : agrégation des deux sources dans la zone « aggregated » du Data Lake.

Le job lit les deux zones brutes, nettoie les données TMDB, les rapproche des
notes IMDb et écrit un instantané complet en Parquet :

    <DATALAKE_DIR>/aggregated/movies/ingest_date=<date du run>/*.parquet

Logique métier du rapprochement : TMDB fournit le catalogue et sa propre note,
IMDb fournit une seconde notation, issue d'une population de votants différente.
Les deux sources partagent l'identifiant IMDb (`movies.imdb_id` = `tconst`), ce
qui permet de comparer les deux réceptions d'un même film.

Le contenu écrit est un instantané complet et non un différentiel : une nouvelle
exécution le même jour remplace la partition du jour, les journées précédentes
restent intactes (partitionOverwriteMode=dynamic).

Lancement : `python aggregate.py`.
"""

import sys
from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import LoadRun, Settings, build_session, has_data, latest_partition, load_settings, log, setup_logging
from schemas import ENVELOPE, TITLE_BASICS, TITLE_RATINGS

JOB = "aggregate"


def blank_to_null(column: Column) -> Column:
    """TMDB renvoie une chaîne vide pour un champ non renseigné : c'est un NULL."""
    trimmed = F.trim(column)
    return F.when(trimmed == "", None).otherwise(trimmed)


def positive_or_null(column: Column) -> Column:
    """Chez TMDB, 0 signifie « inconnu » pour un budget, une recette ou une durée."""
    return F.when(column > 0, column)


def read_raw_tmdb(spark: SparkSession, settings: Settings) -> DataFrame:
    """Lit les messages bruts déposés par le writer, avec un schéma imposé."""
    return spark.read.schema(ENVELOPE).json(str(settings.raw_tmdb))


def read_imdb_ratings(spark: SparkSession, settings: Settings) -> DataFrame | None:
    """Note moyenne et nombre de votes IMDb, depuis la dernière partition collectée."""
    partition = latest_partition(settings.raw_imdb / "title.ratings")
    if partition is None:
        return None
    log.info("Notes IMDb lues depuis %s", partition.name)
    frame = (
        spark.read.option("sep", "\t")
        .option("header", True)
        # Convention IMDb pour une valeur absente
        .option("nullValue", "\\N")
        .schema(TITLE_RATINGS)
        .csv(str(partition))
    )
    return frame.select(
        F.col("tconst"),
        F.col("averageRating").cast("double").alias("imdb_average_rating"),
        F.col("numVotes").cast("int").alias("imdb_num_votes"),
    ).dropDuplicates(["tconst"])


def read_imdb_basics(spark: SparkSession, settings: Settings) -> DataFrame | None:
    """Métadonnées IMDb (année, durée, genres). Dataset optionnel : 228 Mo.

    Absent du profil léger, d'où la lecture conditionnelle : le pipeline doit
    tourner avec les seules notes, qui portent l'essentiel de la valeur métier.
    """
    partition = latest_partition(settings.raw_imdb / "title.basics")
    if partition is None:
        return None
    log.info("Métadonnées IMDb lues depuis %s", partition.name)
    frame = (
        spark.read.option("sep", "\t")
        .option("header", True)
        .option("nullValue", "\\N")
        .schema(TITLE_BASICS)
        .csv(str(partition))
    )
    return (
        # Filtre appliqué avant toute jointure : 12 millions de titres (séries,
        # épisodes, courts métrages) ramenés aux seuls longs métrages.
        frame.where(F.col("titleType") == "movie")
        .select(
            F.col("tconst"),
            F.col("startYear").cast("short").alias("imdb_start_year"),
            F.col("runtimeMinutes").cast("int").alias("imdb_runtime_minutes"),
            F.split(F.col("genres"), ",").alias("imdb_genres"),
        )
        .dropDuplicates(["tconst"])
    )


def deduplicate(raw: DataFrame) -> DataFrame:
    """Ne garde que le dernier état connu de chaque film.

    Le producer reboucle sur les listes TMDB : un même film est donc collecté
    plusieurs fois, et le writer peut relivrer des messages déjà écrits après un
    redémarrage. On ordonne par date de collecte et on garde le plus récent.
    """
    latest = Window.partitionBy("movie_id").orderBy(F.col("fetched_at_ts").desc())
    return (
        raw.withColumn("fetched_at_ts", F.to_timestamp("fetched_at"))
        .withColumn("_rank", F.row_number().over(latest))
        .where(F.col("_rank") == 1)
        .drop("_rank")
    )


def clean_movies(deduplicated: DataFrame) -> DataFrame:
    """Applique les conversions de types et la normalisation des valeurs."""
    payload = F.col("payload")
    return deduplicated.select(
        payload["id"].alias("id"),
        blank_to_null(payload["imdb_id"]).alias("imdb_id"),
        F.trim(payload["title"]).alias("title"),
        blank_to_null(payload["original_title"]).alias("original_title"),
        F.lower(blank_to_null(payload["original_language"])).alias("original_language"),
        blank_to_null(payload["overview"]).alias("overview"),
        blank_to_null(payload["tagline"]).alias("tagline"),
        blank_to_null(payload["status"]).alias("status"),
        F.to_date(blank_to_null(payload["release_date"])).alias("release_date"),
        positive_or_null(payload["runtime"]).alias("runtime"),
        positive_or_null(payload["budget"]).alias("budget"),
        positive_or_null(payload["revenue"]).alias("revenue"),
        payload["popularity"].alias("popularity"),
        payload["vote_average"].alias("vote_average"),
        payload["vote_count"].alias("vote_count"),
        F.coalesce(payload["adult"], F.lit(False)).alias("adult"),
        blank_to_null(payload["homepage"]).alias("homepage"),
        payload["poster_path"].alias("poster_path"),
        payload["backdrop_path"].alias("backdrop_path"),
        # Structures imbriquées conservées telles quelles : elles seront
        # éclatées en tables de liaison par le job de chargement.
        payload["belongs_to_collection"].alias("collection"),
        payload["genres"].alias("genres"),
        payload["production_companies"].alias("production_companies"),
        payload["production_countries"].alias("production_countries"),
        payload["spoken_languages"].alias("spoken_languages"),
        payload["credits"].alias("credits"),
        F.col("fetched_at_ts").alias("source_fetched_at"),
    )


def enrich_with_imdb(
    movies: DataFrame, ratings: DataFrame | None, basics: DataFrame | None
) -> DataFrame:
    """Rapproche chaque film de ses données IMDb via l'identifiant partagé."""
    enriched = movies
    if ratings is not None:
        enriched = enriched.join(ratings, enriched["imdb_id"] == ratings["tconst"], "left").drop(
            "tconst"
        )
    else:
        log.warning("Aucune note IMDb dans le lac : enrichissement laissé vide")
        enriched = enriched.withColumn(
            "imdb_average_rating", F.lit(None).cast("double")
        ).withColumn("imdb_num_votes", F.lit(None).cast("int"))

    if basics is not None:
        enriched = enriched.join(basics, enriched["imdb_id"] == basics["tconst"], "left").drop(
            "tconst"
        )
    else:
        log.info("Dataset title.basics absent (profil léger) : colonnes laissées vides")
        enriched = (
            enriched.withColumn("imdb_start_year", F.lit(None).cast("short"))
            .withColumn("imdb_runtime_minutes", F.lit(None).cast("int"))
            .withColumn("imdb_genres", F.lit(None).cast("array<string>"))
        )

    return (
        enriched.withColumn("has_imdb_match", F.col("imdb_average_rating").isNotNull())
        # Écart de notation : positif quand TMDB note mieux qu'IMDb
        .withColumn("rating_gap", F.round(F.col("vote_average") - F.col("imdb_average_rating"), 2))
        .withColumn(
            "votes_ratio",
            F.when(
                F.col("imdb_num_votes") > 0,
                F.round(F.col("vote_count") / F.col("imdb_num_votes"), 4),
            ),
        )
    )


def main() -> int:
    setup_logging()
    settings = load_settings()

    with LoadRun(settings, JOB) as run:
        if not has_data(settings.raw_tmdb):
            # Cas normal au tout premier démarrage : la collecte n'a pas encore
            # produit de fichier. On sort proprement, sans faire boucler le service.
            log.warning("Aucune donnée TMDB dans %s, rien à agréger", settings.raw_tmdb)
            run.skip("zone raw TMDB vide")
            return 0

        run_date = datetime.now(UTC).date().isoformat()
        run.ingest_date = run_date
        spark = build_session(f"tp2-{JOB}")
        try:
            raw = read_raw_tmdb(spark, settings)
            rows_in = raw.count()

            # Une ligne sans identifiant ou sans titre est inexploitable en aval :
            # on la compte et on l'écarte, plutôt que de faire échouer le job.
            usable = raw.where(
                F.col("payload.id").isNotNull()
                & (F.trim(F.coalesce(F.col("payload.title"), F.lit(""))) != "")
            )
            rejected = rows_in - usable.count()

            movies = clean_movies(deduplicate(usable))
            enriched = enrich_with_imdb(
                movies, read_imdb_ratings(spark, settings), read_imdb_basics(spark, settings)
            ).withColumn("ingest_date", F.lit(run_date))
            enriched.cache()

            clean_rows = enriched.count()
            matched = enriched.where(F.col("has_imdb_match")).count()

            enriched.write.mode("overwrite").partitionBy("ingest_date").parquet(
                str(settings.aggregated)
            )
            log.info(
                "%d lignes brutes -> %d films (%d rapprochés d'IMDb, %d rejetés) dans %s",
                rows_in,
                clean_rows,
                matched,
                rejected,
                settings.aggregated,
            )
            run.succeed(
                raw_rows_in=rows_in,
                distinct_movies_in=clean_rows,
                clean_rows_out=clean_rows,
                rejected_rows=rejected,
                imdb_matched=matched,
            )
        finally:
            spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
