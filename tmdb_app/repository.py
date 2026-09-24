"""Accès à PostgreSQL : écriture des films TMDB et requêtes de lecture."""

from psycopg import Connection
from psycopg.rows import class_row

from tmdb_app.models import (
    CastRow,
    GenreStat,
    MovieDetailRow,
    MovieDetails,
    MovieRow,
    Person,
    TableCount,
)

# =====================================================================
# Écriture
# =====================================================================


def save_movie(conn: Connection, movie: MovieDetails) -> None:
    """Enregistre (ou met à jour) un film et toutes ses relations dans une transaction."""
    with conn.transaction(), conn.cursor() as cur:
        collection = movie.belongs_to_collection
        if collection:
            cur.execute(
                """
                INSERT INTO collections (id, name, poster_path, backdrop_path)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                   SET name = EXCLUDED.name,
                       poster_path = EXCLUDED.poster_path,
                       backdrop_path = EXCLUDED.backdrop_path
                """,
                (
                    collection.id,
                    collection.name,
                    collection.poster_path,
                    collection.backdrop_path,
                ),
            )

        cur.execute(
            """
            INSERT INTO movies (
                id, imdb_id, title, original_title, original_language, overview,
                tagline, status, release_date, runtime, budget, revenue, popularity,
                vote_average, vote_count, adult, homepage, poster_path, backdrop_path,
                collection_id, fetched_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (id) DO UPDATE SET
                imdb_id = EXCLUDED.imdb_id,
                title = EXCLUDED.title,
                original_title = EXCLUDED.original_title,
                original_language = EXCLUDED.original_language,
                overview = EXCLUDED.overview,
                tagline = EXCLUDED.tagline,
                status = EXCLUDED.status,
                release_date = EXCLUDED.release_date,
                runtime = EXCLUDED.runtime,
                budget = EXCLUDED.budget,
                revenue = EXCLUDED.revenue,
                popularity = EXCLUDED.popularity,
                vote_average = EXCLUDED.vote_average,
                vote_count = EXCLUDED.vote_count,
                adult = EXCLUDED.adult,
                homepage = EXCLUDED.homepage,
                poster_path = EXCLUDED.poster_path,
                backdrop_path = EXCLUDED.backdrop_path,
                collection_id = EXCLUDED.collection_id,
                fetched_at = now()
            """,
            (
                movie.id,
                movie.imdb_id,
                movie.title,
                movie.original_title,
                movie.original_language,
                movie.overview,
                movie.tagline or None,
                movie.status,
                movie.release_date,
                movie.runtime,
                movie.budget,
                movie.revenue,
                movie.popularity,
                movie.vote_average,
                movie.vote_count,
                movie.adult,
                movie.homepage or None,
                movie.poster_path,
                movie.backdrop_path,
                collection.id if collection else None,
            ),
        )

        # Référentiels : insertion ou mise à jour du libellé
        cur.executemany(
            "INSERT INTO genres (id, name) VALUES (%s, %s) "
            "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            [(g.id, g.name) for g in movie.genres],
        )
        cur.executemany(
            """
            INSERT INTO production_companies (id, name, origin_country, logo_path)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
               SET name = EXCLUDED.name,
                   origin_country = EXCLUDED.origin_country,
                   logo_path = EXCLUDED.logo_path
            """,
            [
                (c.id, c.name, c.origin_country, c.logo_path)
                for c in movie.production_companies
            ],
        )
        cur.executemany(
            "INSERT INTO countries (iso_3166_1, name) VALUES (%s, %s) "
            "ON CONFLICT (iso_3166_1) DO UPDATE SET name = EXCLUDED.name",
            [(c.iso_3166_1, c.name) for c in movie.production_countries],
        )
        cur.executemany(
            """
            INSERT INTO languages (iso_639_1, name, english_name) VALUES (%s, %s, %s)
            ON CONFLICT (iso_639_1) DO UPDATE
               SET name = EXCLUDED.name, english_name = EXCLUDED.english_name
            """,
            [
                (lang.iso_639_1, lang.name, lang.english_name)
                for lang in movie.spoken_languages
            ],
        )
        _upsert_people(conn, [*movie.credits.cast, *movie.credits.crew])

        # Liaisons : on repart de zéro pour refléter l'état actuel de TMDB
        for table in (
            "movie_genres",
            "movie_production_companies",
            "movie_production_countries",
            "movie_spoken_languages",
            "movie_cast",
            "movie_crew",
        ):
            cur.execute(f"DELETE FROM {table} WHERE movie_id = %s", (movie.id,))

        cur.executemany(
            "INSERT INTO movie_genres (movie_id, genre_id) VALUES (%s, %s)",
            [(movie.id, g.id) for g in movie.genres],
        )
        cur.executemany(
            "INSERT INTO movie_production_companies (movie_id, company_id) VALUES (%s, %s)",
            [(movie.id, c.id) for c in movie.production_companies],
        )
        cur.executemany(
            "INSERT INTO movie_production_countries (movie_id, country_id) VALUES (%s, %s)",
            [(movie.id, c.iso_3166_1) for c in movie.production_countries],
        )
        cur.executemany(
            "INSERT INTO movie_spoken_languages (movie_id, language_id) VALUES (%s, %s)",
            [(movie.id, lang.iso_639_1) for lang in movie.spoken_languages],
        )
        cur.executemany(
            """
            INSERT INTO movie_cast (credit_id, movie_id, person_id, character, cast_order)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (credit_id) DO NOTHING
            """,
            [
                (c.credit_id, movie.id, c.id, c.character, c.order)
                for c in movie.credits.cast
            ],
        )
        cur.executemany(
            """
            INSERT INTO movie_crew (credit_id, movie_id, person_id, department, job)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (credit_id) DO NOTHING
            """,
            [
                (c.credit_id, movie.id, c.id, c.department, c.job)
                for c in movie.credits.crew
            ],
        )


def _upsert_people(conn: Connection, people: list[Person]) -> None:
    """Insère les personnes du casting/équipe en dédoublonnant par id TMDB."""
    unique = {p.id: p for p in people}.values()
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO people (id, name, original_name, gender, known_for_department,
                                popularity, profile_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                original_name = EXCLUDED.original_name,
                gender = EXCLUDED.gender,
                known_for_department = EXCLUDED.known_for_department,
                popularity = EXCLUDED.popularity,
                profile_path = EXCLUDED.profile_path
            """,
            [
                (
                    p.id,
                    p.name,
                    p.original_name,
                    p.gender,
                    p.known_for_department,
                    p.popularity,
                    p.profile_path,
                )
                for p in unique
            ],
        )


# =====================================================================
# Lecture
# =====================================================================


def list_movies(conn: Connection, limit: int = 50) -> list[MovieRow]:
    """Liste les films en base, du plus récent au plus ancien, avec leurs genres."""
    with conn.cursor(row_factory=class_row(MovieRow)) as cur:
        cur.execute(
            """
            SELECT m.id, m.title, m.release_date, m.vote_average, m.runtime,
                   string_agg(g.name, ', ' ORDER BY g.name) AS genres
              FROM movies m
              LEFT JOIN movie_genres mg ON mg.movie_id = m.id
              LEFT JOIN genres g        ON g.id = mg.genre_id
             GROUP BY m.id
             ORDER BY m.release_date DESC NULLS LAST
             LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def find_movies_by_title(conn: Connection, query: str, limit: int = 20) -> list[MovieRow]:
    """Cherche en base les films dont le titre (ou titre original) contient `query`."""
    pattern = "%" + query.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_") + "%"
    with conn.cursor(row_factory=class_row(MovieRow)) as cur:
        cur.execute(
            """
            SELECT m.id, m.title, m.release_date, m.vote_average, m.runtime,
                   string_agg(g.name, ', ' ORDER BY g.name) AS genres
              FROM movies m
              LEFT JOIN movie_genres mg ON mg.movie_id = m.id
              LEFT JOIN genres g        ON g.id = mg.genre_id
             WHERE m.title ILIKE %(p)s OR m.original_title ILIKE %(p)s
             GROUP BY m.id
             ORDER BY m.release_date DESC NULLS LAST
             LIMIT %(limit)s
            """,
            {"p": pattern, "limit": limit},
        )
        return cur.fetchall()


def get_movie_detail(conn: Connection, movie_id: int) -> MovieDetailRow | None:
    """Renvoie la fiche complète d'un film (relations agrégées), ou None s'il est absent."""
    with conn.cursor(row_factory=class_row(MovieDetailRow)) as cur:
        cur.execute(
            """
            SELECT m.id, m.title, m.original_title, m.tagline, m.overview,
                   m.release_date, m.runtime, m.budget, m.revenue,
                   m.vote_average, m.vote_count, m.fetched_at,
                   c.name AS collection,
                   (SELECT string_agg(g.name, ', ' ORDER BY g.name)
                      FROM movie_genres mg JOIN genres g ON g.id = mg.genre_id
                     WHERE mg.movie_id = m.id) AS genres,
                   (SELECT string_agg(pc.name, ', ' ORDER BY pc.name)
                      FROM movie_production_companies mpc
                      JOIN production_companies pc ON pc.id = mpc.company_id
                     WHERE mpc.movie_id = m.id) AS companies,
                   (SELECT string_agg(co.name, ', ' ORDER BY co.name)
                      FROM movie_production_countries mco
                      JOIN countries co ON co.iso_3166_1 = mco.country_id
                     WHERE mco.movie_id = m.id) AS countries,
                   (SELECT string_agg(COALESCE(l.name, l.english_name), ', ' ORDER BY l.iso_639_1)
                      FROM movie_spoken_languages msl
                      JOIN languages l ON l.iso_639_1 = msl.language_id
                     WHERE msl.movie_id = m.id) AS languages,
                   (SELECT string_agg(p.name, ', ' ORDER BY p.name)
                      FROM movie_crew mc JOIN people p ON p.id = mc.person_id
                     WHERE mc.movie_id = m.id AND mc.job = 'Director') AS directors
              FROM movies m
              LEFT JOIN collections c ON c.id = m.collection_id
             WHERE m.id = %s
            """,
            (movie_id,),
        )
        return cur.fetchone()


def get_movie_cast(conn: Connection, movie_id: int, limit: int = 10) -> list[CastRow]:
    """Renvoie les premiers acteurs d'un film dans l'ordre du générique."""
    with conn.cursor(row_factory=class_row(CastRow)) as cur:
        cur.execute(
            """
            SELECT p.name, mc.character, mc.cast_order
              FROM movie_cast mc
              JOIN people p ON p.id = mc.person_id
             WHERE mc.movie_id = %s
             ORDER BY mc.cast_order
             LIMIT %s
            """,
            (movie_id, limit),
        )
        return cur.fetchall()


def count_tables(conn: Connection) -> list[TableCount]:
    """Compte le nombre de lignes de chaque table du schéma public."""
    with conn.cursor(row_factory=class_row(TableCount)) as cur:
        cur.execute(
            """
            SELECT table_name,
                   (xpath('/row/n/text()',
                          query_to_xml(format('SELECT count(*) AS n FROM %I', table_name),
                                       false, true, '')))[1]::text::int AS total
              FROM information_schema.tables
             WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
             ORDER BY table_name
            """
        )
        return cur.fetchall()


def genre_stats(conn: Connection) -> list[GenreStat]:
    """Nombre de films et note moyenne par genre."""
    with conn.cursor(row_factory=class_row(GenreStat)) as cur:
        cur.execute(
            """
            SELECT g.name,
                   count(*)                          AS movies,
                   round(avg(m.vote_average), 2)     AS avg_vote
              FROM genres g
              JOIN movie_genres mg ON mg.genre_id = g.id
              JOIN movies m        ON m.id = mg.movie_id
             GROUP BY g.name
             ORDER BY movies DESC, g.name
            """
        )
        return cur.fetchall()
