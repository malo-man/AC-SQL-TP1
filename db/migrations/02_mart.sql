-- =====================================================================
-- Schéma « mart » : les données propres produites par le pipeline.
--
-- Il reprend le modèle du TP1 (mêmes entités, mêmes clés, mêmes
-- cardinalités) en l'enrichissant des colonnes IMDb, et vit à côté du
-- schéma public qui reste alimenté par le CLI tmdb_app.
--
-- Ce fichier est rejoué à chaque démarrage par le service db-migrate :
-- tout y est donc idempotent (IF NOT EXISTS). C'est ce qui permet
-- d'ajouter le TP2 à une base TP1 déjà initialisée, dont le volume
-- n'exécute plus les scripts de db/init.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- ---------------------------------------------------------------------
-- Référentiels
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.genres (
    id   INTEGER PRIMARY KEY,
    name TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.collections (
    id            INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    poster_path   TEXT,
    backdrop_path TEXT
);

CREATE TABLE IF NOT EXISTS mart.production_companies (
    id             INTEGER PRIMARY KEY,
    name           TEXT NOT NULL,
    origin_country CHAR(2),
    logo_path      TEXT
);

CREATE TABLE IF NOT EXISTS mart.countries (
    iso_3166_1 CHAR(2) PRIMARY KEY,
    name       TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.languages (
    iso_639_1    CHAR(2) PRIMARY KEY,
    name         TEXT,
    english_name TEXT
);

CREATE TABLE IF NOT EXISTS mart.people (
    id                   INTEGER PRIMARY KEY,
    name                 TEXT     NOT NULL,
    original_name        TEXT,
    gender               SMALLINT,          -- 0 non renseigné, 1 femme, 2 homme, 3 non-binaire
    known_for_department TEXT,
    popularity           NUMERIC(10, 3),
    profile_path         TEXT
);

-- ---------------------------------------------------------------------
-- Films : colonnes TMDB du TP1 + enrichissement IMDb
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.movies (
    id                INTEGER PRIMARY KEY,
    imdb_id           TEXT,
    title             TEXT NOT NULL,
    original_title    TEXT,
    original_language CHAR(2),
    overview          TEXT,
    tagline           TEXT,
    status            TEXT,
    release_date      DATE,
    runtime           INTEGER,                 -- en minutes
    budget            BIGINT,                  -- en USD, NULL si inconnu
    revenue           BIGINT,                  -- en USD, NULL si inconnu
    popularity        NUMERIC(10, 3),
    vote_average      NUMERIC(4, 2),
    vote_count        INTEGER,
    adult             BOOLEAN NOT NULL DEFAULT FALSE,
    homepage          TEXT,
    poster_path       TEXT,
    backdrop_path     TEXT,
    collection_id     INTEGER REFERENCES mart.collections (id) ON DELETE SET NULL,

    -- Enrichissement IMDb (jointure movies.imdb_id = title.ratings.tconst)
    imdb_average_rating   NUMERIC(3, 1),
    imdb_num_votes        INTEGER,
    imdb_start_year       SMALLINT,
    imdb_runtime_minutes  INTEGER,
    imdb_genres           TEXT[],
    has_imdb_match        BOOLEAN NOT NULL DEFAULT FALSE,
    -- Écart de notation entre les deux sources : le coeur du rapprochement métier
    rating_gap            NUMERIC(4, 2),
    votes_ratio           NUMERIC(10, 4),

    -- Traçabilité : d'où vient la ligne et quand a-t-elle été produite
    source_fetched_at TIMESTAMPTZ,
    ingest_date       DATE,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mart_movies_release_date ON mart.movies (release_date);
CREATE INDEX IF NOT EXISTS idx_mart_movies_collection   ON mart.movies (collection_id);
CREATE INDEX IF NOT EXISTS idx_mart_movies_imdb         ON mart.movies (imdb_id);
CREATE INDEX IF NOT EXISTS idx_mart_movies_has_imdb     ON mart.movies (has_imdb_match);

-- ---------------------------------------------------------------------
-- Tables de liaison (relations N-N)
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.movie_genres (
    movie_id INTEGER REFERENCES mart.movies (id) ON DELETE CASCADE,
    genre_id INTEGER REFERENCES mart.genres (id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE IF NOT EXISTS mart.movie_production_companies (
    movie_id   INTEGER REFERENCES mart.movies (id)               ON DELETE CASCADE,
    company_id INTEGER REFERENCES mart.production_companies (id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, company_id)
);

CREATE TABLE IF NOT EXISTS mart.movie_production_countries (
    movie_id   INTEGER REFERENCES mart.movies (id)            ON DELETE CASCADE,
    country_id CHAR(2) REFERENCES mart.countries (iso_3166_1) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, country_id)
);

CREATE TABLE IF NOT EXISTS mart.movie_spoken_languages (
    movie_id    INTEGER REFERENCES mart.movies (id)           ON DELETE CASCADE,
    language_id CHAR(2) REFERENCES mart.languages (iso_639_1) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, language_id)
);

-- Relations porteuses : clé primaire = identifiant de crédit TMDB, car une
-- personne peut occuper plusieurs postes sur un même film (cf. TP1).
CREATE TABLE IF NOT EXISTS mart.movie_cast (
    credit_id  TEXT PRIMARY KEY,
    movie_id   INTEGER NOT NULL REFERENCES mart.movies (id) ON DELETE CASCADE,
    person_id  INTEGER NOT NULL REFERENCES mart.people (id) ON DELETE CASCADE,
    character  TEXT,
    cast_order INTEGER
);

CREATE INDEX IF NOT EXISTS idx_mart_cast_movie  ON mart.movie_cast (movie_id);
CREATE INDEX IF NOT EXISTS idx_mart_cast_person ON mart.movie_cast (person_id);

CREATE TABLE IF NOT EXISTS mart.movie_crew (
    credit_id  TEXT PRIMARY KEY,
    movie_id   INTEGER NOT NULL REFERENCES mart.movies (id) ON DELETE CASCADE,
    person_id  INTEGER NOT NULL REFERENCES mart.people (id) ON DELETE CASCADE,
    department TEXT,
    job        TEXT
);

CREATE INDEX IF NOT EXISTS idx_mart_crew_movie  ON mart.movie_crew (movie_id);
CREATE INDEX IF NOT EXISTS idx_mart_crew_person ON mart.movie_crew (person_id);

-- ---------------------------------------------------------------------
-- Audit des exécutions : base de l'indicateur Raw vs Clean
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.load_runs (
    run_id             UUID PRIMARY KEY,
    job                TEXT        NOT NULL,   -- aggregate | load_mart
    ingest_date        DATE,
    raw_rows_in        BIGINT,                 -- lignes brutes lues dans le lac
    distinct_movies_in BIGINT,                 -- films distincts après dédoublonnage
    clean_rows_out     BIGINT,                 -- lignes propres produites
    rejected_rows      BIGINT,                 -- lignes écartées (identifiant ou titre manquant)
    imdb_matched       BIGINT,                 -- films rapprochés d'une note IMDb
    started_at         TIMESTAMPTZ NOT NULL,
    finished_at        TIMESTAMPTZ,
    duration_seconds   NUMERIC(10, 2),
    status             TEXT        NOT NULL,   -- success | failed | skipped
    message            TEXT
);

CREATE INDEX IF NOT EXISTS idx_mart_load_runs_job ON mart.load_runs (job, started_at DESC);

-- ---------------------------------------------------------------------
-- Tables de transit alimentées par Spark.
--
-- Spark écrit dedans en mode overwrite + truncate : la table est vidée,
-- jamais supprimée, ce qui préserve les types définis ici. L'UPSERT vers
-- la table cible est ensuite fait en SQL, ce qui conserve clés, index et
-- contraintes (un write.jdbc en overwrite les détruirait).
-- ---------------------------------------------------------------------

CREATE UNLOGGED TABLE IF NOT EXISTS mart._stg_genres                     (LIKE mart.genres);
CREATE UNLOGGED TABLE IF NOT EXISTS mart._stg_collections                (LIKE mart.collections);
CREATE UNLOGGED TABLE IF NOT EXISTS mart._stg_production_companies       (LIKE mart.production_companies);
CREATE UNLOGGED TABLE IF NOT EXISTS mart._stg_countries                  (LIKE mart.countries);
CREATE UNLOGGED TABLE IF NOT EXISTS mart._stg_languages                  (LIKE mart.languages);
CREATE UNLOGGED TABLE IF NOT EXISTS mart._stg_people                     (LIKE mart.people);
CREATE UNLOGGED TABLE IF NOT EXISTS mart._stg_movies                     (LIKE mart.movies);
CREATE UNLOGGED TABLE IF NOT EXISTS mart._stg_movie_genres               (LIKE mart.movie_genres);
CREATE UNLOGGED TABLE IF NOT EXISTS mart._stg_movie_production_companies (LIKE mart.movie_production_companies);
CREATE UNLOGGED TABLE IF NOT EXISTS mart._stg_movie_production_countries (LIKE mart.movie_production_countries);
CREATE UNLOGGED TABLE IF NOT EXISTS mart._stg_movie_spoken_languages     (LIKE mart.movie_spoken_languages);
CREATE UNLOGGED TABLE IF NOT EXISTS mart._stg_movie_cast                 (LIKE mart.movie_cast);
CREATE UNLOGGED TABLE IF NOT EXISTS mart._stg_movie_crew                 (LIKE mart.movie_crew);

COMMENT ON SCHEMA mart IS 'Données propres produites par le pipeline PySpark (TP2)';
COMMENT ON TABLE  mart.movies IS 'Films TMDB nettoyés et enrichis des notes IMDb';
COMMENT ON COLUMN mart.movies.rating_gap IS 'vote_average TMDB moins imdb_average_rating';
COMMENT ON TABLE  mart.load_runs IS 'Bilan de chaque exécution Spark : lignes brutes lues, propres écrites, rejetées';
