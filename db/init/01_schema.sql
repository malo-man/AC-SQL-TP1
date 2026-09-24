-- =====================================================================
-- Schéma de la base TMDB
-- Les identifiants (id) sont ceux de TMDB : pas de SERIAL, on réutilise
-- la clé source pour pouvoir faire des UPSERT idempotents.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Référentiels
-- ---------------------------------------------------------------------

CREATE TABLE genres (
    id   INTEGER PRIMARY KEY,
    name TEXT    NOT NULL
);

CREATE TABLE collections (
    id            INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    poster_path   TEXT,
    backdrop_path TEXT
);

CREATE TABLE production_companies (
    id             INTEGER PRIMARY KEY,
    name           TEXT NOT NULL,
    origin_country CHAR(2),
    logo_path      TEXT
);

CREATE TABLE countries (
    iso_3166_1 CHAR(2) PRIMARY KEY,
    name       TEXT    NOT NULL
);

CREATE TABLE languages (
    iso_639_1    CHAR(2) PRIMARY KEY,
    name         TEXT,
    english_name TEXT
);

CREATE TABLE people (
    id                   INTEGER PRIMARY KEY,
    name                 TEXT     NOT NULL,
    original_name        TEXT,
    gender               SMALLINT,          -- 0 non renseigné, 1 femme, 2 homme, 3 non-binaire
    known_for_department TEXT,
    popularity           NUMERIC(10, 3),
    profile_path         TEXT
);

-- ---------------------------------------------------------------------
-- Films
-- ---------------------------------------------------------------------

CREATE TABLE movies (
    id                INTEGER PRIMARY KEY,
    imdb_id           TEXT,
    title             TEXT        NOT NULL,
    original_title    TEXT,
    original_language CHAR(2),
    overview          TEXT,
    tagline           TEXT,
    status            TEXT,
    release_date      DATE,
    runtime           INTEGER,                 -- en minutes
    budget            BIGINT,                  -- en USD
    revenue           BIGINT,                  -- en USD
    popularity        NUMERIC(10, 3),
    vote_average      NUMERIC(4, 2),
    vote_count        INTEGER,
    adult             BOOLEAN     NOT NULL DEFAULT FALSE,
    homepage          TEXT,
    poster_path       TEXT,
    backdrop_path     TEXT,
    collection_id     INTEGER REFERENCES collections (id) ON DELETE SET NULL,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_movies_release_date ON movies (release_date);
CREATE INDEX idx_movies_collection   ON movies (collection_id);

-- ---------------------------------------------------------------------
-- Tables de liaison (relations N-N)
-- ---------------------------------------------------------------------

CREATE TABLE movie_genres (
    movie_id INTEGER REFERENCES movies (id) ON DELETE CASCADE,
    genre_id INTEGER REFERENCES genres (id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE movie_production_companies (
    movie_id   INTEGER REFERENCES movies (id)               ON DELETE CASCADE,
    company_id INTEGER REFERENCES production_companies (id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, company_id)
);

CREATE TABLE movie_production_countries (
    movie_id   INTEGER REFERENCES movies (id)             ON DELETE CASCADE,
    country_id CHAR(2) REFERENCES countries (iso_3166_1)  ON DELETE CASCADE,
    PRIMARY KEY (movie_id, country_id)
);

CREATE TABLE movie_spoken_languages (
    movie_id    INTEGER REFERENCES movies (id)          ON DELETE CASCADE,
    language_id CHAR(2) REFERENCES languages (iso_639_1) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, language_id)
);

-- Casting : credit_id est l'identifiant unique TMDB d'un crédit
CREATE TABLE movie_cast (
    credit_id  TEXT PRIMARY KEY,
    movie_id   INTEGER NOT NULL REFERENCES movies (id) ON DELETE CASCADE,
    person_id  INTEGER NOT NULL REFERENCES people (id) ON DELETE CASCADE,
    character  TEXT,
    cast_order INTEGER
);

CREATE INDEX idx_movie_cast_movie  ON movie_cast (movie_id);
CREATE INDEX idx_movie_cast_person ON movie_cast (person_id);

-- Équipe technique (réalisation, scénario, production, ...)
CREATE TABLE movie_crew (
    credit_id  TEXT PRIMARY KEY,
    movie_id   INTEGER NOT NULL REFERENCES movies (id) ON DELETE CASCADE,
    person_id  INTEGER NOT NULL REFERENCES people (id) ON DELETE CASCADE,
    department TEXT,
    job        TEXT
);

CREATE INDEX idx_movie_crew_movie  ON movie_crew (movie_id);
CREATE INDEX idx_movie_crew_person ON movie_crew (person_id);
