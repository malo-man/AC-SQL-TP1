-- =====================================================================
-- TP3 — Schéma cible : contraintes posées sur les données nettoyées.
--
-- Le schéma cible reprend le modèle du TP1 (mêmes entités, clés et
-- relations) enrichi des colonnes IMDb du TP2, et le durcit : chaque règle
-- de validité ou de cohérence de la matrice qui peut s'écrire comme une
-- contrainte le devient.
--
-- Les contraintes sont posées APRÈS le nettoyage, pas avant : PostgreSQL
-- vérifie chaque ligne existante à la création d'une contrainte. Si une
-- anomalie a échappé au nettoyage, ce script échoue, la transaction est
-- annulée et le curated précédent reste en place. La réussite de ce script
-- est donc la preuve que les données sont conformes au schéma cible.
--
-- Les contraintes portent le numéro du contrôle qu'elles garantissent.
-- Les règles qui ne s'écrivent pas en contrainte (cardinalité (1,N),
-- liaisons obsolètes, date dans le futur relative à aujourd'hui) sont
-- vérifiées par le recontrôle (05_recontrole.sql).
-- =====================================================================

-- ---------------------------------------------------------------------
-- Clés primaires (UNI-01) : celles du modèle du TP1
-- ---------------------------------------------------------------------

ALTER TABLE curated.genres               ADD PRIMARY KEY (id);
ALTER TABLE curated.collections          ADD PRIMARY KEY (id);
ALTER TABLE curated.production_companies ADD PRIMARY KEY (id);
ALTER TABLE curated.countries            ADD PRIMARY KEY (iso_3166_1);
ALTER TABLE curated.languages            ADD PRIMARY KEY (iso_639_1);
ALTER TABLE curated.people               ADD PRIMARY KEY (id);
ALTER TABLE curated.movies               ADD PRIMARY KEY (id);
ALTER TABLE curated.movie_genres               ADD PRIMARY KEY (movie_id, genre_id);
ALTER TABLE curated.movie_production_companies ADD PRIMARY KEY (movie_id, company_id);
ALTER TABLE curated.movie_production_countries ADD PRIMARY KEY (movie_id, country_id);
ALTER TABLE curated.movie_spoken_languages     ADD PRIMARY KEY (movie_id, language_id);
ALTER TABLE curated.movie_cast                 ADD PRIMARY KEY (credit_id);
ALTER TABLE curated.movie_crew                 ADD PRIMARY KEY (credit_id);

-- ---------------------------------------------------------------------
-- Clés étrangères (INT-01, INT-02) : les relations du MLD
-- ---------------------------------------------------------------------

ALTER TABLE curated.movies
    ADD FOREIGN KEY (collection_id) REFERENCES curated.collections (id) ON DELETE SET NULL;
ALTER TABLE curated.movie_genres
    ADD FOREIGN KEY (movie_id) REFERENCES curated.movies (id) ON DELETE CASCADE,
    ADD FOREIGN KEY (genre_id) REFERENCES curated.genres (id) ON DELETE CASCADE;
ALTER TABLE curated.movie_production_companies
    ADD FOREIGN KEY (movie_id)   REFERENCES curated.movies (id)               ON DELETE CASCADE,
    ADD FOREIGN KEY (company_id) REFERENCES curated.production_companies (id) ON DELETE CASCADE;
ALTER TABLE curated.movie_production_countries
    ADD FOREIGN KEY (movie_id)   REFERENCES curated.movies (id)            ON DELETE CASCADE,
    ADD FOREIGN KEY (country_id) REFERENCES curated.countries (iso_3166_1) ON DELETE CASCADE;
ALTER TABLE curated.movie_spoken_languages
    ADD FOREIGN KEY (movie_id)    REFERENCES curated.movies (id)           ON DELETE CASCADE,
    ADD FOREIGN KEY (language_id) REFERENCES curated.languages (iso_639_1) ON DELETE CASCADE;
ALTER TABLE curated.movie_cast
    ADD FOREIGN KEY (movie_id)  REFERENCES curated.movies (id) ON DELETE CASCADE,
    ADD FOREIGN KEY (person_id) REFERENCES curated.people (id) ON DELETE CASCADE;
ALTER TABLE curated.movie_crew
    ADD FOREIGN KEY (movie_id)  REFERENCES curated.movies (id) ON DELETE CASCADE,
    ADD FOREIGN KEY (person_id) REFERENCES curated.people (id) ON DELETE CASCADE;

-- ---------------------------------------------------------------------
-- Colonnes obligatoires (CREATE TABLE AS ne recopie pas les NOT NULL)
-- ---------------------------------------------------------------------

ALTER TABLE curated.genres               ALTER COLUMN name SET NOT NULL;
ALTER TABLE curated.collections          ALTER COLUMN name SET NOT NULL;
ALTER TABLE curated.production_companies ALTER COLUMN name SET NOT NULL;
ALTER TABLE curated.countries            ALTER COLUMN name SET NOT NULL;
ALTER TABLE curated.people               ALTER COLUMN name SET NOT NULL;
ALTER TABLE curated.movies
    ALTER COLUMN title          SET NOT NULL,
    ALTER COLUMN adult          SET NOT NULL,
    ALTER COLUMN has_imdb_match SET NOT NULL,
    ALTER COLUMN loaded_at      SET NOT NULL;
ALTER TABLE curated.movie_cast
    ALTER COLUMN movie_id  SET NOT NULL,
    ALTER COLUMN person_id SET NOT NULL;
ALTER TABLE curated.movie_crew
    ALTER COLUMN movie_id  SET NOT NULL,
    ALTER COLUMN person_id SET NOT NULL;

-- ---------------------------------------------------------------------
-- Unicité (UNI-02, UNI-04, UNI-05)
-- ---------------------------------------------------------------------

ALTER TABLE curated.movies
    ADD CONSTRAINT uni02_imdb_id_unique UNIQUE (imdb_id);
-- NULLS NOT DISTINCT : deux crédits sans personnage pour la même personne
-- et le même film sont aussi un doublon.
ALTER TABLE curated.movie_cast
    ADD CONSTRAINT uni04_role_unique UNIQUE NULLS NOT DISTINCT (movie_id, person_id, character);
ALTER TABLE curated.movie_crew
    ADD CONSTRAINT uni05_job_unique UNIQUE NULLS NOT DISTINCT (movie_id, person_id, job);

-- ---------------------------------------------------------------------
-- Validité : formats, plages et domaines (VAL-01 à VAL-15)
-- ---------------------------------------------------------------------

ALTER TABLE curated.movies
    ADD CONSTRAINT val01_imdb_id_format     CHECK (imdb_id ~ '^tt[0-9]{7,}$'),
    ADD CONSTRAINT val02_vote_average_range CHECK (vote_average BETWEEN 0 AND 10),
    ADD CONSTRAINT val03_imdb_rating_range  CHECK (imdb_average_rating BETWEEN 1 AND 10),
    ADD CONSTRAINT val04_votes_positive     CHECK (vote_count >= 0 AND imdb_num_votes >= 0),
    ADD CONSTRAINT val05_runtime_range      CHECK (runtime BETWEEN 1 AND 600),
    -- Borne basse seulement : une contrainte n'est vérifiée qu'à l'écriture,
    -- une borne « aujourd'hui + 5 ans » deviendrait fausse avec le temps.
    -- La borne haute est vérifiée par le contrôle VAL-06.
    ADD CONSTRAINT val06_release_date_min   CHECK (release_date >= DATE '1888-01-01'),
    ADD CONSTRAINT val07_status_domain      CHECK (status IN ('Rumored', 'Planned', 'In Production',
                                                              'Post Production', 'Released', 'Canceled')),
    ADD CONSTRAINT val08_language_format    CHECK (original_language ~ '^[a-z]{2}$'),
    ADD CONSTRAINT val10_budget_plausible   CHECK (budget >= 1000),
    ADD CONSTRAINT val10_revenue_plausible  CHECK (revenue >= 1000),
    ADD CONSTRAINT val11_homepage_url       CHECK (homepage ~ '^https?://[^[:space:]]+$'),
    ADD CONSTRAINT val12_poster_path        CHECK (poster_path ~ '^/[A-Za-z0-9_-]+\.(jpg|jpeg|png|svg)$'),
    ADD CONSTRAINT val12_backdrop_path      CHECK (backdrop_path ~ '^/[A-Za-z0-9_-]+\.(jpg|jpeg|png|svg)$'),
    ADD CONSTRAINT val14_popularity         CHECK (popularity >= 0);

ALTER TABLE curated.languages
    ADD CONSTRAINT val08_iso_639_1_format CHECK (iso_639_1 ~ '^[a-z]{2}$');
ALTER TABLE curated.movie_spoken_languages
    ADD CONSTRAINT val08_language_format CHECK (language_id ~ '^[a-z]{2}$');
ALTER TABLE curated.countries
    ADD CONSTRAINT val09_iso_3166_1_format CHECK (iso_3166_1 ~ '^[A-Z]{2}$');
ALTER TABLE curated.movie_production_countries
    ADD CONSTRAINT val09_country_format CHECK (country_id ~ '^[A-Z]{2}$');
ALTER TABLE curated.production_companies
    ADD CONSTRAINT val09_origin_country_format CHECK (origin_country ~ '^[A-Z]{2}$'),
    ADD CONSTRAINT val12_logo_path             CHECK (logo_path ~ '^/[A-Za-z0-9_-]+\.(jpg|jpeg|png|svg)$');
ALTER TABLE curated.collections
    ADD CONSTRAINT val12_poster_path   CHECK (poster_path ~ '^/[A-Za-z0-9_-]+\.(jpg|jpeg|png|svg)$'),
    ADD CONSTRAINT val12_backdrop_path CHECK (backdrop_path ~ '^/[A-Za-z0-9_-]+\.(jpg|jpeg|png|svg)$');
ALTER TABLE curated.people
    ADD CONSTRAINT val12_profile_path   CHECK (profile_path ~ '^/[A-Za-z0-9_-]+\.(jpg|jpeg|png|svg)$'),
    ADD CONSTRAINT val13_gender_domain  CHECK (gender IN (1, 2, 3)),
    ADD CONSTRAINT val14_popularity     CHECK (popularity >= 0),
    ADD CONSTRAINT val15_department     CHECK (known_for_department IN (
        'Acting', 'Directing', 'Writing', 'Production', 'Camera', 'Editing', 'Sound', 'Art',
        'Costume & Make-Up', 'Visual Effects', 'Lighting', 'Crew', 'Creator'));
ALTER TABLE curated.movie_cast
    ADD CONSTRAINT val14_cast_order CHECK (cast_order >= 0);
ALTER TABLE curated.movie_crew
    ADD CONSTRAINT val15_department CHECK (department IN (
        'Acting', 'Directing', 'Writing', 'Production', 'Camera', 'Editing', 'Sound', 'Art',
        'Costume & Make-Up', 'Visual Effects', 'Lighting', 'Crew'));

-- VAL-16 : texte normalisé, sur toutes les colonnes texte du catalogue.
-- Générée plutôt qu'écrite à la main : une colonne ajoutée plus tard sera
-- couverte sans modifier ce script.
DO $$
DECLARE
    c RECORD;
BEGIN
    FOR c IN SELECT * FROM dq.text_columns('curated') LOOP
        EXECUTE format(
            'ALTER TABLE curated.%I ADD CONSTRAINT %I CHECK (btrim(%I::text) <> '''' AND %I::text = btrim(%I::text))',
            c.table_name, left('val16_' || c.table_name || '_' || c.column_name, 63),
            c.column_name, c.column_name, c.column_name
        );
    END LOOP;
END $$;

-- ---------------------------------------------------------------------
-- Cohérence entre colonnes (COH-01 à COH-04)
-- ---------------------------------------------------------------------

ALTER TABLE curated.movies
    ADD CONSTRAINT coh01_no_rating_without_votes
        CHECK (coalesce(vote_count, 0) > 0 OR vote_average IS NULL),
    ADD CONSTRAINT coh02_imdb_match
        CHECK (has_imdb_match = (imdb_average_rating IS NOT NULL) AND (NOT has_imdb_match OR imdb_id IS NOT NULL)),
    ADD CONSTRAINT coh03_rating_gap
        CHECK (rating_gap IS NOT DISTINCT FROM round(vote_average - imdb_average_rating, 2)),
    ADD CONSTRAINT coh04_votes_ratio
        CHECK (votes_ratio IS NOT DISTINCT FROM round(vote_count::numeric / nullif(imdb_num_votes, 0), 4));

-- ---------------------------------------------------------------------
-- Index : ceux du mart
-- ---------------------------------------------------------------------

CREATE INDEX ON curated.movies (release_date);
CREATE INDEX ON curated.movies (collection_id);
CREATE INDEX ON curated.movie_cast (movie_id);
CREATE INDEX ON curated.movie_cast (person_id);
CREATE INDEX ON curated.movie_crew (movie_id);
CREATE INDEX ON curated.movie_crew (person_id);

COMMENT ON TABLE curated.movies IS 'Films nettoyés : conformes au schéma cible, corrections tracées dans dq.corrections';

ANALYZE curated.movies, curated.people, curated.movie_cast, curated.movie_crew;
