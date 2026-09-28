-- =====================================================================
-- TP3 — Socle de l'audit qualité : schéma « dq ».
--
-- Le schéma dq ne contient aucune donnée métier. Il décrit et trace le
-- contrôle qualité :
--
--   controls     la matrice de contrôles, sous forme de données : chaque
--                contrôle porte sa règle, sa sévérité, son seuil, son
--                traitement, sa justification et ses deux requêtes ;
--   runs         une ligne par exécution de l'audit ;
--   results      le bilan de chaque contrôle, avant et après nettoyage ;
--   anomalies    le détail des lignes en anomalie (dernière exécution) ;
--   corrections  le journal des corrections appliquées (dernière exécution).
--
-- Un même contrôle s'exécute sur deux schémas : « mart » (les données du
-- TP2, phase avant) puis « curated » (les données nettoyées, phase après).
-- Ses requêtes sont donc écrites avec le paramètre %1$I à la place du nom
-- de schéma, substitué par format() au moment de l'exécution.
--
-- Rejouable : CREATE ... IF NOT EXISTS et CREATE OR REPLACE partout. Ce
-- fichier est exécuté au début de chaque audit, dans la même transaction.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS dq;
COMMENT ON SCHEMA dq IS 'Contrôle qualité des données (TP3) : matrice, exécutions, résultats, corrections';

-- ---------------------------------------------------------------------
-- Tables du modèle contrôlé
--
-- Les 13 tables du modèle du TP1, avec l'expression qui identifie une
-- ligne. Elle sert à nommer une anomalie ou une correction (« movies:278 »,
-- « movie_genres:278/18 ») et aux contrôles génériques qui parcourent le
-- catalogue de PostgreSQL.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dq.model_tables (
    table_name TEXT     PRIMARY KEY,
    kind       TEXT     NOT NULL CHECK (kind IN ('entité', 'référentiel', 'liaison', 'relation porteuse')),
    key_expr   TEXT     NOT NULL,
    load_order SMALLINT NOT NULL
);

INSERT INTO dq.model_tables (table_name, kind, key_expr, load_order) VALUES
    ('genres',                     'référentiel',       'id::text',                               1),
    ('collections',                'référentiel',       'id::text',                               2),
    ('production_companies',       'référentiel',       'id::text',                               3),
    ('countries',                  'référentiel',       'iso_3166_1::text',                       4),
    ('languages',                  'référentiel',       'iso_639_1::text',                        5),
    ('people',                     'entité',            'id::text',                               6),
    ('movies',                     'entité',            'id::text',                               7),
    ('movie_genres',               'liaison',           'movie_id || ''/'' || genre_id',          8),
    ('movie_production_companies', 'liaison',           'movie_id || ''/'' || company_id',        9),
    ('movie_production_countries', 'liaison',           'movie_id || ''/'' || country_id',       10),
    ('movie_spoken_languages',     'liaison',           'movie_id || ''/'' || language_id',      11),
    ('movie_cast',                 'relation porteuse', 'credit_id',                             12),
    ('movie_crew',                 'relation porteuse', 'credit_id',                             13)
ON CONFLICT (table_name) DO UPDATE
    SET kind = EXCLUDED.kind, key_expr = EXCLUDED.key_expr, load_order = EXCLUDED.load_order;

-- ---------------------------------------------------------------------
-- Matrice de contrôles
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dq.controls (
    control_id     TEXT          PRIMARY KEY,           -- ex. VAL-03
    dimension      TEXT          NOT NULL
                   CHECK (dimension IN ('complétude', 'unicité', 'validité', 'cohérence', 'intégrité')),
    table_name     TEXT          NOT NULL,
    column_name    TEXT,                                -- NULL : contrôle portant sur la ligne entière
    rule           TEXT          NOT NULL,              -- énoncé de la règle
    severity       TEXT          NOT NULL CHECK (severity IN ('critique', 'majeure', 'mineure', 'info')),
    threshold      NUMERIC(5, 4) NOT NULL DEFAULT 0,    -- taux d'anomalies toléré (0 = aucune)
    treatment      TEXT          NOT NULL
                   CHECK (treatment IN ('suppression', 'imputation', 'substitution', 'correction', 'conservation')),
    justification  TEXT          NOT NULL,
    population_sql TEXT          NOT NULL,              -- SELECT count(*) des lignes contrôlées
    anomaly_sql    TEXT          NOT NULL,              -- SELECT record_key, observed_value des lignes en anomalie
    updated_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

COMMENT ON TABLE  dq.controls IS 'Matrice de contrôles qualité : une ligne par contrôle, exécutable sur mart et sur curated';
COMMENT ON COLUMN dq.controls.anomaly_sql IS 'Requête paramétrée par le schéma (%1$I), qui renvoie record_key et observed_value. Un % littéral s''écrit %%';

-- ---------------------------------------------------------------------
-- Exécutions et résultats
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dq.runs (
    run_id           UUID        PRIMARY KEY,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at      TIMESTAMPTZ,
    mart_ingest_date DATE,                   -- instantané du pipeline audité
    mart_movies      BIGINT,
    curated_movies   BIGINT,
    anomalies_before BIGINT,
    anomalies_after  BIGINT,
    corrections      BIGINT
);

CREATE TABLE IF NOT EXISTS dq.results (
    run_id       UUID        NOT NULL REFERENCES dq.runs (run_id) ON DELETE CASCADE,
    phase        TEXT        NOT NULL CHECK (phase IN ('avant', 'après')),
    control_id   TEXT        NOT NULL,
    schema_name  TEXT        NOT NULL,
    checked_rows BIGINT      NOT NULL,
    anomaly_rows BIGINT      NOT NULL,
    anomaly_rate NUMERIC(9, 6),              -- NULL quand rien n'est contrôlable
    passed       BOOLEAN,                    -- taux <= seuil ; NULL = non évaluable
    executed_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, phase, control_id)
);

CREATE TABLE IF NOT EXISTS dq.anomalies (
    run_id         UUID NOT NULL REFERENCES dq.runs (run_id) ON DELETE CASCADE,
    phase          TEXT NOT NULL,
    control_id     TEXT NOT NULL,
    record_key     TEXT,
    observed_value TEXT
);

CREATE INDEX IF NOT EXISTS idx_dq_anomalies_run ON dq.anomalies (run_id, phase, control_id);

CREATE TABLE IF NOT EXISTS dq.corrections (
    correction_id BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        UUID        NOT NULL REFERENCES dq.runs (run_id) ON DELETE CASCADE,
    control_id    TEXT        NOT NULL,
    table_name    TEXT        NOT NULL,
    record_key    TEXT        NOT NULL,
    column_name   TEXT,                      -- NULL : ligne supprimée
    action        TEXT        NOT NULL
                  CHECK (action IN ('suppression', 'imputation', 'substitution', 'correction')),
    old_value     TEXT,
    new_value     TEXT,
    corrected_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_dq_corrections_run ON dq.corrections (run_id, control_id);

COMMENT ON TABLE dq.results     IS 'Bilan de chaque contrôle, phase avant (mart) et après (curated)';
COMMENT ON TABLE dq.anomalies   IS 'Lignes en anomalie, conservées pour la dernière exécution seulement';
COMMENT ON TABLE dq.corrections IS 'Journal des corrections du nettoyage, conservé pour la dernière exécution seulement';

-- ---------------------------------------------------------------------
-- Fonctions
-- ---------------------------------------------------------------------

-- Exécution en cours : ouverte par 02_audit.sql, visible jusqu'à la fin de
-- la transaction (set_config local), ce qui évite toute variable psql.
CREATE OR REPLACE FUNCTION dq.current_run() RETURNS UUID
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_run TEXT := current_setting('dq.run_id', true);
BEGIN
    IF v_run IS NULL OR v_run = '' THEN
        RAISE EXCEPTION 'Aucune exécution ouverte : 02_audit.sql doit être exécuté dans la même transaction';
    END IF;
    RETURN v_run::uuid;
END $$;

CREATE OR REPLACE FUNCTION dq.severity_weight(p_severity TEXT) RETURNS INTEGER
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE p_severity WHEN 'critique' THEN 4 WHEN 'majeure' THEN 3 WHEN 'mineure' THEN 2 ELSE 1 END
$$;

-- Nombre de lignes d'une table, NULL si elle n'existe pas (curated avant le
-- premier nettoyage) : les vues de synthèse restent interrogeables.
CREATE OR REPLACE FUNCTION dq.row_count(p_schema TEXT, p_table TEXT) RETURNS BIGINT
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_count BIGINT;
BEGIN
    IF to_regclass(format('%I.%I', p_schema, p_table)) IS NULL THEN
        RETURN NULL;
    END IF;
    EXECUTE format('SELECT count(*) FROM %I.%I', p_schema, p_table) INTO v_count;
    RETURN v_count;
END $$;

-- Colonnes texte d'un schéma parmi les tables du modèle, lues dans le
-- catalogue : un contrôle ou une correction qui s'appuie dessus couvre
-- automatiquement toute colonne ajoutée plus tard.
CREATE OR REPLACE FUNCTION dq.text_columns(p_schema TEXT)
RETURNS TABLE (table_name TEXT, column_name TEXT, key_expr TEXT)
LANGUAGE sql STABLE AS $$
    SELECT c.table_name::text, c.column_name::text, t.key_expr
    FROM information_schema.columns c
    JOIN dq.model_tables t ON t.table_name = c.table_name
    WHERE c.table_schema = p_schema
      AND c.data_type IN ('text', 'character', 'character varying')
    ORDER BY t.load_order, c.ordinal_position
$$;

-- Nombre de valeurs texte renseignées : la population du contrôle d'hygiène.
CREATE OR REPLACE FUNCTION dq.text_cells(p_schema TEXT) RETURNS BIGINT
LANGUAGE plpgsql STABLE AS $$
DECLARE
    col   RECORD;
    v_sum BIGINT := 0;
    v_n   BIGINT;
BEGIN
    FOR col IN SELECT * FROM dq.text_columns(p_schema) LOOP
        EXECUTE format('SELECT count(%I) FROM %I.%I', col.column_name, p_schema, col.table_name)
            INTO v_n;
        v_sum := v_sum + v_n;
    END LOOP;
    RETURN v_sum;
END $$;

-- Valeurs texte mal formées : vides, ou entourées d'espaces. Le cast en
-- text retire le remplissage d'un CHAR(2), si bien qu'un code '' stocké
-- en CHAR(2) ressort bien comme une chaîne vide.
CREATE OR REPLACE FUNCTION dq.text_hygiene(p_schema TEXT)
RETURNS TABLE (record_key TEXT, observed_value TEXT)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    col RECORD;
BEGIN
    FOR col IN SELECT * FROM dq.text_columns(p_schema) LOOP
        RETURN QUERY EXECUTE format(
            'SELECT %L || '':'' || (%s), %L || '' = '' || quote_literal(%I::text)
               FROM %I.%I
              WHERE %I IS NOT NULL
                AND (btrim(%I::text) = '''' OR %I::text <> btrim(%I::text))',
            col.table_name, col.key_expr, col.column_name, col.column_name,
            p_schema, col.table_name,
            col.column_name, col.column_name, col.column_name, col.column_name
        );
    END LOOP;
END $$;

-- Moteur de l'audit : exécute chaque contrôle de la matrice sur un schéma
-- et enregistre son bilan et le détail des lignes en anomalie.
CREATE OR REPLACE FUNCTION dq.run_controls(p_schema TEXT, p_phase TEXT)
RETURNS SETOF dq.results
LANGUAGE plpgsql AS $$
DECLARE
    v_run     UUID := dq.current_run();
    c         dq.controls%ROWTYPE;
    v_checked BIGINT;
    v_found   BIGINT;
BEGIN
    FOR c IN SELECT * FROM dq.controls ORDER BY control_id LOOP
        EXECUTE format(c.population_sql, p_schema) INTO v_checked;

        EXECUTE format(
            'INSERT INTO dq.anomalies (run_id, phase, control_id, record_key, observed_value)
             SELECT $1, $2, $3, a.record_key, a.observed_value
             FROM (%s) AS a (record_key, observed_value)',
            format(c.anomaly_sql, p_schema)
        ) USING v_run, p_phase, c.control_id;
        GET DIAGNOSTICS v_found = ROW_COUNT;

        RETURN QUERY
        INSERT INTO dq.results (run_id, phase, control_id, schema_name,
                                checked_rows, anomaly_rows, anomaly_rate, passed)
        VALUES (
            v_run, p_phase, c.control_id, p_schema, coalesce(v_checked, 0), v_found,
            CASE WHEN v_checked > 0 THEN round(v_found::numeric / v_checked, 6) END,
            CASE WHEN v_checked > 0 THEN v_found::numeric / v_checked <= c.threshold END
        )
        RETURNING *;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------
-- Corrections journalisées, appliquées au schéma curated
--
-- Chaque correction passe par l'une de ces deux fonctions, qui écrivent
-- d'abord dans dq.corrections (clé, ancienne et nouvelle valeur), puis
-- modifient la table avec le même prédicat : rien n'est corrigé sans trace.
-- Dans les fragments SQL passés en paramètre, la ligne s'appelle « t ».
-- ---------------------------------------------------------------------

-- Remplace la valeur d'une colonne sur les lignes visées.
CREATE OR REPLACE FUNCTION dq.fix(
    p_control TEXT, p_action TEXT, p_table TEXT, p_column TEXT, p_new_value TEXT, p_where TEXT
) RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_key TEXT := (SELECT key_expr FROM dq.model_tables WHERE table_name = p_table);
    v_n   BIGINT;
BEGIN
    EXECUTE format(
        'INSERT INTO dq.corrections (run_id, control_id, table_name, record_key, column_name,
                                     action, old_value, new_value)
         SELECT $1, $2, $3, %s, $4, $5, t.%I::text, (%s)::text FROM curated.%I AS t WHERE %s',
        v_key, p_column, p_new_value, p_table, p_where
    ) USING dq.current_run(), p_control, p_table, p_column, p_action;

    EXECUTE format('UPDATE curated.%I AS t SET %I = %s WHERE %s',
                   p_table, p_column, p_new_value, p_where);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN v_n;
END $$;

-- Supprime les lignes visées ; la ligne supprimée est conservée en JSON.
CREATE OR REPLACE FUNCTION dq.remove(p_control TEXT, p_table TEXT, p_where TEXT)
RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_key TEXT := (SELECT key_expr FROM dq.model_tables WHERE table_name = p_table);
    v_n   BIGINT;
BEGIN
    EXECUTE format(
        'INSERT INTO dq.corrections (run_id, control_id, table_name, record_key, column_name,
                                     action, old_value, new_value)
         SELECT $1, $2, $3, %s, NULL, ''suppression'', left(row_to_json(t)::text, 500), NULL
           FROM curated.%I AS t WHERE %s',
        v_key, p_table, p_where
    ) USING dq.current_run(), p_control, p_table;

    EXECUTE format('DELETE FROM curated.%I AS t WHERE %s', p_table, p_where);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN v_n;
END $$;

-- Indicateurs métier calculés sur un schéma : c'est ce que voit le
-- dashboard, et donc ce que les anomalies faussent.
CREATE OR REPLACE FUNCTION dq.kpis(p_schema TEXT)
RETURNS TABLE (indicator TEXT, value NUMERIC)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    IF to_regclass(format('%I.movies', p_schema)) IS NULL THEN
        RETURN;
    END IF;
    RETURN QUERY EXECUTE format($q$
        SELECT k.indicator, k.value FROM (
            SELECT
                count(*)::numeric                                              AS films,
                count(vote_average)::numeric                                   AS films_notes_tmdb,
                round(avg(vote_average), 3)                                    AS note_tmdb_moyenne,
                round(avg(imdb_average_rating), 3)                             AS note_imdb_moyenne,
                round(avg(rating_gap), 3)                                      AS ecart_tmdb_imdb_moyen,
                round(100.0 * count(*) FILTER (WHERE has_imdb_match) / nullif(count(*), 0), 2)
                                                                               AS couverture_imdb_pct,
                count(runtime)::numeric                                        AS films_avec_duree,
                count(budget)::numeric                                         AS films_avec_budget,
                round(avg(budget))                                             AS budget_moyen,
                count(release_date)::numeric                                   AS films_dates
            FROM %1$I.movies
        ) s
        CROSS JOIN LATERAL (VALUES
            ('films',                  s.films),
            ('films notés TMDB',       s.films_notes_tmdb),
            ('note TMDB moyenne',      s.note_tmdb_moyenne),
            ('note IMDb moyenne',      s.note_imdb_moyenne),
            ('écart TMDB - IMDb moyen', s.ecart_tmdb_imdb_moyen),
            ('couverture IMDb (%%)',   s.couverture_imdb_pct),
            ('films avec durée',       s.films_avec_duree),
            ('films avec budget',      s.films_avec_budget),
            ('budget moyen (USD)',     s.budget_moyen),
            ('films datés',            s.films_dates)
        ) AS k (indicator, value)
    $q$, p_schema);
END $$;

-- Notes moyennes par genre sur un schéma : l'indicateur v_genre_stats du
-- dashboard TP2, recalculé pour mesurer l'effet du nettoyage.
CREATE OR REPLACE FUNCTION dq.genre_ratings(p_schema TEXT)
RETURNS TABLE (genre TEXT, films BIGINT, note_tmdb NUMERIC, note_imdb NUMERIC, ecart NUMERIC)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    IF to_regclass(format('%I.movies', p_schema)) IS NULL THEN
        RETURN;
    END IF;
    RETURN QUERY EXECUTE format($q$
        SELECT g.name, count(*), round(avg(m.vote_average), 3),
               round(avg(m.imdb_average_rating), 3), round(avg(m.rating_gap), 3)
        FROM %1$I.movies m
        JOIN %1$I.movie_genres mg ON mg.movie_id = m.id
        JOIN %1$I.genres g        ON g.id = mg.genre_id
        GROUP BY g.name
    $q$, p_schema);
END $$;

-- ---------------------------------------------------------------------
-- Vues de synthèse (dernière exécution terminée)
-- ---------------------------------------------------------------------

CREATE OR REPLACE VIEW dq.v_last_run AS
SELECT *
FROM dq.runs
WHERE finished_at IS NOT NULL
ORDER BY started_at DESC
LIMIT 1;

-- Chaque contrôle avant et après nettoyage, avec sa priorité et son verdict.
-- Priorité = poids de la sévérité (4 à 1) x taux d'anomalies avant, en %.
CREATE OR REPLACE VIEW dq.v_results_last AS
SELECT
    c.control_id,
    c.dimension,
    c.table_name,
    c.column_name,
    c.rule,
    c.severity,
    c.threshold,
    c.treatment,
    b.checked_rows                                          AS checked_before,
    b.anomaly_rows                                          AS anomalies_before,
    round(100 * b.anomaly_rate, 2)                          AS pct_before,
    a.checked_rows                                          AS checked_after,
    a.anomaly_rows                                          AS anomalies_after,
    round(100 * a.anomaly_rate, 2)                          AS pct_after,
    round(dq.severity_weight(c.severity) * 100 * coalesce(b.anomaly_rate, 0), 2) AS priority,
    CASE
        WHEN b.checked_rows = 0 AND coalesce(a.checked_rows, 0) = 0 THEN 'non évaluable'
        WHEN b.anomaly_rows = 0 AND coalesce(a.anomaly_rows, 0) = 0 THEN 'conforme'
        WHEN a.anomaly_rows = 0                                     THEN 'corrigé'
        WHEN c.treatment = 'conservation' OR a.passed               THEN 'résiduel accepté'
        ELSE 'à traiter'
    END                                                     AS verdict,
    c.justification
FROM dq.v_last_run r
JOIN dq.results b  ON b.run_id = r.run_id AND b.phase = 'avant'
JOIN dq.controls c ON c.control_id = b.control_id
LEFT JOIN dq.results a ON a.run_id = r.run_id AND a.phase = 'après' AND a.control_id = b.control_id;

-- Score par dimension : moyenne des taux de conformité des contrôles
-- évaluables (100 = aucune anomalie), et part des contrôles sous leur seuil.
CREATE OR REPLACE VIEW dq.v_dimension_scores AS
WITH scored AS (
    SELECT res.phase, c.dimension, res.anomaly_rows, res.anomaly_rate, res.passed
    FROM dq.v_last_run r
    JOIN dq.results res ON res.run_id = r.run_id
    JOIN dq.controls c  ON c.control_id = res.control_id
)
SELECT
    phase,
    dimension,
    count(*) FILTER (WHERE passed IS NOT NULL)                         AS controls,
    count(*) FILTER (WHERE passed)                                     AS controls_passed,
    sum(anomaly_rows)                                                  AS anomalies,
    round(100 * avg(1 - anomaly_rate), 2)                              AS score
FROM scored
GROUP BY GROUPING SETS ((phase, dimension), (phase))
ORDER BY phase DESC, dimension NULLS LAST;

CREATE OR REPLACE VIEW dq.v_corrections_summary AS
SELECT
    co.control_id,
    co.action,
    co.table_name,
    co.column_name,
    count(*) AS rows_corrected
FROM dq.v_last_run r
JOIN dq.corrections co ON co.run_id = r.run_id
GROUP BY co.control_id, co.action, co.table_name, co.column_name
ORDER BY co.control_id, co.table_name, co.column_name;

CREATE OR REPLACE VIEW dq.v_volumes AS
SELECT
    t.table_name,
    t.kind,
    dq.row_count('mart', t.table_name)    AS mart_rows,
    dq.row_count('curated', t.table_name) AS curated_rows,
    dq.row_count('curated', t.table_name) - dq.row_count('mart', t.table_name) AS delta
FROM dq.model_tables t
ORDER BY t.load_order;

CREATE OR REPLACE VIEW dq.v_kpi_impact AS
SELECT
    m.indicator,
    m.value                AS mart,
    c.value                AS curated,
    c.value - m.value      AS delta
FROM dq.kpis('mart') WITH ORDINALITY AS m (indicator, value, n)
LEFT JOIN dq.kpis('curated') AS c (indicator, value) ON c.indicator = m.indicator
ORDER BY m.n;

CREATE OR REPLACE VIEW dq.v_genre_impact AS
SELECT
    m.genre,
    m.films                            AS films_mart,
    c.films                            AS films_curated,
    m.note_tmdb                        AS note_tmdb_mart,
    c.note_tmdb                        AS note_tmdb_curated,
    c.note_tmdb - m.note_tmdb          AS effet_note_tmdb,
    m.ecart                            AS ecart_mart,
    c.ecart                            AS ecart_curated
FROM dq.genre_ratings('mart') m
LEFT JOIN dq.genre_ratings('curated') c ON c.genre = m.genre
ORDER BY abs(coalesce(c.note_tmdb - m.note_tmdb, 0)) DESC, m.films DESC;

-- Historique : anomalies par exécution, phase et dimension. Fait apparaître
-- l'effet des corrections remontées dans le pipeline sur le mart lui-même.
CREATE OR REPLACE VIEW dq.v_history AS
SELECT
    r.run_id,
    r.started_at,
    res.phase,
    c.dimension,
    sum(res.anomaly_rows)                        AS anomalies,
    count(*) FILTER (WHERE res.passed = false)   AS controls_failed
FROM dq.runs r
JOIN dq.results res ON res.run_id = r.run_id
JOIN dq.controls c  ON c.control_id = res.control_id
WHERE r.finished_at IS NOT NULL
GROUP BY r.run_id, r.started_at, res.phase, c.dimension;
