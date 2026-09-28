-- =====================================================================
-- TP3 — Vérification de la cartographie et du schéma (lecture seule).
--
-- Confronte la cartographie documentée (dictionnaire du TP1, sujet1.md) à
-- ce que la base contient réellement : sources et volumes par couche,
-- tables, colonnes, types, clés et relations. Script psql (méta-commandes
-- \echo), exécuté par quality/run.sh ; sa sortie alimente cartographie.md.
-- =====================================================================

\echo '=== 1. Sources et volumes par couche (dernière exécution de chaque job)'
SELECT job, status, ingest_date, raw_rows_in, distinct_movies_in, clean_rows_out,
       rejected_rows, imdb_matched, round(duration_seconds, 1) AS duration_s, finished_at
FROM mart.v_raw_vs_clean
ORDER BY CASE job WHEN 'aggregate' THEN 1 WHEN 'load_mart' THEN 2 ELSE 3 END;

\echo '=== 2. Tables par schéma : TP1 (public), TP2 (mart), TP3 (curated)'
SELECT t.table_name,
       t.kind,
       dq.row_count('public', t.table_name)  AS public_rows,
       dq.row_count('mart', t.table_name)    AS mart_rows,
       dq.row_count('curated', t.table_name) AS curated_rows,
       (SELECT count(*) FROM information_schema.columns c
         WHERE c.table_schema = 'mart' AND c.table_name = t.table_name) AS mart_columns
FROM dq.model_tables t
ORDER BY t.load_order;

\echo '=== 3. Colonnes du mart : types réels'
SELECT c.table_name, c.column_name,
       format_type(a.atttypid, a.atttypmod) AS type,
       CASE WHEN c.is_nullable = 'NO' THEN 'NOT NULL' ELSE '' END AS nullable,
       coalesce(c.column_default, '') AS default_value
FROM information_schema.columns c
JOIN pg_attribute a ON a.attrelid = format('%I.%I', c.table_schema, c.table_name)::regclass
                   AND a.attname = c.column_name
JOIN dq.model_tables t ON t.table_name = c.table_name
WHERE c.table_schema = 'mart'
ORDER BY t.load_order, c.ordinal_position;

\echo '=== 4. Écarts de structure entre le modèle du TP1 (public) et le mart du TP2'
WITH cols AS (
    SELECT c.table_schema, c.table_name, c.column_name,
           format_type(a.atttypid, a.atttypmod) AS type
    FROM information_schema.columns c
    JOIN pg_attribute a ON a.attrelid = format('%I.%I', c.table_schema, c.table_name)::regclass
                       AND a.attname = c.column_name
    WHERE c.table_schema IN ('public', 'mart')
      AND c.table_name IN (SELECT table_name FROM dq.model_tables)
)
SELECT coalesce(p.table_name, m.table_name)   AS table_name,
       coalesce(p.column_name, m.column_name) AS column_name,
       p.type AS type_tp1,
       m.type AS type_tp2,
       CASE WHEN p.column_name IS NULL THEN 'ajoutée au TP2'
            WHEN m.column_name IS NULL THEN 'absente du TP2'
            ELSE 'type modifié' END AS ecart
FROM (SELECT * FROM cols WHERE table_schema = 'public') p
FULL JOIN (SELECT * FROM cols WHERE table_schema = 'mart') m
       ON m.table_name = p.table_name AND m.column_name = p.column_name
WHERE p.column_name IS NULL OR m.column_name IS NULL OR p.type <> m.type
ORDER BY 1, 2;

\echo '=== 5. Contraintes du mart : clés primaires, étrangères, unicité'
SELECT r.relname AS table_name,
       CASE c.contype WHEN 'p' THEN 'PK' WHEN 'f' THEN 'FK' WHEN 'u' THEN 'UNIQUE' WHEN 'c' THEN 'CHECK' END AS type,
       pg_get_constraintdef(c.oid) AS definition
FROM pg_constraint c
JOIN pg_class r ON r.oid = c.conrelid
JOIN dq.model_tables t ON t.table_name = r.relname
WHERE c.connamespace = 'mart'::regnamespace
  AND c.contype IN ('p', 'f', 'u', 'c')
ORDER BY t.load_order, 2 DESC, 3;

\echo '=== 6. Relations du MLD : présentes dans chaque schéma ?'
WITH fk AS (
    SELECT c.connamespace::regnamespace::text AS schema_name,
           src.relname AS from_table,
           dst.relname AS to_table
    FROM pg_constraint c
    JOIN pg_class src ON src.oid = c.conrelid
    JOIN pg_class dst ON dst.oid = c.confrelid
    WHERE c.contype = 'f' AND c.connamespace::regnamespace::text IN ('public', 'mart', 'curated')
)
SELECT from_table || ' -> ' || to_table AS relation,
       bool_or(schema_name = 'public')  AS tp1_public,
       bool_or(schema_name = 'mart')    AS tp2_mart,
       bool_or(schema_name = 'curated') AS tp3_curated
FROM fk
GROUP BY 1
ORDER BY 1;

\echo '=== 7. Contraintes du schéma cible (curated) par type'
SELECT CASE contype WHEN 'p' THEN 'PK' WHEN 'f' THEN 'FK' WHEN 'u' THEN 'UNIQUE' WHEN 'c' THEN 'CHECK' END AS type,
       count(*) AS curated,
       (SELECT count(*) FROM pg_constraint m
         WHERE m.connamespace = 'mart'::regnamespace AND m.contype = c.contype
           AND m.conrelid IN (SELECT format('mart.%I', table_name)::regclass FROM dq.model_tables)) AS mart
FROM pg_constraint c
WHERE c.connamespace = 'curated'::regnamespace
GROUP BY contype
ORDER BY 1;

\echo '=== 8. Attributs volontairement non reliés au TP1 : couverture par le référentiel'
SELECT 'movies.original_language -> languages' AS attribut,
       count(original_language) AS renseignes,
       count(*) FILTER (WHERE original_language IN (SELECT iso_639_1 FROM mart.languages)) AS dans_referentiel
FROM mart.movies
UNION ALL
SELECT 'production_companies.origin_country -> countries',
       count(*) FILTER (WHERE btrim(origin_country) <> ''),
       count(*) FILTER (WHERE origin_country IN (SELECT iso_3166_1 FROM mart.countries))
FROM mart.production_companies;

\echo '=== 9. Rapprochement TMDB x IMDb : couverture de la source 2'
SELECT count(*)                                            AS films,
       count(imdb_id)                                      AS avec_imdb_id,
       count(*) FILTER (WHERE has_imdb_match)              AS avec_note_imdb,
       count(imdb_start_year)                              AS avec_title_basics,
       round(100.0 * count(*) FILTER (WHERE has_imdb_match) / nullif(count(*), 0), 1) AS couverture_pct
FROM mart.movies;
