-- =====================================================================
-- TP3 — Comparaison avant / après nettoyage (lecture seule).
--
-- Reprend la dernière exécution terminée : contrôles, scores, volumes et
-- effet sur les indicateurs du dashboard. Script psql, exécuté par
-- quality/run.sh.
-- =====================================================================

\echo '=== 1. Exécution'
SELECT run_id, started_at, finished_at - started_at AS duree, mart_ingest_date,
       mart_movies, curated_movies, anomalies_before, anomalies_after, corrections
FROM dq.v_last_run;

\echo '=== 2. Verdicts'
SELECT verdict, count(*) AS controles, sum(anomalies_before) AS anomalies_avant,
       sum(anomalies_after) AS anomalies_apres
FROM dq.v_results_last GROUP BY verdict ORDER BY verdict;

\echo '=== 3. Anomalies détectées, par priorité (sévérité x taux)'
SELECT control_id, severity, rule, anomalies_before AS avant, pct_before AS "% avant",
       anomalies_after AS apres, pct_after AS "% apres", treatment, verdict
FROM dq.v_results_last
WHERE anomalies_before > 0 OR anomalies_after > 0
ORDER BY priority DESC, control_id;

\echo '=== 4. Score par dimension (moyenne des taux de conformité des contrôles)'
SELECT coalesce(a.dimension, 'global') AS dimension,
       a.score AS score_avant, p.score AS score_apres,
       a.controls_passed || '/' || a.controls AS sous_seuil_avant,
       p.controls_passed || '/' || p.controls AS sous_seuil_apres
FROM (SELECT * FROM dq.v_dimension_scores WHERE phase = 'avant') a
JOIN (SELECT * FROM dq.v_dimension_scores WHERE phase = 'après') p
  ON p.dimension IS NOT DISTINCT FROM a.dimension
ORDER BY a.dimension NULLS LAST;

\echo '=== 5. Corrections appliquées'
SELECT action, count(DISTINCT control_id) AS controles, sum(rows_corrected) AS lignes
FROM dq.v_corrections_summary GROUP BY action ORDER BY lignes DESC;

SELECT * FROM dq.v_corrections_summary;

\echo '=== 6. Volumes par table'
SELECT * FROM dq.v_volumes;

\echo '=== 7. Effet sur les indicateurs'
SELECT * FROM dq.v_kpi_impact;

\echo '=== 8. Effet sur la note moyenne par genre (dashboard TP2)'
SELECT * FROM dq.v_genre_impact;
