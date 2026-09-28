-- =====================================================================
-- TP3 — Recontrôle (phase « après »), bilan et rétention.
--
-- Les mêmes contrôles, exactement, sont repassés sur le schéma curated :
-- la comparaison avant/après porte donc sur des mesures identiques.
-- Attendu : chaque anomalie corrigée tombe à zéro ; ne restent que les
-- anomalies « conservées », dont la matrice justifie le maintien.
-- =====================================================================

SELECT control_id, checked_rows, anomaly_rows, anomaly_rate, passed
FROM dq.run_controls('curated', 'après');

UPDATE dq.runs
SET finished_at     = clock_timestamp(),
    curated_movies  = (SELECT count(*) FROM curated.movies),
    anomalies_after = (SELECT sum(anomaly_rows) FROM dq.results
                       WHERE run_id = dq.current_run() AND phase = 'après'),
    corrections     = (SELECT count(*) FROM dq.corrections WHERE run_id = dq.current_run())
WHERE run_id = dq.current_run();

-- Rétention : le détail ligne à ligne ne sert qu'à la dernière exécution
-- (l'audit de référence est archivé en CSV par quality/run.sh) ; les bilans
-- par contrôle sont gardés 30 jours pour suivre les tendances.
DELETE FROM dq.anomalies   WHERE run_id <> dq.current_run();
DELETE FROM dq.corrections WHERE run_id <> dq.current_run();
DELETE FROM dq.runs        WHERE started_at < now() - interval '30 days';

-- Bilan : aucun contrôle ne doit rester « à traiter ».
SELECT verdict, count(*) AS controls, sum(anomalies_before) AS anomalies_before,
       sum(anomalies_after) AS anomalies_after
FROM dq.v_results_last
GROUP BY verdict
ORDER BY verdict;
