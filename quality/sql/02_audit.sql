-- =====================================================================
-- TP3 — Audit du mart (phase « avant »).
--
-- Ouvre une nouvelle exécution puis passe chaque contrôle de la matrice sur
-- le schéma mart, tel que le pipeline du TP2 l'a chargé. Le mart n'est que
-- lu : l'état d'origine reste intact et comparable.
--
-- L'identifiant d'exécution est posé dans le paramètre de session
-- dq.run_id, relu par les scripts suivants (dq.current_run()). Pour
-- rejouer l'audit à la main, enchaîner les fichiers dans une même session :
--   psql -1 -f 00_dq_schema.sql -f 01_matrice_controles.sql -f 02_audit.sql ...
-- =====================================================================

SELECT set_config('dq.run_id', gen_random_uuid()::text, false);

INSERT INTO dq.runs (run_id, mart_ingest_date, mart_movies)
SELECT dq.current_run(), max(ingest_date), count(*)
FROM mart.movies;

SELECT control_id, checked_rows, anomaly_rows, anomaly_rate, passed
FROM dq.run_controls('mart', 'avant');

UPDATE dq.runs
SET anomalies_before = (SELECT sum(anomaly_rows) FROM dq.results
                        WHERE run_id = dq.current_run() AND phase = 'avant')
WHERE run_id = dq.current_run();
