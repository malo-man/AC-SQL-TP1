#!/bin/sh
# Contrôle qualité à la demande (TP3) : exécute l'étape « quality » du
# pipeline, puis archive ses résultats en CSV et les rapports de lecture.
#
#   quality/run.sh                        résultats dans quality/resultats/
#   quality/run.sh <dossier de sortie>    résultats dans le dossier indiqué
#
# L'étape tourne déjà après chaque chargement Spark : ce script sert à la
# déclencher sans attendre le prochain cycle et à figer ses résultats. Les
# fichiers sont écrits côté hôte, ils appartiennent donc à l'utilisateur.
#
# Prérequis : la plateforme est démarrée (docker compose up -d).
set -eu
cd "$(dirname "$0")/.."

OUT="${1:-quality/resultats}"
mkdir -p "$OUT"

# psql du conteneur postgres, avec les identifiants de son environnement
psql() {
    docker compose exec -T postgres \
        sh -c 'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -X -q -v ON_ERROR_STOP=1 -P pager=off "$@"' \
        psql "$@"
}

csv() {
    psql --csv -c "$2" > "$OUT/$1.csv"
    echo "   $OUT/$1.csv"
}

echo "== Audit, nettoyage et recontrôle (attend la fin d'un cycle Spark en cours)"
docker compose exec -T spark-jobs python /app/quality.py --wait

echo "== Exports CSV"
csv execution "SELECT * FROM dq.v_last_run"
csv matrice_controles "
    SELECT control_id, dimension, table_name, column_name, rule, severity, threshold,
           treatment, justification
    FROM dq.controls ORDER BY control_id"
csv avant_apres "
    SELECT control_id, dimension, table_name, column_name, rule, severity, threshold, treatment,
           checked_before, anomalies_before, pct_before, checked_after, anomalies_after, pct_after,
           priority, verdict
    FROM dq.v_results_last ORDER BY control_id"
csv scores_dimensions "SELECT coalesce(dimension, 'global') AS dimension, phase, controls, controls_passed, anomalies, score
                       FROM dq.v_dimension_scores"
csv corrections "SELECT * FROM dq.v_corrections_summary"
csv volumes "SELECT * FROM dq.v_volumes"
csv impact_indicateurs "SELECT * FROM dq.v_kpi_impact"
csv impact_genres "SELECT * FROM dq.v_genre_impact"
# Dix exemples par contrôle : le détail complet reste interrogeable dans dq.anomalies
csv exemples_anomalies "
    SELECT control_id, phase, record_key, observed_value
    FROM (SELECT a.*, row_number() OVER (PARTITION BY a.control_id, a.phase ORDER BY a.record_key) AS n
            FROM dq.anomalies a JOIN dq.v_last_run r USING (run_id)) s
    WHERE n <= 10
    ORDER BY control_id, phase DESC, record_key"

echo "== Rapports"
for report in quality/sql/rapports/*.sql; do
    name="$(basename "$report" .sql)"
    psql -f - < "$report" > "$OUT/$name.txt"
    echo "   $OUT/$name.txt"
done

echo "== Bilan"
psql -c "SELECT verdict, count(*) AS controles, sum(anomalies_before) AS anomalies_avant,
                sum(anomalies_after) AS anomalies_apres
         FROM dq.v_results_last GROUP BY verdict ORDER BY verdict"
