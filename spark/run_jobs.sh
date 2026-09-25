#!/bin/sh
# Ordonnanceur du pipeline : agrégation puis chargement, en boucle.
#
# Volontairement minimal. Un Airflow (ou équivalent) demanderait trois services
# supplémentaires, une base de métadonnées et un scheduler, pour orchestrer ici
# un enchaînement linéaire de deux tâches.
#
# L'échec d'un job n'interrompt pas la boucle : il est déjà tracé dans
# mart.load_runs et visible dans Grafana, et la tentative suivante peut réussir
# (fichier en cours d'écriture, base momentanément indisponible...).
set -u

INTERVAL_MINUTES="${SPARK_INTERVAL_MINUTES:-5}"
RUN_ONCE="${SPARK_RUN_ONCE:-0}"

while true; do
    echo "=== $(date -u +%FT%TZ) : agrégation ==="
    python /app/aggregate.py || echo "!!! agrégation en échec, voir mart.load_runs"

    echo "=== $(date -u +%FT%TZ) : chargement dans PostgreSQL ==="
    python /app/load_mart.py || echo "!!! chargement en échec, voir mart.load_runs"

    if [ "$RUN_ONCE" = "1" ]; then
        echo "SPARK_RUN_ONCE=1 : arrêt après une exécution"
        break
    fi

    echo "=== prochaine exécution dans ${INTERVAL_MINUTES} min ==="
    sleep $((INTERVAL_MINUTES * 60))
done
