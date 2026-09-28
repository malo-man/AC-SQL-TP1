"""Contrôle qualité des données chargées dans le mart (TP3).

Le job exécute les scripts SQL numérotés de quality/sql, dans l'ordre et dans
une seule transaction :

    00_dq_schema        socle du schéma dq (matrice, résultats, journal)
    01_matrice_controles catalogue des contrôles
    02_audit            contrôles sur le mart : phase « avant »
    03_nettoyage        construction du schéma curated, corrections journalisées
    04_schema_cible     contraintes du schéma cible, qui valident chaque ligne
    05_recontrole       mêmes contrôles sur curated : phase « après »

Tout ou rien : si une contrainte du schéma cible refuse une ligne, la
transaction est annulée et le schéma curated précédent reste en place. Les
scripts sont du SQL pur, sans méta-commande psql : on peut aussi les jouer à
la main, dans une même session psql.

Le job prend le verrou du pipeline : le contrôle des liaisons obsolètes lit
les tables de transit _stg_, que le chargement vide puis remplit en plusieurs
transactions.

Lancement : `python quality.py` (s'efface si un autre job tourne) ou
`python quality.py --wait` (attend son tour, c'est ce que fait quality/run.sh).
"""

import argparse
import os
import sys
from pathlib import Path

import psycopg

from common import LoadRun, load_settings, log, pipeline_lock, setup_logging

JOB = "quality"
SQL_DIR = Path(os.getenv("QUALITY_SQL_DIR", "/quality/sql"))
# L'agrégation avec title.basics peut prendre quelques minutes
WAIT_SECONDS = float(os.getenv("QUALITY_WAIT_SECONDS", "600"))


def scripts() -> list[Path]:
    """Scripts de l'étape, dans l'ordre de leur numéro ; rapports/ est exclu."""
    return sorted(SQL_DIR.glob("[0-9][0-9]_*.sql"))


def run_scripts(conninfo: str, files: list[Path]) -> tuple:
    """Joue les scripts dans une transaction et renvoie le bilan de l'exécution."""
    with psycopg.connect(conninfo) as conn:
        for path in files:
            log.info("Exécution de %s", path.name)
            # Sans paramètre, psycopg envoie le texte tel quel : un fichier peut
            # contenir plusieurs ordres et des blocs DO.
            conn.execute(path.read_text(encoding="utf-8"))
        summary = conn.execute(
            """
            SELECT r.mart_ingest_date, r.mart_movies, r.curated_movies,
                   r.anomalies_before, r.anomalies_after, r.corrections,
                   (SELECT count(*) FROM dq.controls),
                   (SELECT count(*) FROM curated.movies WHERE has_imdb_match)
            FROM dq.runs r
            WHERE r.run_id = dq.current_run()
            """
        ).fetchone()
        conn.commit()
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit, nettoyage et recontrôle du mart")
    parser.add_argument(
        "--wait", action="store_true", help="attendre la fin d'un job en cours au lieu de s'effacer"
    )
    args = parser.parse_args()

    setup_logging()
    settings = load_settings()
    files = scripts()
    if not files:
        log.error("Aucun script dans %s : volume quality/sql non monté ?", SQL_DIR)
        return 1

    wait = WAIT_SECONDS if args.wait else 0
    with LoadRun(settings, JOB) as run, pipeline_lock(settings, JOB, wait) as acquired:
        if not acquired:
            log.warning("Un autre job du pipeline est en cours, %s est ignoré", JOB)
            run.skip("exécution concurrente")
            return 0

        ingest_date, mart, curated, before, after, corrections, controls, matched = run_scripts(
            settings.conninfo, files
        )
        run.ingest_date = ingest_date.isoformat() if ingest_date else None
        message = (
            f"{controls} contrôles : {before} anomalies avant, {after} après, "
            f"{corrections} corrections"
        )
        log.info("%s ; %d films dans le mart, %d dans curated", message, mart, curated)
        run.succeed(
            message=message,
            raw_rows_in=mart,
            distinct_movies_in=mart,
            clean_rows_out=curated,
            rejected_rows=mart - curated,
            imdb_matched=matched,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
