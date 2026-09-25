"""Exposition du contenu du Data Lake sous forme de métriques Prometheus.

Le service parcourt les manifestes déposés par les collecteurs et publie le
volume réellement présent dans le lac. C'est le côté « Raw » de l'indicateur
Raw vs Clean, le côté « Clean » venant de PostgreSQL via postgres-exporter.

Pourquoi un service dédié plutôt que des compteurs dans chaque collecteur :
un compteur de processus repart de zéro à chaque redémarrage et ne décrit que
ce que *ce* processus a écrit, alors qu'on veut connaître l'état du lac. En ne
lisant que les manifestes (quelques Ko), la mesure reste instantanée même quand
le lac pèse plusieurs centaines de Mo.

Lancement : `python datalake_exporter.py`.
"""

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from prometheus_client import Counter, Gauge, start_http_server

JOB = "datalake-exporter"

log = logging.getLogger(JOB)

RAW_ROWS = Gauge(
    "pipeline_raw_rows", "Lignes brutes présentes dans le Data Lake", ["source", "dataset"]
)
RAW_ROWS_LATEST = Gauge(
    "pipeline_raw_rows_latest",
    "Lignes brutes de la dernière partition collectée",
    ["source", "dataset"],
)
RAW_BYTES = Gauge(
    "pipeline_raw_bytes", "Octets occupés par la zone raw", ["source", "dataset"]
)
RAW_FILES = Gauge(
    "pipeline_raw_files", "Fichiers de données de la zone raw", ["source", "dataset"]
)
RAW_PARTITIONS = Gauge(
    "pipeline_raw_partitions", "Partitions ingest_date de la zone raw", ["source", "dataset"]
)
RAW_LAST_INGEST = Gauge(
    "pipeline_raw_last_ingest_timestamp_seconds",
    "Date de la dernière donnée brute déposée",
    ["source", "dataset"],
)
AGGREGATED_BYTES = Gauge("pipeline_aggregated_bytes", "Octets de la zone agrégée", ["dataset"])
AGGREGATED_FILES = Gauge("pipeline_aggregated_files", "Fichiers de la zone agrégée", ["dataset"])
AGGREGATED_PARTITIONS = Gauge(
    "pipeline_aggregated_partitions", "Partitions de la zone agrégée", ["dataset"]
)
ERRORS = Counter("pipeline_errors_total", "Erreurs rencontrées par le service", ["job"])
LAST_SUCCESS = Gauge(
    "pipeline_last_success_timestamp_seconds", "Horodatage du dernier succès", ["job"]
)


@dataclass(frozen=True)
class Settings:
    datalake: Path
    interval_seconds: float
    metrics_port: int


def load_settings() -> Settings:
    return Settings(
        datalake=Path(os.getenv("DATALAKE_DIR", "/datalake")),
        interval_seconds=float(os.getenv("EXPORTER_INTERVAL_SECONDS", "30")),
        metrics_port=int(os.getenv("METRICS_PORT", "8000")),
    )


def partitions(dataset_dir: Path) -> list[Path]:
    """Partitions ingest_date=... d'un dataset, de la plus ancienne à la plus récente."""
    return sorted(
        p for p in dataset_dir.iterdir() if p.is_dir() and p.name.startswith("ingest_date=")
    )


def read_timestamp(manifest: dict) -> float | None:
    """Date de production du fichier, quel que soit le collecteur qui l'a écrit."""
    stamp = manifest.get("written_at") or manifest.get("fetched_at")
    if not stamp:
        return None
    try:
        return datetime.fromisoformat(stamp).timestamp()
    except ValueError:
        return None


def scan_dataset(dataset_dir: Path) -> dict[str, float]:
    """Additionne le contenu d'un dataset à partir de ses manifestes."""
    totals = {"rows": 0.0, "rows_latest": 0.0, "bytes": 0.0, "files": 0.0, "partitions": 0.0}
    last_ingest = 0.0
    found = partitions(dataset_dir)
    totals["partitions"] = float(len(found))

    for partition in found:
        rows = 0.0
        for entry in partition.iterdir():
            if not entry.is_file():
                continue
            if entry.name.startswith("_") and entry.name.endswith(".json"):
                manifest = json.loads(entry.read_text())
                rows += float(manifest.get("rows", 0))
                last_ingest = max(last_ingest, read_timestamp(manifest) or 0.0)
            elif not entry.name.startswith((".", "_")):
                totals["files"] += 1
                totals["bytes"] += entry.stat().st_size
        totals["rows"] += rows
        if partition is found[-1]:
            totals["rows_latest"] = rows

    totals["last_ingest"] = last_ingest
    return totals


def scan_raw(settings: Settings) -> None:
    """Publie le volume de chaque dataset de la zone raw."""
    raw = settings.datalake / "raw"
    if not raw.is_dir():
        return
    for source_dir in sorted(p for p in raw.iterdir() if p.is_dir()):
        for dataset_dir in sorted(p for p in source_dir.iterdir() if p.is_dir()):
            labels = (source_dir.name, dataset_dir.name)
            totals = scan_dataset(dataset_dir)
            RAW_ROWS.labels(*labels).set(totals["rows"])
            RAW_ROWS_LATEST.labels(*labels).set(totals["rows_latest"])
            RAW_BYTES.labels(*labels).set(totals["bytes"])
            RAW_FILES.labels(*labels).set(totals["files"])
            RAW_PARTITIONS.labels(*labels).set(totals["partitions"])
            if totals["last_ingest"]:
                RAW_LAST_INGEST.labels(*labels).set(totals["last_ingest"])


def scan_aggregated(settings: Settings) -> None:
    """Publie le volume de la zone agrégée (Parquet, sans manifeste)."""
    aggregated = settings.datalake / "aggregated"
    if not aggregated.is_dir():
        return
    for dataset_dir in sorted(p for p in aggregated.iterdir() if p.is_dir()):
        files = [
            f
            for f in dataset_dir.rglob("*")
            if f.is_file() and not f.name.startswith((".", "_"))
        ]
        AGGREGATED_FILES.labels(dataset_dir.name).set(len(files))
        AGGREGATED_BYTES.labels(dataset_dir.name).set(sum(f.stat().st_size for f in files))
        AGGREGATED_PARTITIONS.labels(dataset_dir.name).set(len(partitions(dataset_dir)))


def collect(settings: Settings) -> None:
    """Un passage complet sur le lac."""
    # Les séries sont remises à zéro à chaque passage : un dataset supprimé du
    # lac doit disparaître des métriques, pas rester figé sur sa dernière valeur.
    for gauge in (
        RAW_ROWS, RAW_ROWS_LATEST, RAW_BYTES, RAW_FILES, RAW_PARTITIONS, RAW_LAST_INGEST,
        AGGREGATED_BYTES, AGGREGATED_FILES, AGGREGATED_PARTITIONS,
    ):
        gauge.clear()
    scan_raw(settings)
    scan_aggregated(settings)
    LAST_SUCCESS.labels(JOB).set(time.time())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings()
    start_http_server(settings.metrics_port)
    log.info(
        "Métriques du lac %s exposées sur le port %d (toutes les %.0f s)",
        settings.datalake,
        settings.metrics_port,
        settings.interval_seconds,
    )
    while True:
        try:
            collect(settings)
        except (OSError, ValueError):
            ERRORS.labels(JOB).inc()
            log.exception("Passage sur le Data Lake en échec")
        time.sleep(settings.interval_seconds)


if __name__ == "__main__":
    main()
