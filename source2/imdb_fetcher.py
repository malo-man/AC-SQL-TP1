"""Source 2 : téléchargement périodique des datasets publics IMDb vers le Data Lake.

Les fichiers sont conservés tels quels (TSV compressé gzip) dans la zone raw :

    <DATALAKE_DIR>/raw/imdb/<dataset>/ingest_date=YYYY-MM-DD/<dataset>.tsv.gz
    <DATALAKE_DIR>/raw/imdb/<dataset>/ingest_date=YYYY-MM-DD/_manifest.json

Lancement : `python imdb_fetcher.py` (boucle infinie) ou `python imdb_fetcher.py --once`.
"""

import gzip
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
from prometheus_client import Counter, Gauge, start_http_server

BASE_URL = "https://datasets.imdbws.com"
CHUNK_SIZE = 1024 * 1024
RETRY_DELAY_SECONDS = 15 * 60

log = logging.getLogger("imdb_fetcher")

FILES_DOWNLOADED = Counter(
    "imdb_files_downloaded_total", "Fichiers IMDb téléchargés dans le Data Lake", ["dataset"]
)
BYTES_DOWNLOADED = Counter(
    "imdb_bytes_downloaded_total", "Octets IMDb téléchargés dans le Data Lake", ["dataset"]
)
FETCH_ERRORS = Counter("imdb_fetch_errors_total", "Échecs de téléchargement IMDb", ["dataset"])
RAW_ROWS = Gauge(
    "imdb_raw_rows", "Nombre de lignes (hors en-tête) du dernier fichier brut", ["dataset"]
)
LAST_SUCCESS = Gauge(
    "imdb_last_success_timestamp_seconds",
    "Horodatage du dernier fichier brut disponible",
    ["dataset"],
)


@dataclass(frozen=True)
class Settings:
    raw_dir: Path
    datasets: list[str]
    interval_seconds: float
    metrics_port: int


def load_settings() -> Settings:
    """Lit la configuration depuis les variables d'environnement."""
    datalake_dir = Path(os.getenv("DATALAKE_DIR", "/datalake"))
    datasets = os.getenv("IMDB_DATASETS", "title.ratings,title.basics")
    return Settings(
        raw_dir=datalake_dir / "raw" / "imdb",
        datasets=[d.strip() for d in datasets.split(",") if d.strip()],
        interval_seconds=float(os.getenv("IMDB_FETCH_INTERVAL_HOURS", "24")) * 3600,
        metrics_port=int(os.getenv("METRICS_PORT", "8000")),
    )


def count_rows(path: Path) -> int:
    """Compte les lignes de données d'un TSV gzip (l'en-tête est exclu)."""
    with gzip.open(path, "rb") as f:
        return sum(1 for _ in f) - 1


def publish_metrics(dataset: str, manifest: dict) -> None:
    """Expose dans Prometheus l'état du dernier fichier brut."""
    RAW_ROWS.labels(dataset).set(manifest["rows"])
    LAST_SUCCESS.labels(dataset).set(datetime.fromisoformat(manifest["fetched_at"]).timestamp())


def fetch_dataset(client: httpx.Client, dataset: str, raw_dir: Path) -> None:
    """Télécharge un dataset IMDb dans la partition du jour, sauf s'il y est déjà."""
    fetched_at = datetime.now(UTC)
    target_dir = raw_dir / dataset / f"ingest_date={fetched_at.date().isoformat()}"
    target = target_dir / f"{dataset}.tsv.gz"
    manifest_path = target_dir / "_manifest.json"

    if target.exists() and manifest_path.exists():
        log.info("%s déjà présent dans %s, ignoré", dataset, target_dir)
        publish_metrics(dataset, json.loads(manifest_path.read_text()))
        return

    target_dir.mkdir(parents=True, exist_ok=True)
    # Écriture dans un fichier temporaire caché puis renommage : Spark ignore les
    # fichiers commençant par "." et ne lit donc jamais un téléchargement partiel.
    tmp = target.with_name(f".{target.name}.part")
    sha256 = hashlib.sha256()
    size = 0
    try:
        with client.stream("GET", f"/{dataset}.tsv.gz") as response:
            response.raise_for_status()
            with tmp.open("wb") as f:
                for chunk in response.iter_raw(CHUNK_SIZE):
                    f.write(chunk)
                    sha256.update(chunk)
                    size += len(chunk)
            last_modified = response.headers.get("last-modified")
        rows = count_rows(tmp)
        tmp.rename(target)
    finally:
        tmp.unlink(missing_ok=True)

    manifest = {
        "source": "imdb",
        "dataset": dataset,
        "url": f"{BASE_URL}/{dataset}.tsv.gz",
        "fetched_at": fetched_at.isoformat(),
        "source_last_modified": last_modified,
        "format": "tsv+gzip",
        "null_value": "\\N",
        "size_bytes": size,
        "sha256": sha256.hexdigest(),
        "rows": rows,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))

    FILES_DOWNLOADED.labels(dataset).inc()
    BYTES_DOWNLOADED.labels(dataset).inc(size)
    publish_metrics(dataset, manifest)
    log.info("%s téléchargé : %d lignes, %.1f Mo -> %s", dataset, rows, size / 1e6, target)


def run_once(client: httpx.Client, settings: Settings) -> bool:
    """Récupère tous les datasets configurés. Renvoie False si au moins un a échoué."""
    ok = True
    for dataset in settings.datasets:
        try:
            fetch_dataset(client, dataset, settings.raw_dir)
        except (httpx.HTTPError, OSError):
            log.exception("Échec du téléchargement de %s", dataset)
            FETCH_ERRORS.labels(dataset).inc()
            ok = False
    return ok


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings()
    once = "--once" in sys.argv[1:]
    if not once:
        start_http_server(settings.metrics_port)
        log.info("Métriques Prometheus exposées sur le port %d", settings.metrics_port)

    timeout = httpx.Timeout(30.0, read=120.0)
    with httpx.Client(base_url=BASE_URL, follow_redirects=True, timeout=timeout) as client:
        while True:
            ok = run_once(client, settings)
            if once:
                sys.exit(0 if ok else 1)
            delay = settings.interval_seconds if ok else RETRY_DELAY_SECONDS
            log.info("Prochaine collecte dans %.0f minutes", delay / 60)
            time.sleep(delay)


if __name__ == "__main__":
    main()
