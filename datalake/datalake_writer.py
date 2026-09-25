"""Consommation du topic Kafka et dépôt des messages bruts dans le Data Lake.

Les messages sont écrits tels qu'ils ont été publiés, en JSON Lines compressé,
dans la zone raw partitionnée par date d'ingestion :

    <DATALAKE_DIR>/raw/tmdb/movies/ingest_date=YYYY-MM-DD/part-HHMMSS-xxxxxxxx.jsonl.gz
    <DATALAKE_DIR>/raw/tmdb/movies/ingest_date=YYYY-MM-DD/_part-HHMMSS-xxxxxxxx.json

Garantie de livraison : l'offset Kafka n'est validé qu'**après** le renommage du
fichier et l'écriture de son manifeste. Un arrêt brutal fait donc au pire relire
des messages déjà écrits (at-least-once) ; les doublons sont éliminés par Spark,
alors qu'une perte de message serait, elle, irrattrapable.

Lancement : `python datalake_writer.py`.
"""

import gzip
import hashlib
import json
import logging
import os
import signal
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from prometheus_client import Counter, Gauge, start_http_server

JOB = "datalake-writer"
SOURCE = "tmdb"
DATASET = "movies"

log = logging.getLogger(JOB)

MESSAGES_CONSUMED = Counter(
    "pipeline_kafka_messages_consumed_total", "Messages lus depuis Kafka", ["job", "topic"]
)
FILES_WRITTEN = Counter(
    "pipeline_raw_files_written_total", "Fichiers déposés dans la zone raw", ["source", "dataset"]
)
ROWS_WRITTEN = Counter(
    "pipeline_raw_rows_written_total", "Lignes déposées dans la zone raw", ["source", "dataset"]
)
CONSUMER_LAG = Gauge(
    "pipeline_kafka_consumer_lag", "Messages publiés mais pas encore écrits dans le lac",
    ["job", "topic", "partition"],
)
ERRORS = Counter("pipeline_errors_total", "Erreurs rencontrées par le service", ["job"])
LAST_SUCCESS = Gauge(
    "pipeline_last_success_timestamp_seconds", "Horodatage du dernier succès", ["job"]
)


@dataclass(frozen=True)
class Settings:
    raw_dir: Path
    bootstrap: str
    topic: str
    group_id: str
    batch_size: int
    flush_seconds: float
    metrics_port: int


def load_settings() -> Settings:
    """Lit la configuration depuis les variables d'environnement."""
    datalake_dir = Path(os.getenv("DATALAKE_DIR", "/datalake"))
    return Settings(
        raw_dir=datalake_dir / "raw" / SOURCE / DATASET,
        bootstrap=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
        topic=os.getenv("KAFKA_TOPIC", "tmdb.movies.raw"),
        group_id=os.getenv("KAFKA_GROUP_ID", "datalake-writer"),
        batch_size=int(os.getenv("WRITER_BATCH_SIZE", "200")),
        flush_seconds=float(os.getenv("WRITER_FLUSH_SECONDS", "60")),
        metrics_port=int(os.getenv("METRICS_PORT", "8000")),
    )


class Batch:
    """Messages accumulés en mémoire entre deux écritures dans le lac."""

    def __init__(self) -> None:
        self.values: list[bytes] = []
        self.offsets: dict[int, tuple[int, int]] = {}
        self.started_at = time.monotonic()

    def add(self, message: Any) -> None:
        self.values.append(message.value())
        partition, offset = message.partition(), message.offset()
        first, _ = self.offsets.get(partition, (offset, offset))
        self.offsets[partition] = (first, offset)

    def is_ready(self, settings: Settings) -> bool:
        """Un lot part dès qu'il est assez gros, ou assez vieux (débit faible)."""
        if not self.values:
            return False
        return (
            len(self.values) >= settings.batch_size
            or time.monotonic() - self.started_at >= settings.flush_seconds
        )


def write_batch(batch: Batch, settings: Settings, topic: str) -> Path:
    """Écrit le lot en JSON Lines gzip puis son manifeste, et renvoie le fichier produit."""
    written_at = datetime.now(UTC)
    target_dir = settings.raw_dir / f"ingest_date={written_at.date().isoformat()}"
    target_dir.mkdir(parents=True, exist_ok=True)

    stem = f"part-{written_at.strftime('%H%M%S')}-{uuid.uuid4().hex[:8]}"
    target = target_dir / f"{stem}.jsonl.gz"
    # Fichier temporaire caché : Spark ignore les noms commençant par "." et ne
    # lira donc jamais une écriture en cours.
    tmp = target_dir / f".{stem}.jsonl.gz.part"

    sha256 = hashlib.sha256()
    try:
        with gzip.open(tmp, "wb") as f:
            for value in batch.values:
                line = value + b"\n"
                f.write(line)
                sha256.update(line)
        size = tmp.stat().st_size
        tmp.rename(target)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise

    manifest = {
        "source": SOURCE,
        "dataset": DATASET,
        "topic": topic,
        "offsets": {str(p): {"first": f, "last": l} for p, (f, l) in batch.offsets.items()},
        "written_at": written_at.isoformat(),
        "format": "jsonl+gzip",
        "rows": len(batch.values),
        "size_bytes": size,
        "sha256": sha256.hexdigest(),
    }
    (target_dir / f"_{stem}.json").write_text(json.dumps(manifest, indent=2))
    return target


def publish_lag(consumer: Consumer, topic: str) -> None:
    """Expose le retard du consumer : messages publiés mais pas encore dans le lac."""
    for assignment in consumer.assignment():
        try:
            _, high = consumer.get_watermark_offsets(assignment, timeout=5, cached=False)
            position = consumer.position([assignment])[0].offset
        except KafkaException:
            continue
        # offset < 0 : aucune position connue pour l'instant (lot pas encore validé)
        current = position if position and position >= 0 else high
        CONSUMER_LAG.labels(JOB, topic, str(assignment.partition)).set(max(high - current, 0))


class Runner:
    """Boucle de consommation, interruptible proprement par SIGTERM."""

    def __init__(self) -> None:
        self.running = True
        signal.signal(signal.SIGTERM, self._stop)
        signal.signal(signal.SIGINT, self._stop)

    def _stop(self, *_: Any) -> None:
        log.info("Arrêt demandé, écriture du lot en cours")
        self.running = False

    def flush(self, consumer: Consumer, batch: Batch, settings: Settings) -> Batch:
        """Écrit le lot dans le lac puis valide les offsets, dans cet ordre."""
        if not batch.values:
            return batch
        target = write_batch(batch, settings, settings.topic)
        # Les offsets ne sont validés qu'une fois la donnée en sécurité sur disque
        consumer.commit(asynchronous=False)
        FILES_WRITTEN.labels(SOURCE, DATASET).inc()
        ROWS_WRITTEN.labels(SOURCE, DATASET).inc(len(batch.values))
        LAST_SUCCESS.labels(JOB).set(time.time())
        log.info("%d messages écrits dans %s", len(batch.values), target)
        return Batch()

    def run(self, consumer: Consumer, settings: Settings) -> None:
        batch = Batch()
        last_lag_check = 0.0
        while self.running:
            message = consumer.poll(1.0)
            if message is not None:
                if message.error():
                    # La fin de partition n'est pas une erreur : il n'y a juste rien de neuf
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        ERRORS.labels(JOB).inc()
                        log.error("Erreur Kafka : %s", message.error())
                else:
                    batch.add(message)
                    MESSAGES_CONSUMED.labels(JOB, message.topic()).inc()

            if batch.is_ready(settings):
                batch = self.flush(consumer, batch, settings)

            if time.monotonic() - last_lag_check > 15:
                publish_lag(consumer, settings.topic)
                last_lag_check = time.monotonic()

        # Arrêt demandé : on ne perd pas le lot en cours
        self.flush(consumer, batch, settings)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings()
    start_http_server(settings.metrics_port)
    log.info("Métriques Prometheus exposées sur le port %d", settings.metrics_port)

    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap,
            "group.id": settings.group_id,
            # Un nouveau groupe repart du début du topic : aucun message déjà publié n'est perdu
            "auto.offset.reset": "earliest",
            # Validation manuelle : c'est l'écriture dans le lac qui décide, pas une horloge
            "enable.auto.commit": False,
            "client.id": JOB,
        }
    )
    consumer.subscribe([settings.topic])
    log.info("Abonné au topic %s, écriture dans %s", settings.topic, settings.raw_dir)

    runner = Runner()
    try:
        runner.run(consumer, settings)
    finally:
        consumer.close()
        log.info("Consumer fermé")


if __name__ == "__main__":
    main()
