"""Source 1 : interrogation continue de l'API TMDB et publication dans Kafka.

Le producer parcourt en boucle les listes TMDB configurées (populaires, mieux notés),
récupère la fiche complète de chaque film et publie **le JSON brut** dans le topic Kafka.

Aucune validation ni filtrage ici : les modèles Pydantic du TP1 ignorent les champs
inconnus et en supprimeraient donc, ce qui est incompatible avec une zone raw fidèle
à la source. Le nettoyage est le travail de Spark, en aval.

Message publié (clé = identifiant du film, pour que tous les états d'un film
atterrissent dans la même partition) :

    {"source": "tmdb", "endpoint": "/movie/278", "movie_id": 278,
     "fetched_at": "...", "schema_version": 1, "payload": {...}}

Lancement : `python tmdb_producer.py`.
"""

import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from confluent_kafka import KafkaException, Producer
from prometheus_client import Counter, Gauge, start_http_server

JOB = "tmdb-producer"
BASE_URL = "https://api.themoviedb.org/3"
SCHEMA_VERSION = 1

log = logging.getLogger(JOB)

API_REQUESTS = Counter(
    "pipeline_api_requests_total", "Appels à l'API source", ["job", "endpoint", "status"]
)
MESSAGES_PRODUCED = Counter(
    "pipeline_kafka_messages_produced_total", "Messages publiés dans Kafka", ["job", "topic"]
)
ERRORS = Counter("pipeline_errors_total", "Erreurs rencontrées par le service", ["job"])
LAST_SUCCESS = Gauge(
    "pipeline_last_success_timestamp_seconds", "Horodatage du dernier succès", ["job"]
)
CURSOR_PAGE = Gauge("pipeline_producer_page", "Page TMDB en cours de collecte", ["list"])


@dataclass(frozen=True)
class Settings:
    token: str
    language: str
    lists: list[str]
    max_pages: int
    max_movies: int
    interval_seconds: float
    bootstrap: str
    topic: str
    state_file: Path
    metrics_port: int


def load_settings() -> Settings:
    """Lit la configuration depuis les variables d'environnement."""
    lists = os.getenv("TMDB_LISTS", "popular,top_rated")
    return Settings(
        token=os.getenv("TMDB_READ_ACCESS_TOKEN", ""),
        language=os.getenv("TMDB_LANGUAGE", "fr-FR"),
        lists=[name.strip() for name in lists.split(",") if name.strip()],
        max_pages=int(os.getenv("TMDB_MAX_PAGES", "10")),
        max_movies=int(os.getenv("TMDB_MAX_MOVIES", "0")),
        interval_seconds=float(os.getenv("TMDB_POLL_INTERVAL_SECONDS", "1.0")),
        bootstrap=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
        topic=os.getenv("KAFKA_TOPIC", "tmdb.movies.raw"),
        state_file=Path(os.getenv("PRODUCER_STATE_FILE", "/state/cursor.json")),
        metrics_port=int(os.getenv("METRICS_PORT", "8000")),
    )


class Cursor:
    """Position de la collecte, conservée sur disque pour reprendre après un redémarrage."""

    def __init__(self, path: Path, lists: list[str]) -> None:
        self._path = path
        self._lists = lists
        self.list_name = lists[0]
        self.page = 1
        self.index = 0
        self._load()

    def _load(self) -> None:
        try:
            saved = json.loads(self._path.read_text())
        except (OSError, ValueError):
            return
        # Une liste retirée de la configuration ne doit pas bloquer le producer
        if saved.get("list") in self._lists:
            self.list_name = saved["list"]
            self.page = int(saved.get("page", 1))
            self.index = int(saved.get("index", 0))
            log.info("Reprise à %s page %d, film %d", self.list_name, self.page, self.index)

    def save(self) -> None:
        """Écrit la position de façon atomique (fichier temporaire puis renommage)."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps({"list": self.list_name, "page": self.page, "index": self.index})
        )
        tmp.rename(self._path)

    def next_page(self, total_pages: int, max_pages: int) -> None:
        """Passe à la page suivante, ou à la liste suivante quand la fin est atteinte."""
        self.index = 0
        self.page += 1
        if self.page <= min(total_pages, max_pages):
            return
        # Liste épuisée : on enchaîne sur la suivante, puis on reboucle indéfiniment
        position = self._lists.index(self.list_name)
        self.list_name = self._lists[(position + 1) % len(self._lists)]
        self.page = 1
        log.info("Liste épuisée, passage à %s", self.list_name)


class TmdbClient:
    """Appels à l'API TMDB, avec gestion de la limitation de débit (HTTP 429)."""

    def __init__(self, settings: Settings) -> None:
        self._http = httpx.Client(
            base_url=BASE_URL,
            headers={"Authorization": f"Bearer {settings.token}", "accept": "application/json"},
            params={"language": settings.language},
            timeout=httpx.Timeout(15.0),
        )

    def close(self) -> None:
        self._http.close()

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Effectue un GET en réessayant tant que TMDB nous demande de ralentir."""
        while True:
            response = self._http.get(path, params=params)
            API_REQUESTS.labels(JOB, path.split("/")[1], str(response.status_code)).inc()
            if response.status_code == 429:
                delay = float(response.headers.get("retry-after", "5"))
                log.warning("429 de TMDB, nouvelle tentative dans %.0f s", delay)
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response.json()


def build_message(movie_id: int, payload: dict[str, Any]) -> bytes:
    """Construit l'enveloppe JSON publiée dans Kafka autour du payload TMDB intact."""
    envelope = {
        "source": "tmdb",
        "endpoint": f"/movie/{movie_id}",
        "movie_id": movie_id,
        "fetched_at": datetime.now(UTC).isoformat(),
        "schema_version": SCHEMA_VERSION,
        "payload": payload,
    }
    return json.dumps(envelope, ensure_ascii=False).encode()


def on_delivery(error: Any, message: Any) -> None:
    """Callback de confirmation d'écriture côté broker."""
    if error is not None:
        ERRORS.labels(JOB).inc()
        log.error("Message non publié : %s", error)
        return
    MESSAGES_PRODUCED.labels(JOB, message.topic()).inc()
    LAST_SUCCESS.labels(JOB).set(time.time())


class Runner:
    """Boucle de collecte, interruptible proprement par SIGTERM (docker compose down)."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.running = True
        self.produced = 0
        signal.signal(signal.SIGTERM, self._stop)
        signal.signal(signal.SIGINT, self._stop)

    def _stop(self, *_: Any) -> None:
        log.info("Arrêt demandé, vidage du tampon Kafka en cours")
        self.running = False

    def run(self, client: TmdbClient, producer: Producer, cursor: Cursor) -> None:
        settings = self.settings
        while self.running:
            try:
                page = client.get(f"/movie/{cursor.list_name}", page=cursor.page)
            except httpx.HTTPError:
                # Coupure réseau ou incident TMDB : on réessaie sans faire tomber le service
                ERRORS.labels(JOB).inc()
                log.exception("Liste %s inaccessible, nouvelle tentative dans 30 s", cursor.list_name)
                time.sleep(30)
                continue
            results = page.get("results", [])
            CURSOR_PAGE.labels(cursor.list_name).set(cursor.page)
            log.info(
                "Liste %s, page %d/%d : %d films",
                cursor.list_name,
                cursor.page,
                min(page.get("total_pages", 1), settings.max_pages),
                len(results),
            )

            while self.running and cursor.index < len(results):
                movie_id = results[cursor.index]["id"]
                try:
                    payload = client.get(f"/movie/{movie_id}", append_to_response="credits")
                    producer.produce(
                        settings.topic,
                        key=str(movie_id).encode(),
                        value=build_message(movie_id, payload),
                        on_delivery=on_delivery,
                    )
                    self.produced += 1
                except (httpx.HTTPError, KafkaException, BufferError):
                    ERRORS.labels(JOB).inc()
                    log.exception("Film %s ignoré", movie_id)

                cursor.index += 1
                cursor.save()
                # Laisse le client Kafka traiter les accusés de réception en attente
                producer.poll(0)

                if settings.max_movies and self.produced >= settings.max_movies:
                    log.info("Limite de %d films atteinte, arrêt", settings.max_movies)
                    self.running = False
                    return
                time.sleep(settings.interval_seconds)

            if self.running:
                cursor.next_page(page.get("total_pages", 1), settings.max_pages)
                cursor.save()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings()
    if not settings.token:
        log.error("TMDB_READ_ACCESS_TOKEN est vide : renseigne-le dans .env")
        sys.exit(1)

    start_http_server(settings.metrics_port)
    log.info("Métriques Prometheus exposées sur le port %d", settings.metrics_port)

    producer = Producer(
        {
            "bootstrap.servers": settings.bootstrap,
            # Publication fiable : accusé de tous les réplicas et pas de doublon en cas de retry
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "gzip",
            "linger.ms": 50,
            "client.id": JOB,
        }
    )
    client = TmdbClient(settings)
    cursor = Cursor(settings.state_file, settings.lists)
    runner = Runner(settings)
    try:
        runner.run(client, producer, cursor)
    finally:
        remaining = producer.flush(30)
        if remaining:
            log.error("%d messages non confirmés à l'arrêt", remaining)
        client.close()
        log.info("%d films publiés durant cette exécution", runner.produced)


if __name__ == "__main__":
    main()
