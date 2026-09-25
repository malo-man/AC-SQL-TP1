"""Socle commun aux jobs PySpark : configuration, session Spark, audit des exécutions."""

import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType

import psycopg
from pyspark.sql import SparkSession

JDBC_JAR = "/opt/jars/postgresql.jar"

log = logging.getLogger("spark")


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@dataclass(frozen=True)
class Settings:
    datalake: Path
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str

    @property
    def raw_tmdb(self) -> Path:
        return self.datalake / "raw" / "tmdb" / "movies"

    @property
    def raw_imdb(self) -> Path:
        return self.datalake / "raw" / "imdb"

    @property
    def aggregated(self) -> Path:
        return self.datalake / "aggregated" / "movies"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}"

    @property
    def jdbc_properties(self) -> dict[str, str]:
        return {
            "user": self.pg_user,
            "password": self.pg_password,
            "driver": "org.postgresql.Driver",
        }

    @property
    def conninfo(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )


def load_settings() -> Settings:
    """Lit la configuration depuis les variables d'environnement."""
    return Settings(
        datalake=Path(os.getenv("DATALAKE_DIR", "/datalake")),
        pg_host=os.getenv("POSTGRES_HOST", "postgres"),
        pg_port=int(os.getenv("POSTGRES_PORT", "5432")),
        pg_db=os.getenv("POSTGRES_DB", "tmdb"),
        pg_user=os.getenv("POSTGRES_USER", "tmdb"),
        pg_password=os.getenv("POSTGRES_PASSWORD", "tmdb"),
    )


def build_session(app_name: str) -> SparkSession:
    """Ouvre une session Spark locale configurée pour le pipeline."""
    session = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        # Driver JDBC embarqué dans l'image, ajouté au classpath du driver et des exécuteurs
        .config("spark.jars", JDBC_JAR)
        .config("spark.driver.extraClassPath", JDBC_JAR)
        .config("spark.executor.extraClassPath", JDBC_JAR)
        # Seules les partitions présentes en sortie sont réécrites : relancer le
        # job ne détruit pas l'historique des journées précédentes.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        # 200 partitions de mélange (défaut Spark) pour quelques milliers de films
        # ne créent que de la surcharge : un jeu de cette taille tient sur 8.
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        # Le petit côté d'une jointure peut être diffusé : title.ratings tient
        # largement dans cette limite une fois compressé en mémoire.
        .config("spark.sql.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        # Pas d'interface web : un port de moins à publier et à superviser
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    return session


# ---------------------------------------------------------------------
# Data Lake
# ---------------------------------------------------------------------


def partitions(path: Path) -> list[Path]:
    """Liste les partitions ingest_date=... d'un dataset, de la plus ancienne à la plus récente."""
    if not path.is_dir():
        return []
    return sorted(p for p in path.iterdir() if p.is_dir() and p.name.startswith("ingest_date="))


def latest_partition(path: Path) -> Path | None:
    """Partition la plus récente d'un dataset, ou None s'il n'a jamais été collecté.

    Ne lire que la dernière partition est essentiel côté IMDb : le dataset est
    republié en entier chaque jour, lire toutes les partitions multiplierait
    chaque titre par le nombre de journées collectées.
    """
    found = partitions(path)
    return found[-1] if found else None


def has_data(path: Path) -> bool:
    """Indique si un dataset contient au moins un fichier de données exploitable."""
    return any(
        f.is_file() and not f.name.startswith(("_", "."))
        for part in partitions(path)
        for f in part.iterdir()
    )


# ---------------------------------------------------------------------
# Audit des exécutions (table mart.load_runs)
# ---------------------------------------------------------------------


@dataclass
class LoadRun:
    """Trace une exécution de job dans mart.load_runs.

    C'est cette table qui alimente l'indicateur Raw vs Clean : plutôt que
    d'exposer des métriques depuis un job batch (que Prometheus ne trouverait
    jamais en train de tourner), chaque exécution laisse son bilan en base, que
    postgres-exporter transforme ensuite en métriques.
    """

    settings: Settings
    job: str
    run_id: uuid.UUID = field(default_factory=uuid.uuid4)
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    counts: dict[str, int | None] = field(default_factory=dict)
    status: str = "failed"
    message: str | None = None
    ingest_date: str | None = None

    def __enter__(self) -> "LoadRun":
        return self

    def succeed(self, message: str | None = None, **counts: int | None) -> None:
        """Marque l'exécution comme réussie et enregistre ses compteurs."""
        self.status = "success"
        self.message = message
        self.counts.update(counts)

    def skip(self, message: str) -> None:
        """Marque l'exécution comme sans objet (rien à traiter)."""
        self.status = "skipped"
        self.message = message

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc is not None:
            self.status = "failed"
            self.message = f"{exc_type.__name__}: {exc}"[:500]
        finished_at = datetime.now(UTC)
        try:
            with psycopg.connect(self.settings.conninfo, autocommit=True) as conn:
                conn.execute(
                    """
                    INSERT INTO mart.load_runs (
                        run_id, job, ingest_date, raw_rows_in, distinct_movies_in,
                        clean_rows_out, rejected_rows, imdb_matched,
                        started_at, finished_at, duration_seconds, status, message
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        self.run_id,
                        self.job,
                        self.ingest_date,
                        self.counts.get("raw_rows_in"),
                        self.counts.get("distinct_movies_in"),
                        self.counts.get("clean_rows_out"),
                        self.counts.get("rejected_rows"),
                        self.counts.get("imdb_matched"),
                        self.started_at,
                        finished_at,
                        (finished_at - self.started_at).total_seconds(),
                        self.status,
                        self.message,
                    ),
                )
            log.info("Exécution %s : %s %s", self.job, self.status, self.counts or "")
        except psycopg.Error:
            # Un échec d'écriture de l'audit ne doit pas masquer le résultat du job
            log.exception("Bilan de l'exécution non enregistré")
