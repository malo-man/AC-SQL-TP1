"""Provisionnement de Metabase par son API REST.

Metabase n'a pas de mécanisme de configuration par fichiers comme Grafana :
tout se fait normalement à la souris. Ce script rejoue ces clics via l'API,
pour que « docker compose up -d » suffise vraiment et que le dashboard soit
versionné dans le dépôt plutôt que piégé dans un volume.

Il est idempotent : chaque objet est recherché par son nom avant d'être créé,
donc un second passage met à jour au lieu de dupliquer.

Lancement : `python provision.py`.
"""

import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any

import httpx

from questions import DASHBOARD_DESCRIPTION, DASHBOARD_NAME, DATABASE_NAME, QUESTIONS

log = logging.getLogger("metabase-init")


@dataclass(frozen=True)
class Settings:
    base_url: str
    email: str
    password: str
    first_name: str
    last_name: str
    site_name: str
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    boot_timeout: float


def load_settings() -> Settings:
    return Settings(
        base_url=os.getenv("METABASE_URL", "http://metabase:3000"),
        email=os.getenv("METABASE_EMAIL", "admin@tp2.local"),
        password=os.getenv("METABASE_PASSWORD", "TP2metabase!"),
        first_name=os.getenv("METABASE_FIRST_NAME", "Admin"),
        last_name=os.getenv("METABASE_LAST_NAME", "TP2"),
        site_name=os.getenv("METABASE_SITE_NAME", "TP2 — Plateforme Data"),
        pg_host=os.getenv("POSTGRES_HOST", "postgres"),
        pg_port=int(os.getenv("POSTGRES_PORT", "5432")),
        pg_db=os.getenv("POSTGRES_DB", "tmdb"),
        pg_user=os.getenv("POSTGRES_USER", "tmdb"),
        pg_password=os.getenv("POSTGRES_PASSWORD", "tmdb"),
        boot_timeout=float(os.getenv("METABASE_BOOT_TIMEOUT", "600")),
    )


def wait_until_ready(client: httpx.Client, timeout: float) -> None:
    """Metabase applique ses migrations au premier démarrage : cela prend une minute ou deux."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            response = client.get("/api/health")
            if response.status_code == 200:
                log.info("Metabase est prêt")
                return
        except httpx.HTTPError:
            pass
        time.sleep(5)
    raise RuntimeError(f"Metabase n'a pas démarré en {timeout:.0f} s")


def log_in(client: httpx.Client, settings: Settings) -> httpx.Response:
    """Ouvre une session avec le compte administrateur."""
    log.info("Instance déjà installée : connexion")
    response = client.post(
        "/api/session", json={"username": settings.email, "password": settings.password}
    )
    response.raise_for_status()
    return response


def authenticate(client: httpx.Client, settings: Settings) -> None:
    """Crée le compte administrateur au premier passage, se connecte aux suivants."""
    properties = client.get("/api/session/properties").json()

    # « setup-token » ne suffit pas à décider : Metabase continue de le publier
    # après l'installation, tout en refusant de le réutiliser (403).
    if properties.get("has-user-setup"):
        response = log_in(client, settings)
    else:
        log.info("Première installation : création du compte administrateur")
        response = client.post(
            "/api/setup",
            json={
                "token": properties.get("setup-token"),
                "user": {
                    "first_name": settings.first_name,
                    "last_name": settings.last_name,
                    "email": settings.email,
                    "password": settings.password,
                    "site_name": settings.site_name,
                },
                "prefs": {
                    "site_name": settings.site_name,
                    "site_locale": "fr",
                    "allow_tracking": False,
                },
            },
        )
        if response.status_code == 403:
            # Installation faite entre-temps (démarrage simultané, propriété absente)
            response = log_in(client, settings)
        else:
            response.raise_for_status()

    session_id = response.json().get("id")
    if session_id:
        # Les versions récentes posent aussi un cookie ; l'en-tête reste accepté
        client.headers["X-Metabase-Session"] = session_id


def ensure_database(client: httpx.Client, settings: Settings) -> int:
    """Déclare la connexion à PostgreSQL, restreinte au schéma mart."""
    existing = client.get("/api/database").json()
    databases = existing.get("data", existing) if isinstance(existing, dict) else existing
    for database in databases:
        if database["name"] == DATABASE_NAME:
            log.info("Connexion « %s » déjà déclarée (id %s)", DATABASE_NAME, database["id"])
            return database["id"]

    log.info("Déclaration de la connexion « %s »", DATABASE_NAME)
    response = client.post(
        "/api/database",
        json={
            "name": DATABASE_NAME,
            "engine": "postgres",
            "details": {
                "host": settings.pg_host,
                "port": settings.pg_port,
                "dbname": settings.pg_db,
                "user": settings.pg_user,
                "password": settings.pg_password,
                "ssl": False,
                # Les données propres du pipeline uniquement : le schéma public
                # appartient au TP1 et n'a pas à encombrer l'explorateur.
                "schema-filters-type": "inclusion",
                "schema-filters-patterns": "mart",
            },
        },
    )
    response.raise_for_status()
    database_id = response.json()["id"]
    client.post(f"/api/database/{database_id}/sync_schema")
    return database_id


def ensure_cards(client: httpx.Client, database_id: int) -> list[tuple[int, dict]]:
    """Crée ou met à jour chaque question, et renvoie leurs identifiants."""
    existing = {card["name"]: card["id"] for card in client.get("/api/card").json()}
    cards: list[tuple[int, dict]] = []

    for question in QUESTIONS:
        payload = {
            "name": question["name"],
            "description": question["description"],
            "display": question["display"],
            "type": "question",
            "dataset_query": {
                "type": "native",
                "database": database_id,
                "native": {"query": question["sql"], "template-tags": {}},
            },
            "visualization_settings": question["visualization_settings"],
        }
        card_id = existing.get(question["name"])
        if card_id:
            client.put(f"/api/card/{card_id}", json=payload).raise_for_status()
            log.info("Question mise à jour : %s", question["name"])
        else:
            response = client.post("/api/card", json=payload)
            response.raise_for_status()
            card_id = response.json()["id"]
            log.info("Question créée : %s", question["name"])
        cards.append((card_id, question))
    return cards


def ensure_dashboard(client: httpx.Client, cards: list[tuple[int, dict]]) -> int:
    """Crée le dashboard et y dispose les questions selon leur position déclarée."""
    existing = client.get("/api/dashboard").json()
    dashboard_id = next((d["id"] for d in existing if d["name"] == DASHBOARD_NAME), None)

    if dashboard_id is None:
        response = client.post(
            "/api/dashboard", json={"name": DASHBOARD_NAME, "description": DASHBOARD_DESCRIPTION}
        )
        response.raise_for_status()
        dashboard_id = response.json()["id"]
        log.info("Dashboard créé (id %s)", dashboard_id)

    # Les cartes sont réécrites en bloc : le dépôt décrit la mise en page
    dashcards = [
        {
            "id": -(index + 1),  # identifiant négatif = nouvelle carte
            "card_id": card_id,
            "row": question["layout"]["row"],
            "col": question["layout"]["col"],
            "size_x": question["layout"]["size_x"],
            "size_y": question["layout"]["size_y"],
            "series": [],
            "parameter_mappings": [],
            "visualization_settings": {},
        }
        for index, (card_id, question) in enumerate(cards)
    ]
    client.put(f"/api/dashboard/{dashboard_id}", json={"dashcards": dashcards}).raise_for_status()
    log.info("%d questions placées sur le dashboard", len(dashcards))
    return dashboard_id


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings()

    with httpx.Client(base_url=settings.base_url, timeout=60.0) as client:
        wait_until_ready(client, settings.boot_timeout)
        authenticate(client, settings)
        database_id = ensure_database(client, settings)
        cards = ensure_cards(client, database_id)
        dashboard_id = ensure_dashboard(client, cards)

    log.info(
        "Dashboard « %s » disponible : %s/dashboard/%s",
        DASHBOARD_NAME,
        os.getenv("METABASE_PUBLIC_URL", "http://localhost:3001"),
        dashboard_id,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (httpx.HTTPError, RuntimeError, KeyError):
        log.exception("Provisionnement de Metabase en échec")
        sys.exit(1)
