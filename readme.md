# Plateforme Data TMDB × IMDb

Projet en deux temps autour d'un même sujet métier, le cinéma :

- **TP1 — Audit, cartographie et modélisation** ([sujet.md](sujet.md)) : étude des sources,
  dictionnaire de données, MCD/MLD, schéma PostgreSQL et application d'import.
- **TP2 — Pipeline temps réel et plateforme Data** ([sujet2.md](sujet2.md)) : la même
  modélisation transformée en plateforme automatisée et observable.

```
API TMDB ──► producer ──► Kafka ──► writer ──┐
                                             ├──► Data Lake ──► PySpark ──► PostgreSQL ──► Metabase
Datasets IMDb ──► fetcher ───────────────────┘
                                        Prometheus ──► Grafana
```

![Architecture](docs/architecture.png)

Le détail des choix techniques et du fonctionnement interne est dans
**[docs/choix-techniques.md](docs/choix-techniques.md)**.

---

## Prérequis

- Docker et Docker Compose v2 (`docker compose version`) ;
- environ **1,6 Go** d'images à télécharger la première fois, **2 Go** d'espace disque ;
- un **Read Access Token TMDB (v4)**, gratuit :
  [themoviedb.org/settings/api](https://www.themoviedb.org/settings/api).

Aucune dépendance Python n'est à installer sur la machine : tout s'exécute en conteneur.

## Démarrage

```bash
cp .env.example .env
# renseigner TMDB_READ_ACCESS_TOKEN dans .env
docker compose up -d
```

C'est tout : le topic Kafka, le schéma de la base et le dashboard Metabase sont créés
automatiquement. Comptez **5 à 10 minutes au premier démarrage** (construction des images,
migrations de Metabase, première collecte IMDb), puis quelques secondes ensuite.

Suivre le démarrage :

```bash
docker compose ps                 # tout doit être "running" ou "healthy"
docker compose logs -f tmdb-producer datalake-writer spark-jobs
```

Les conteneurs `kafka-init`, `db-migrate` et `metabase-init` s'arrêtent après leur travail :
un état `Exited (0)` est normal pour eux.

## Services et accès

Tous les ports sont publiés sur `127.0.0.1` uniquement.

| Service | URL | Identifiants | Rôle |
|---|---|---|---|
| **Metabase** | http://localhost:3001 | `admin@tp2.local` / `TP2metabase!` | Data Viz : dashboard « Pipeline TMDB × IMDb » |
| **Grafana** | http://localhost:3000 | `admin` / `admin` | Supervision : 3 dashboards dans le dossier TP2 |
| **Prometheus** | http://localhost:9090 | — | Métriques brutes et état des cibles |
| PostgreSQL | `localhost:5433` | `tmdb` / `tmdb` | Base `tmdb`, schémas `public` (TP1) et `mart` (TP2) |
| cAdvisor | http://localhost:8081 | — | Métriques des conteneurs |
| Exporters | `:9100` `:9101` `:9102` `:9103` `:9104` `:9187` | — | `/metrics` de chaque service |

Kafka n'est volontairement pas exposé sur l'hôte : il se consulte depuis son conteneur
(voir ci-dessous).

## Suivre une donnée de bout en bout

Le parcours de la démonstration, étape par étape.

### 1. Collecter — l'API est interrogée en continu

```bash
docker compose logs --tail 5 tmdb-producer
```

### 2. Transporter — les messages arrivent dans Kafka

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic tmdb.movies.raw \
    --from-beginning --max-messages 1 | head -c 400

# Nombre de messages dans le topic
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:9092 --topic tmdb.movies.raw
```

### 3. Stocker — les messages sont déposés dans le Data Lake

```bash
docker compose exec datalake-writer find /datalake -type f | head

# Manifeste d'un fichier brut : lignes, taille, empreinte, offsets Kafka
docker compose exec datalake-writer sh -c 'cat /datalake/raw/tmdb/movies/*/_part-*.json | head -20'

# Aperçu d'une donnée brute
docker compose exec datalake-writer sh -c \
    'zcat /datalake/raw/tmdb/movies/*/part-*.jsonl.gz | head -1 | cut -c1-300'
```

### 4 et 5. Transformer et charger — PySpark produit les données propres

```bash
docker compose logs --tail 30 spark-jobs

# Forcer une exécution immédiate sans attendre le prochain cycle
docker compose exec spark-jobs python /app/aggregate.py
docker compose exec spark-jobs python /app/load_mart.py
```

```bash
docker compose exec postgres psql -U tmdb -d tmdb \
    -c "SELECT count(*) FROM mart.movies" \
    -c "SELECT * FROM mart.v_raw_vs_clean" \
    -c "SELECT title, tmdb_rating, imdb_rating, rating_gap FROM mart.v_movies_enriched
        WHERE has_imdb_match ORDER BY abs(rating_gap) DESC LIMIT 5"
```

### 6. Visualiser — le dashboard exploite PostgreSQL

http://localhost:3001 → dashboard **Pipeline TMDB × IMDb**.

### 7. Superviser — le traitement est vérifiable

http://localhost:3000 → dossier **TP2** → dashboard **Pipeline Data (Raw vs Clean)**.

En ligne de commande :

```bash
# Toutes les cibles doivent être "up"
curl -s localhost:9090/api/v1/targets | python3 -m json.tool | grep -E '"(job|health)"'

# Indicateur Raw vs Clean
curl -s 'localhost:9090/api/v1/query?query=pipeline_raw_rows'
curl -s 'localhost:9090/api/v1/query?query=pipeline_clean_rows'
```

## Profils de données

| | Léger (par défaut) | Complet |
|---|---|---|
| `IMDB_DATASETS` | `title.ratings` | `title.ratings,title.basics` |
| Téléchargé | 9 Mo | 237 Mo |
| Apporte | note moyenne et nombre de votes IMDb | + année, durée et genres IMDb |
| Durée d'un cycle Spark | quelques secondes | 1 à 3 minutes |

Le profil léger suffit à toute la chaîne : c'est `title.ratings` qui porte le rapprochement
métier. Pour passer au profil complet, modifier `IMDB_DATASETS` dans `.env` puis :

```bash
docker compose up -d imdb-fetcher
```

Le volume collecté côté TMDB se règle avec `TMDB_POLL_INTERVAL_SECONDS` (1 film par seconde
par défaut) et `TMDB_MAX_PAGES` (5 pages, soit 100 films par liste).

## Commandes utiles

```bash
docker compose ps                          # état des services
docker compose logs -f <service>           # journaux d'un service
docker compose restart <service>
docker compose up -d --build <service>     # après modification du code

# Volume occupé par le Data Lake
docker compose exec datalake-writer du -sh /datalake/raw /datalake/aggregated

# Exécuter les jobs Spark sur les fixtures, sans réseau (voir fixtures/README.md)
docker compose run --rm --no-deps -v "$PWD/fixtures/datalake:/fixtures" \
    -e DATALAKE_DIR=/fixtures spark-jobs python /app/aggregate.py
```

## Dépannage

| Symptôme | Cause probable et solution |
|---|---|
| `tmdb-producer` redémarre en boucle | `TMDB_READ_ACCESS_TOKEN` absent ou invalide dans `.env` |
| `Aucune donnée TMDB dans le lac` | Normal les premières minutes : le writer écrit par lots de 200 messages ou toutes les 60 s |
| `has_imdb_match` à `false` partout | L'agrégation a tourné avant la fin de la première collecte IMDb (environ une minute). Le cycle Spark suivant corrige de lui-même ; pour ne pas attendre : `docker compose exec spark-jobs python /app/aggregate.py` |
| Metabase répond `502` au démarrage | Il applique ses migrations : compter 1 à 2 minutes, `metabase-init` attend automatiquement |
| Port déjà utilisé | Changer le port correspondant dans `.env` (`GRAFANA_PORT`, `METABASE_PORT`...) |
| Cible Prometheus `down` | Le service correspondant est arrêté : `docker compose up -d <service>` |

## Arrêt et réinitialisation

```bash
docker compose down            # arrête tout, conserve les données
docker compose down -v         # EFFACE le Data Lake, la base (TP1 compris) et les dashboards
```

## Application du TP1

Le client interactif développé au TP1 reste utilisable et indépendant du pipeline : il écrit
dans le schéma `public`, que le TP2 ne touche pas.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d postgres
python main.py
```

## Liens

- [Instructions et rendu](https://nowledgeable.com)
- [API TMDB (source 1)](https://developer.themoviedb.org/docs/getting-started)
- [Datasets IMDb (source 2)](https://developer.imdb.com/non-commercial-datasets/) — voir [source2/README.md](source2/README.md)
- [Organisation du Data Lake](datalake/README.md)
- [Choix techniques et fonctionnement](docs/choix-techniques.md)
