# Data Lake

Le Data Lake stocke toutes les données collectées **avant traitement**, pour pouvoir les
retraiter à tout moment (nouveau job Spark, correction d'un bug, audit d'une valeur).

Ce dossier contient aussi le service qui alimente la zone raw côté TMDB :
[`datalake_writer.py`](datalake_writer.py), qui consomme le topic Kafka.

## Solution retenue : un volume Docker partagé

Le Data Lake est un **système de fichiers** : le volume Docker nommé `datalake`, déclaré dans
`docker-compose.yml` et monté sur `/datalake` dans chaque conteneur qui en a besoin.

```
                       volume Docker « datalake »
                       ┌──────────────────────────┐
imdb-fetcher ────────► │ /datalake/raw/imdb/…     │
datalake-writer ─────► │ /datalake/raw/tmdb/…     │ ──► spark-jobs (aggregate.py)
spark-jobs ──────────► │ /datalake/aggregated/…   │ ──► spark-jobs (load_mart.py)
                       └──────────────────────────┘
                                   │
                                   └──► datalake-exporter (lecture seule, métriques)
```

Pourquoi pas un stockage objet type MinIO/S3 :

- le volume de données reste modeste (quelques dizaines de Mo par jour en profil léger) et
  tient sur un seul hôte ;
- Spark lit un dossier local nativement, sans connecteur `s3a` ni jars Hadoop-AWS supplémentaires ;
- un service de moins à démarrer, configurer, sécuriser et superviser ;
- le volume Docker est persistant : il survit à `docker compose down` et aux redémarrages.

Limite assumée : un seul hôte, pas d'accès distant ni de réplication. Pour passer à l'échelle,
la même arborescence pourrait être reprise telle quelle dans un bucket S3.

## Arborescence

```
/datalake
├── raw/                          données brutes, identiques à la source
│   ├── tmdb/                     source 1 — messages de l'API TMDB lus depuis Kafka
│   │   └── movies/
│   │       └── ingest_date=2026-09-25/
│   │           ├── part-110640-2ee4d9d1.jsonl.gz
│   │           ├── _part-110640-2ee4d9d1.json
│   │           ├── part-110740-9f13c0ab.jsonl.gz
│   │           └── _part-110740-9f13c0ab.json
│   └── imdb/                     source 2 — datasets IMDb (service imdb-fetcher)
│       └── title.ratings/
│           └── ingest_date=2026-09-25/
│               ├── title.ratings.tsv.gz
│               └── _manifest.json
└── aggregated/                   rapprochement TMDB ⟷ IMDb, entrée du chargement
    └── movies/
        └── ingest_date=2026-09-25/
            └── *.parquet
```

| Zone          | Contenu                                           | Écrit par         | Lu par  |
|---------------|---------------------------------------------------|-------------------|---------|
| `raw/tmdb`    | Messages JSON de l'API TMDB tels que reçus        | `datalake-writer` | `aggregate.py` |
| `raw/imdb`    | TSV gzip IMDb tels que téléchargés                | `imdb-fetcher`    | `aggregate.py` |
| `aggregated`  | Films TMDB nettoyés et enrichis des données IMDb (`imdb_id` = `tconst`) | `aggregate.py` | `load_mart.py` |

Les données **propres** ne sont pas dans le Data Lake : `load_mart.py` les charge directement
dans le schéma `mart` de PostgreSQL.

## Règles d'écriture

Tout service qui écrit dans le Data Lake respecte ces conventions :

1. **La zone raw n'est jamais modifiée.** Une donnée brute est écrite une fois, telle que reçue,
   et n'est ni corrigée ni supprimée. Tout nettoyage se fait en aval.
2. **Partitionnement par date d'ingestion** (UTC), au format Hive `ingest_date=YYYY-MM-DD`.
   Spark reconnaît ce format et ajoute automatiquement une colonne `ingest_date`, ce qui permet
   de ne traiter qu'une journée ou de suivre l'historique.
3. **Écriture atomique.** On écrit dans un fichier temporaire caché (`.<nom>.part`) puis on le
   renomme. Le renommage est instantané : un lecteur voit soit rien, soit le fichier complet.
4. **Fichiers techniques préfixés par `_` ou `.`** (`_manifest.json`, `.xxx.part`). Spark les
   ignore quand il lit un dossier : seules les données sont chargées.
5. **Un manifeste par fichier de données**, qui trace son origine : source, date d'écriture,
   format, taille, empreinte SHA-256, nombre de lignes, et selon le collecteur l'URL et la date
   de mise à jour côté source (IMDb) ou les offsets Kafka couverts (TMDB). C'est la base de
   l'indicateur Raw vs Clean et de l'audit des données.
   IMDb écrit un fichier par partition, donc un seul `_manifest.json` ; le writer TMDB écrit
   plusieurs fichiers par jour, donc un manifeste `_<nom du fichier>.json` pour chacun.
6. **Idempotence.** Relancer une collecte le même jour ne duplique pas les données : côté IMDb
   une partition déjà complète est ignorée, côté agrégation la partition du jour est remplacée.

## Lire le Data Lake depuis Spark

Le service Spark monte le même volume :

```yaml
  spark-jobs:
    volumes:
      - datalake:/datalake
```

Les fichiers `.gz` sont décompressés automatiquement par Spark :

```python
# Notes IMDb : une seule partition, la plus récente (le dataset est republié en entier
# chaque jour, lire tout l'historique multiplierait chaque titre)
ratings = (
    spark.read
    .option("sep", "\t")
    .option("header", True)
    .option("nullValue", "\\N")
    .csv("/datalake/raw/imdb/title.ratings/ingest_date=2026-09-25")
)

# Messages TMDB : toutes les partitions, avec le schéma déclaré dans spark/schemas.py
raw = spark.read.schema(ENVELOPE).json("/datalake/raw/tmdb/movies")
# colonnes : source, endpoint, movie_id, fetched_at, schema_version, payload, ingest_date
```

## Consulter le Data Lake

Le volume n'est pas un dossier du dépôt : on y accède via un conteneur.

```bash
# Lister les fichiers
docker compose exec datalake-writer find /datalake -type f

# Lire un manifeste
docker compose exec datalake-writer sh -c 'cat /datalake/raw/tmdb/movies/*/_part-*.json'

# Aperçu d'une donnée brute
docker compose exec datalake-writer sh -c \
    'zcat /datalake/raw/tmdb/movies/*/part-*.jsonl.gz | head -1 | cut -c1-300'

# Taille occupée par zone
docker compose exec datalake-writer du -sh /datalake/raw/* /datalake/aggregated
```

Les mêmes informations sont exposées en métriques par `datalake-exporter`
(`pipeline_raw_rows`, `pipeline_raw_bytes`, `pipeline_raw_files`, `pipeline_aggregated_*`)
et visibles dans le dashboard Grafana « Pipeline Data ».

## Sauvegarde et réinitialisation

```bash
# Archiver le Data Lake dans ./datalake-backup.tar.gz
docker compose run --rm --no-deps -v "$PWD":/backup datalake-writer \
    tar czf /backup/datalake-backup.tar.gz -C /datalake .

# Tout effacer (Data Lake ET base PostgreSQL) — irréversible
docker compose down -v
```

`docker compose down` sans `-v` arrête les services **sans** toucher aux données.

## Volumétrie et rétention

| Contenu | Taille |
|---|---|
| `title.ratings.tsv.gz` (profil léger) | 9 Mo par collecte quotidienne |
| `title.basics.tsv.gz` (profil complet, optionnel) | 228 Mo par collecte quotidienne |
| Messages TMDB | environ 13 Ko par film collecté |

Aucune purge automatique pour l'instant. Si l'espace devient un problème, supprimer les
anciennes partitions `ingest_date=…` de la zone raw reste compatible avec la règle 1 dès lors
que les données ont été chargées en base.
