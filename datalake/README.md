# Data Lake

Le Data Lake stocke toutes les données collectées **avant traitement**, pour pouvoir les
retraiter à tout moment (nouveau job Spark, correction d'un bug, audit d'une valeur).

## Solution retenue : un volume Docker partagé

Le Data Lake est un **système de fichiers** : le volume Docker nommé `datalake`, déclaré dans
`docker-compose.yml` et monté sur `/datalake` dans chaque conteneur qui en a besoin.

```
                    volume Docker « datalake »
                    ┌──────────────────────────┐
imdb-fetcher ─────► │ /datalake/raw/imdb/…     │
(consumer Kafka) ─► │ /datalake/raw/tmdb/…     │ ─────► Spark (lecture seule)
(agrégation) ─────► │ /datalake/aggregated/…   │
                    └──────────────────────────┘
```

Pourquoi pas un stockage objet type MinIO/S3 :

- le volume de données reste modeste (quelques centaines de Mo par jour) et tient sur un seul hôte ;
- Spark lit un dossier local nativement, sans connecteur `s3a` ni jars Hadoop-AWS supplémentaires ;
- un service de moins à démarrer, configurer, sécuriser et superviser ;
- le volume Docker est persistant : il survit à `docker compose down` et aux redémarrages.

Limite assumée : un seul hôte, pas d'accès distant ni de réplication. Pour passer à l'échelle,
la même arborescence pourrait être reprise telle quelle dans un bucket S3.

## Arborescence

```
/datalake
├── raw/                          données brutes, identiques à la source
│   ├── imdb/                     source 2 — datasets IMDb (service imdb-fetcher)
│   │   ├── title.ratings/
│   │   │   └── ingest_date=2026-09-25/
│   │   │       ├── title.ratings.tsv.gz
│   │   │       └── _manifest.json
│   │   └── title.basics/
│   │       └── ingest_date=2026-09-25/…
│   └── tmdb/                     source 1 — messages de l'API TMDB lus depuis Kafka (à venir)
│       └── ingest_date=YYYY-MM-DD/…
└── aggregated/                   rapprochement TMDB ⟷ IMDb, entrée du job PySpark (à venir)
    └── ingest_date=YYYY-MM-DD/…
```

| Zone          | Contenu                                           | Écrit par             | Lu par  |
|---------------|---------------------------------------------------|-----------------------|---------|
| `raw/imdb`    | TSV gzip IMDb tels que téléchargés                | `imdb-fetcher`        | Agrégation, Spark |
| `raw/tmdb`    | Messages JSON de l'API TMDB tels que reçus        | consumer Kafka        | Agrégation, Spark |
| `aggregated`  | Films TMDB enrichis des données IMDb (`imdb_id` = `tconst`) | agrégation  | Spark   |

Les données **propres** ne sont pas dans le Data Lake : Spark les charge directement dans
PostgreSQL.

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
5. **Un manifeste par partition** (`_manifest.json`) qui trace l'origine du fichier : URL,
   date de collecte, date de mise à jour côté source, taille, empreinte SHA-256 et nombre de
   lignes. C'est la base de l'indicateur Raw vs Clean et de l'audit des données.
6. **Idempotence.** Relancer une collecte le même jour ne duplique pas les données : une
   partition déjà complète est ignorée.

## Lire le Data Lake depuis Spark

Le service Spark monte le même volume, en lecture seule :

```yaml
  spark:
    volumes:
      - datalake:/datalake:ro
```

Les fichiers `.tsv.gz` sont décompressés automatiquement par Spark :

```python
ratings = (
    spark.read
    .option("sep", "\t")
    .option("header", True)
    .option("nullValue", "\\N")
    .csv("/datalake/raw/imdb/title.ratings")        # toutes les partitions
)
# colonnes : tconst, averageRating, numVotes, ingest_date

latest = ratings.where("ingest_date = '2026-09-25'")  # une seule journée
```

## Consulter le Data Lake

Le volume n'est pas un dossier du dépôt : on y accède via un conteneur.

```bash
# Lister les fichiers
docker compose run --rm imdb-fetcher find /datalake -type f

# Lire un manifeste
docker compose run --rm imdb-fetcher sh -c 'cat /datalake/raw/imdb/title.ratings/*/_manifest.json'

# Aperçu d'un fichier brut
docker compose run --rm imdb-fetcher sh -c 'zcat /datalake/raw/imdb/title.ratings/*/title.ratings.tsv.gz | head'

# Taille occupée par zone
docker compose run --rm imdb-fetcher du -sh /datalake/raw/* /datalake/aggregated
```

## Sauvegarde et réinitialisation

```bash
# Archiver le Data Lake dans ./datalake-backup.tar.gz
docker compose run --rm -v "$PWD":/backup imdb-fetcher \
  tar czf /backup/datalake-backup.tar.gz -C /datalake .

# Tout effacer (Data Lake ET base PostgreSQL) — irréversible
docker compose down -v
```

`docker compose down` sans `-v` arrête les services **sans** toucher aux données.

## Rétention

Aucune purge automatique pour l'instant : chaque jour ajoute environ 210 Mo de fichiers IMDb.
Si l'espace devient un problème, supprimer les anciennes partitions `ingest_date=…` de la zone
raw (ce qui reste compatible avec la règle 1 tant que les données ont été chargées en base).
