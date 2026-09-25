# Source 2 — Datasets publics IMDb

## Rôle dans le projet

L'API TMDB (source 1) fournit le catalogue de films, leur popularité et leur note TMDB.
IMDb apporte un **second point de vue sur la réception des films** : la note moyenne et le
nombre de votes IMDb, ainsi que des métadonnées de référence (année, durée, genres).

Le rapprochement se fait sur l'identifiant IMDb, déjà présent côté TMDB :

```
TMDB movies.imdb_id  ⟷  IMDb tconst   (ex. tt0111161)
```

Cas d'usage métier : comparer les notes TMDB et IMDb, repérer les films populaires mais mal
notés (ou l'inverse), pondérer la note par le nombre de votes, contrôler la cohérence des
durées et des années de sortie entre les deux sources.

## Origine et licence

- Documentation : https://developer.imdb.com/non-commercial-datasets/
- Téléchargement : https://datasets.imdbws.com/
- Fichiers régénérés **chaque jour** par IMDb.
- Licence : usage personnel et **non commercial** uniquement (cadre pédagogique de ce TP).

## Format

TSV (séparateur tabulation) compressé en gzip, UTF-8, première ligne = en-tête.
Les valeurs manquantes sont codées `\N` (à lire avec `nullValue="\\N"` dans Spark).

### `title.ratings.tsv.gz` (~9 Mo, ~1,7 M lignes)

| Colonne         | Type    | Description                   |
|-----------------|---------|-------------------------------|
| `tconst`        | string  | Identifiant IMDb du titre     |
| `averageRating` | decimal | Note moyenne pondérée (1–10)  |
| `numVotes`      | integer | Nombre de votes               |

### `title.basics.tsv.gz` (~200 Mo, ~12 M lignes)

| Colonne          | Type    | Description                                              |
|------------------|---------|----------------------------------------------------------|
| `tconst`         | string  | Identifiant IMDb du titre                                |
| `titleType`      | string  | `movie`, `short`, `tvSeries`, `tvEpisode`…               |
| `primaryTitle`   | string  | Titre principal                                          |
| `originalTitle`  | string  | Titre original                                           |
| `isAdult`        | 0/1     | Contenu adulte                                           |
| `startYear`      | YYYY    | Année de sortie                                          |
| `endYear`        | YYYY    | Année de fin (séries), `\N` sinon                        |
| `runtimeMinutes` | integer | Durée en minutes                                         |
| `genres`         | string  | Jusqu'à 3 genres séparés par des virgules                |

Ces fichiers couvrent tout IMDb (séries, épisodes, courts métrages…) : le filtrage sur
`titleType = 'movie'` et la jointure avec TMDB sont faits en aval, pas à la collecte.

## Mécanisme de collecte

Le service `imdb-fetcher` (`imdb_fetcher.py`) :

1. télécharge chaque dataset configuré toutes les `IMDB_FETCH_INTERVAL_HOURS` heures (24 par défaut) ;
2. l'écrit **sans transformation** dans la zone raw du Data Lake, partitionnée par date d'ingestion ;
3. écrit d'abord un fichier caché `.<dataset>.tsv.gz.part` puis le renomme, pour qu'aucun lecteur ne voie un fichier incomplet ;
4. ignore un dataset déjà récupéré le jour même (redémarrage sans re-téléchargement) ;
5. en cas d'échec, réessaie 15 minutes plus tard.

```
datalake/raw/imdb/
├── title.ratings/
│   └── ingest_date=2026-09-25/
│       ├── title.ratings.tsv.gz
│       └── _manifest.json
└── title.basics/
    └── ingest_date=2026-09-25/
        ├── title.basics.tsv.gz
        └── _manifest.json
```

Le `_manifest.json` trace chaque téléchargement : URL, date de collecte, `Last-Modified` côté
IMDb, taille, empreinte SHA-256 et nombre de lignes.

## Configuration

| Variable                    | Défaut                       | Rôle                                  |
|-----------------------------|------------------------------|---------------------------------------|
| `IMDB_DATASETS`             | `title.ratings,title.basics` | Datasets à télécharger                |
| `IMDB_FETCH_INTERVAL_HOURS` | `24`                         | Intervalle entre deux collectes       |
| `IMDB_METRICS_PORT`         | `9101`                       | Port local des métriques Prometheus   |

## Métriques Prometheus

Exposées sur `http://imdb-fetcher:8000/metrics` (réseau Docker) et `http://localhost:9101/metrics`.

| Métrique                               | Type    | Description                                   |
|----------------------------------------|---------|-----------------------------------------------|
| `imdb_raw_rows{dataset}`               | gauge   | Lignes du dernier fichier brut (**Raw** côté Raw vs Clean) |
| `imdb_last_success_timestamp_seconds`  | gauge   | Date du dernier fichier brut disponible       |
| `imdb_files_downloaded_total`          | counter | Fichiers téléchargés                          |
| `imdb_bytes_downloaded_total`          | counter | Octets téléchargés                            |
| `imdb_fetch_errors_total`              | counter | Échecs de téléchargement                      |

## Utilisation

```bash
docker compose up -d imdb-fetcher
docker compose logs -f imdb-fetcher

# Collecte ponctuelle (ex. uniquement le petit fichier)
docker compose run --rm -e IMDB_DATASETS=title.ratings imdb-fetcher python imdb_fetcher.py --once

# Contenu du Data Lake
docker compose run --rm imdb-fetcher find /datalake -type f
```
