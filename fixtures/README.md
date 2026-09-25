# Fixtures

Mini Data Lake servant à exécuter les jobs PySpark **sans réseau et en quelques secondes**,
pendant le développement ou en revue de code.

```
fixtures/datalake/
├── raw/tmdb/movies/ingest_date=2026-09-20/part-fixture.jsonl
└── raw/imdb/title.ratings/ingest_date=2026-09-20/title.ratings.tsv
```

Les messages TMDB sont de vrais messages produits par le pipeline, dont les crédits ont été
tronqués pour rester lisibles. Le jeu contient volontairement trois pièges :

| Ligne | Piège | Comportement attendu |
|---|---|---|
| `969681` présent deux fois | doublon, la seconde occurrence a un `fetched_at` plus récent et une note de 9.9 | une seule ligne en sortie, celle à 9.9 |
| `999999999` | titre `null` | ligne rejetée, comptée dans `rejected_rows`, sans faire échouer le job |
| `tt27165187` | absent de `title.ratings` | `has_imdb_match = false`, notes IMDb à `null` |

Le fichier IMDb contient aussi une ligne dont les valeurs sont `\N` (convention IMDb pour
« absent ») afin de vérifier qu'elle est bien lue comme `null` et non comme la chaîne `"\N"`.

## Utilisation

```bash
# Agrégation sur les fixtures : écrit dans fixtures/datalake/aggregated (ignoré par git)
docker compose run --rm --no-deps \
  -v "$PWD/fixtures/datalake:/fixtures" -e DATALAKE_DIR=/fixtures \
  spark-jobs python aggregate.py
```
