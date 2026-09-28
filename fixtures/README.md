# Fixtures

Mini Data Lake servant à exécuter les jobs PySpark **sans réseau et en quelques secondes**,
pendant le développement ou en revue de code.

```
fixtures/datalake/
├── raw/tmdb/movies/ingest_date=2026-09-20/part-fixture.jsonl
└── raw/imdb/title.ratings/ingest_date=2026-09-20/title.ratings.tsv
```

Les messages TMDB sont de vrais messages produits par le pipeline, dont les crédits ont été
tronqués pour rester lisibles. Le jeu contient volontairement plusieurs pièges ; les quatre
derniers reprennent des anomalies relevées par l'audit qualité du TP3 (voir
[quality/rapport-audit.md](../quality/rapport-audit.md)) et corrigées depuis dans le pipeline :

| Ligne | Piège | Comportement attendu |
|---|---|---|
| `969681` présent deux fois | doublon, la seconde occurrence a un `fetched_at` plus récent et une note de 9.9 | une seule ligne en sortie, celle à 9.9 |
| `999999999` | titre `null` | ligne rejetée, comptée dans `rejected_rows`, sans faire échouer le job |
| `tt27165187` | absent de `title.ratings` | `has_imdb_match = false`, notes IMDb à `null` |
| `888888888`, film à venir | aucun vote et `vote_average` à 0, budget de 7 USD | `vote_average`, `budget` et `revenue` à `null` |
| `888888888`, société `999000001` | `origin_country` vide (`""`) | `origin_country` à `null` et non `''` |
| `888888888`, personne `999000002` | `gender` à 0, personnage `""` | `gender` et `character` à `null` |
| `888888888`, crédits `fixture-crew-0` et `-1` | même réalisateur crédité deux fois | un seul crédit chargé |

Le fichier IMDb contient aussi une ligne dont les valeurs sont `\N` (convention IMDb pour
« absent ») afin de vérifier qu'elle est bien lue comme `null` et non comme la chaîne `"\N"`.

## Utilisation

```bash
# Agrégation sur les fixtures : écrit dans fixtures/datalake/aggregated (ignoré par git)
docker compose run --rm --no-deps \
  -v "$PWD/fixtures/datalake:/fixtures" -e DATALAKE_DIR=/fixtures \
  spark-jobs python aggregate.py
```
