# Synthèse pour la restitution orale — TP3

## Le message en une phrase

Les données du TP2 « passaient » : pas une clé cassée, pas une note hors échelle. Pourtant le
dashboard affichait une note TMDB moyenne fausse de 0,9 point, parce qu'une absence de vote était
enregistrée comme une note de 0. L'audit l'a mesuré, le nettoyage l'a corrigé, puis la correction
a été remontée dans le pipeline pour que l'erreur ne se reproduise plus.

## Chiffres clés

| | |
|---|---|
| Données auditées | 619 films, 62 270 personnes, 83 401 crédits |
| Contrôles | **49**, sur les 5 dimensions (complétude 11, unicité 6, validité 16, cohérence 10, intégrité 6) |
| Anomalies dans le mart | **56 951**, aucune critique |
| Après nettoyage | **28 250**, toutes conservées volontairement et justifiées |
| Contrôles « à traiter » | **0** |
| Score global | 95,8 % → 97,2 % ; validité 96,9 % → **100 %** |
| Corrections journalisées | 28 591 lignes, 10 contrôles |
| Effet métier | note TMDB moyenne **6,57 → 7,46**, documentaires 2,08 → 7,62 |
| Après correction à la source | le mart lui-même tombe à 28 258 anomalies ; le nettoyage SQL n'a plus que 8 imputations à faire |

## Déroulé (4 minutes environ)

1. **Cartographie** (30 s) — Le dictionnaire du TP1 confronté à la base : les 13 relations du MLD
   sont bien là dans les trois schémas, mais trois promesses du dictionnaire ne sont pas tenues
   par le pipeline (pays vide en NULL, liaisons à jour, référentiels toujours reliés).
2. **Matrice** (45 s) — Une matrice exécutable : chaque contrôle est une ligne de table avec sa
   règle, sa sévérité, son seuil, son traitement, sa justification et ses requêtes. Les mêmes
   contrôles tournent sur le mart puis sur les données nettoyées.
3. **Anomalies** (1 min) — Classement par importance (sévérité × taux), pas par volume. Trois
   exemples : les notes 0 (74 films, effet de 0,9 point), les pays vides (127 sociétés), les
   liaisons que le chargement ne retire jamais (5 rôles fantômes sur *Avengers : Doomsday*).
4. **Nettoyage** (1 min) — Deux règles : ne jamais inventer (une valeur fausse devient NULL, pas
   une estimation) et ne jamais perdre un film. Une seule imputation : la durée IMDb, valeur
   mesurée par l'autre source. Tout est journalisé, et les contraintes du schéma cible, posées
   après coup, prouvent la conformité.
5. **Avant / après et correction à la source** (45 s) — Validité à 100 %, 0 contrôle à traiter.
   Puis les corrections remontées dans Spark : le mart est juste, et le dashboard du TP2 avec lui.

## Démonstration (si le temps le permet)

```bash
quality/run.sh                                   # audit, nettoyage, recontrôle en 4 s
```

- Metabase → dashboard **Qualité des données** : score par dimension, notes par genre avant/après.
- Grafana → **TP3 — Qualité des données** : chute des anomalies du mart après la correction à la
  source.
- En SQL : `SELECT * FROM dq.corrections WHERE control_id = 'COH-01' LIMIT 5;` — la trace de
  chaque correction, ancienne et nouvelle valeur.

## Ce qu'on retient

- **Des données valides ne sont pas des données justes.** Les anomalies qui comptent le plus
  étaient sémantiques (0 = « pas de note »), invisibles pour les contraintes du TP2.
- **Corriger en aval ne suffit pas** : le nettoyage SQL mesure et prouve, la correction à la
  source rend le résultat durable.
- **Journaliser chaque correction** a permis de trouver un défaut du nettoyage lui-même : un
  ordre de suppression qui faisait perdre un rôle, invisible pour un recontrôle.

## Questions probables

| Question | Réponse courte |
|---|---|
| Pourquoi ne pas corriger directement le mart ? | Il est réécrit toutes les 5 minutes, et on perdrait l'état d'origine à comparer. Les corrections durables sont faites dans Spark. |
| Pourquoi ne pas imputer les notes ou les budgets manquants ? | Ce serait présenter une estimation comme une mesure ; NULL dit exactement ce qu'on sait. |
| 28 250 anomalies restantes, c'est beaucoup ? | 97,6 % sont le genre non renseigné des personnes, une donnée personnelle qu'on ne devine pas. Chaque résidu est justifié dans la matrice. |
| Comment ajouter un contrôle ? | Une ligne dans `01_matrice_controles.sql` ; il apparaît dans les résultats, Metabase et Grafana sans autre modification. |
| Que se passe-t-il si le nettoyage oublie un cas ? | La contrainte correspondante du schéma cible refuse la ligne, toute l'exécution est annulée, l'ancien `curated` reste en place, l'échec est visible dans Grafana. |
