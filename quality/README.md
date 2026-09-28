# Qualité des données (TP3)

Audit de qualité des données produites par le pipeline du TP2, nettoyage vers un schéma cible et
recontrôle, écrits en SQL et intégrés à la plateforme. Sujet : [sujet3.md](../sujet3.md).

## Livrables

| Demandé | Où |
|---|---|
| Cartographie mise à jour | [cartographie.md](cartographie.md), vérifiée par [sql/rapports/cartographie.sql](sql/rapports/cartographie.sql) |
| Matrice de contrôles qualité | [matrice-controles.md](matrice-controles.md), exécutable dans [sql/01_matrice_controles.sql](sql/01_matrice_controles.sql) |
| Scripts SQL d'audit et de nettoyage | [sql/](sql/) : `00` à `05`, plus les rapports de lecture dans `sql/rapports/` |
| Résultats avant / après | [resultats/](resultats/) (audit de référence) et [resultats/apres-correction-source/](resultats/apres-correction-source/) |
| Documentation technique | ce document |
| Anomalies, corrections et justifications | [rapport-audit.md](rapport-audit.md) |
| Synthèse pour la restitution orale | [synthese-restitution.md](synthese-restitution.md) |

## Exécuter

Le contrôle qualité est la **troisième étape du pipeline** : l'ordonnanceur Spark l'enchaîne après
chaque chargement, toutes les 5 minutes, sans rien à lancer. Son bilan apparaît dans
`mart.load_runs` (job `quality`), dans le dashboard Metabase **Qualité des données** et dans le
dashboard Grafana **TP3 — Qualité des données**.

Pour le déclencher sans attendre et archiver ses résultats :

```bash
docker compose up -d                 # plateforme démarrée
quality/run.sh                       # résultats dans quality/resultats/
quality/run.sh /tmp/audit-du-jour    # ou dans un autre dossier
```

`run.sh` attend la fin d'un cycle Spark en cours (verrou du pipeline), exécute l'étape, puis
exporte les CSV et les rapports de lecture. Les fichiers sont écrits côté hôte.

Les scripts sont du SQL pur et se rejouent aussi à la main, dans **une seule session** psql (ils
se passent l'identifiant d'exécution par un paramètre de session). Arrêter `spark-jobs` avant,
pour ne pas croiser un chargement :

```bash
docker compose stop spark-jobs
cat quality/sql/0*.sql | docker compose exec -T postgres psql -U tmdb -d tmdb -1 -f -
docker compose start spark-jobs
```

Lecture seule, à tout moment :

```sql
SELECT * FROM dq.v_results_last ORDER BY priority DESC;  -- chaque contrôle, avant et après
SELECT * FROM dq.v_dimension_scores;                     -- score par dimension
SELECT * FROM dq.v_kpi_impact;                           -- effet sur les indicateurs
SELECT * FROM dq.corrections WHERE control_id = 'COH-01'; -- journal d'une correction
```

## Fonctionnement

```
00_dq_schema         socle : tables, fonctions et vues du schéma dq (idempotent)
01_matrice_controles la matrice, chargée dans dq.controls
02_audit             ouvre une exécution ; les 49 contrôles sur mart   -> phase « avant »
03_nettoyage         curated = copie de mart ; corrections via dq.fix / dq.remove, journalisées
04_schema_cible      clés, unicités, CHECK du schéma cible, posés sur curated
05_recontrole        les 49 contrôles sur curated -> phase « après » ; bilan ; rétention
```

`spark/quality.py` exécute ces six fichiers **dans une seule transaction**, sous le verrou
consultatif du pipeline, et trace l'exécution dans `mart.load_runs`.

**Une matrice exécutable.** Chaque contrôle de `dq.controls` porte deux requêtes écrites avec
`%1$I` à la place du schéma. `dq.run_controls(schéma, phase)` les exécute par `format()` et
enregistre le bilan (`dq.results`) et les lignes fautives (`dq.anomalies`). La comparaison
avant/après porte ainsi sur des mesures strictement identiques.

**Des corrections tracées.** `dq.fix(contrôle, action, table, colonne, nouvelle valeur,
condition)` écrit d'abord chaque ligne visée dans `dq.corrections` (clé, ancienne et nouvelle
valeur), puis applique la modification avec la même condition. `dq.remove` fait de même pour
une suppression et conserve la ligne supprimée en JSON. Aucune correction sans trace.

**Des contraintes comme preuve.** Les contraintes du schéma cible sont posées **après** le
nettoyage : PostgreSQL vérifie alors chaque ligne existante. Si une anomalie a échappé au
nettoyage, `04_schema_cible.sql` échoue, la transaction est annulée et le `curated` précédent
reste en place. Une exécution réussie prouve la conformité.

**Des contrôles génériques.** L'hygiène du texte (VAL-16) et sa contrainte parcourent les colonnes
texte lues dans `information_schema` : une colonne ajoutée au modèle est couverte d'office.

**Rétention.** `dq.results` garde 30 jours de bilans (tendances) ; `dq.anomalies` et
`dq.corrections`, volumineux, seulement la dernière exécution. L'audit de référence est archivé
en CSV dans `resultats/`.

## Schémas

| Schéma | Contenu | Cycle de vie |
|---|---|---|
| `dq` | matrice, exécutions, résultats, anomalies, journal, vues de synthèse | persistant, rejoué de façon idempotente |
| `curated` | les 13 tables du modèle, nettoyées, avec les 94 contraintes du schéma cible | recréé à chaque exécution, entièrement dérivé du mart |

Détail des objets : [cartographie.md §7 et §8](cartographie.md#7-schéma-cible-curated).

## Choix techniques

| Besoin | Retenu | Écarté | Raison |
|---|---|---|---|
| Où nettoyer | schéma `curated` à part | corriger le mart en place | le mart est réécrit par UPSERT toutes les 5 minutes : une correction en place serait écrasée, et l'état d'origine serait perdu pour la comparaison |
| Corrections durables | remontées dans `aggregate.py` et `load_mart.py` | nettoyage SQL seul | le dashboard du TP2 lit le mart : c'est lui qui doit être juste. `curated` reste le filet de sécurité et le lieu des corrections qui mêlent deux sources (imputation) |
| Matrice | table `dq.controls` avec ses requêtes | document seul, ou requêtes figées dans un script | une seule définition, exécutée avant et après, lue par les rapports, Metabase et Grafana |
| Exécution | étape du pipeline, sous le verrou commun | service ou tâche à part | le contrôle des liaisons obsolètes lit les tables de transit, qu'un chargement remplit en plusieurs transactions |
| Atomicité | une transaction pour les six scripts | scripts indépendants | tout ou rien : jamais de `curated` à moitié nettoyé ni de résultats sans leur nettoyage |
| Valeur inconnue | NULL | moyenne, médiane, valeur par défaut | ne jamais présenter une estimation comme une mesure ; seule imputation : la durée IMDb, valeur mesurée par une autre source |
| Métriques | requêtes postgres-exporter sur les vues `dq` | Pushgateway | même mécanisme que l'indicateur Raw vs Clean du TP2 |

## Visualiser

| Outil | Où | Contenu |
|---|---|---|
| Metabase | http://localhost:3001, dashboard **Qualité des données** | anomalies avant/après, contrôles à traiter, décisions de la matrice, score par dimension, contrôles par priorité, effet sur les notes par genre et les indicateurs, tendance, corrections |
| Grafana | http://localhost:3000, dossier TP2, **TP3 — Qualité des données** | scores avant/après, anomalies du mart par dimension dans le temps, résidus, détail de la dernière exécution |
| Prometheus | `pipeline_dq_control_anomalies`, `pipeline_dq_control_anomaly_rate`, `pipeline_dq_control_passed`, `pipeline_dq_dimension_score` | par contrôle, dimension, sévérité et phase |

Les vues `dq.*` n'existent qu'après la première exécution de l'étape, qui suit le premier
chargement : sur une plateforme neuve, les cartes et les panneaux qualité restent vides quelques
minutes.

## Résultats exportés

| Fichier | Contenu |
|---|---|
| `execution.csv` | l'exécution : volumes, anomalies avant/après, corrections |
| `matrice_controles.csv` | la matrice telle qu'exécutée |
| `avant_apres.csv` | chaque contrôle : population, anomalies et taux avant/après, priorité, verdict |
| `scores_dimensions.csv` | score et contrôles sous le seuil, par dimension et par phase |
| `corrections.csv` | corrections par contrôle, table et colonne |
| `volumes.csv` | lignes par table, mart et curated |
| `impact_indicateurs.csv`, `impact_genres.csv` | indicateurs du dashboard recalculés sur mart et curated |
| `exemples_anomalies.csv` | dix lignes fautives par contrôle |
| `cartographie.txt`, `profilage.txt`, `avant_apres.txt` | sorties des rapports de `sql/rapports/` |

## Limites

- Le contrôle des liaisons obsolètes compare au **dernier chargement** : juste après un
  redémarrage (tables de transit vides), il est « non évaluable » jusqu'au cycle suivant.
- Relancer l'étape à la main pendant qu'un cycle attend son tour fait sauter le chargement de ce
  cycle (statut `skipped`) ; le suivant rattrape.
- Les contrôles cherchent des anomalies **présentes** : une ligne perdue par erreur au nettoyage
  ne serait vue que dans le journal et les volumes (cas relevé et corrigé, voir
  [rapport-audit.md §6](rapport-audit.md#6-incident-relevé-pendant-le-nettoyage)).
- Pas d'alerting : la règle à créer en premier est « contrôles à traiter > 0 ».
