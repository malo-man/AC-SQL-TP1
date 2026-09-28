# Rapport d'audit qualité — données TMDB × IMDb

Audit des données produites par le pipeline du TP2 (schéma `mart`), réalisé le 28/09/2026 avec
la [matrice de 49 contrôles](matrice-controles.md). Toutes les valeurs de ce rapport viennent des
fichiers de [`resultats/`](resultats/) et se reproduisent avec `quality/run.sh`.

## En bref

| | Mart du TP2 | Données nettoyées (`curated`) | Mart après correction à la source |
|---|---:|---:|---:|
| Films | 619 | 619 | 619 |
| Anomalies relevées | **56 951** | **28 250** | **28 258** |
| Contrôles sans aucune anomalie | 23 / 49 | 33 / 49 | 33 / 49 |
| Contrôles sous leur seuil | 36 / 49 | 46 / 49 | 46 / 49 |
| Score global (moyenne des taux de conformité) | 95,83 % | 97,15 % | 97,13 % |
| Note TMDB moyenne | 6,57 | 7,46 | 7,46 |
| Contrôles « à traiter » | — | **0** | **0** |

- **Aucune anomalie critique** : clés primaires, clés étrangères et plages de notes tiennent.
- Les anomalies **corrigées** (10 contrôles) faussaient des indicateurs du dashboard : la note
  moyenne TMDB était sous-estimée de 0,89 point, jusqu'à 5,5 points pour les documentaires.
- Les anomalies **restantes** (16 contrôles, 28 250 lignes) sont conservées volontairement :
  données absentes à la source, que rien de fiable ne permet de reconstituer. 97,6 % d'entre
  elles sont le genre non renseigné des personnes.
- Les corrections ont ensuite été **remontées dans le pipeline** : le mart lui-même est désormais
  conforme sur tous les contrôles corrigés.

## 1. Périmètre et méthode

| Élément | Valeur |
|---|---|
| Données auditées | schéma `mart`, instantané du 28/09/2026 (`ingest_date`) |
| Volume | 619 films, 62 270 personnes, 83 401 crédits, 5 478 liaisons, 1 712 entrées de référentiels |
| Sources | 6 209 messages TMDB bruts (listes populaires, mieux notés, à l'affiche, à venir), IMDb `title.ratings` et `title.basics` |
| Exécution de référence | `7528c748-…`, 4 secondes, dans le pipeline (job `quality`) |

Démarche, dans l'ordre du sujet :

1. **Cartographie** : sources, tables, types et relations vérifiés sur la base
   ([cartographie.md](cartographie.md)).
2. **Matrice** : 49 contrôles sur les cinq dimensions, chacun avec sa sévérité, son seuil et le
   traitement retenu ([matrice-controles.md](matrice-controles.md)).
3. **Audit** : les contrôles sont passés sur le mart (`02_audit.sql`), le profilage
   ([resultats/profilage.txt](resultats/profilage.txt)) a servi à calibrer règles et seuils.
4. **Détection et classement** : par dimension, par sévérité, par importance (section 2).
5. **Nettoyage** : les corrections sont appliquées dans le schéma `curated` et journalisées une
   à une dans `dq.corrections` (section 3).
6. **Contrôle final** : les contraintes du schéma cible sont posées, puis les **mêmes** contrôles
   repassés sur `curated` (section 4).
7. **Correction à la source**, puis nouvel audit du mart (section 5).

## 2. Anomalies détectées

### 2.1 Par dimension

| Dimension | Contrôles | En anomalie | Anomalies (mart) | Après nettoyage |
|---|---:|---:|---:|---:|
| Complétude | 11 | 11 | 28 218 | 28 208 |
| Unicité | 6 | 3 | 10 | 8 |
| Validité | 16 | 4 | 28 592 | 0 |
| Cohérence | 10 | 6 | 129 | 33 |
| Intégrité | 6 | 2 | 2 | 1 |
| **Total** | **49** | **26** | **56 951** | **28 250** |

### 2.2 Par sévérité

| Sévérité | Contrôles | En anomalie | Anomalies (mart) | Après nettoyage |
|---|---:|---:|---:|---:|
| critique | 8 | 0 | 0 | 0 |
| majeure | 14 | 9 | 318 | 94 |
| mineure | 21 | 12 | 28 607 | 132 |
| info | 6 | 5 | 28 026 | 28 024 |

### 2.3 Classement par importance

Importance = poids de la sévérité (critique 4, majeure 3, mineure 2, info 1) × taux d'anomalies.
Le volume seul tromperait : 27 589 genres à 0 pèsent moins, pour le métier, que 74 notes
fausses.

| # | Contrôle | Anomalie | Sév. | Lignes | Taux | Importance | Décision |
|---:|---|---|---|---:|---:|---:|---|
| 1 | VAL-13 | genre des personnes à 0 (valeur sentinelle) | mineure | 27 589 | 44,3 % | 88,6 | substitution |
| 2 | COM-05 | budget ou recettes inconnus | info | 301 | 48,6 % | 48,6 | conservation |
| 3 | COM-09 | genre des personnes non renseigné | info | 27 589 | 44,3 % | 44,3 | conservation |
| 4 | **COH-01** | **note TMDB de 0 sans aucun vote** | majeure | 74 | 12,0 % | 35,9 | substitution |
| 5 | COM-04 | synopsis absent | mineure | 106 | 17,1 % | 34,3 | conservation |
| 6 | COM-11 | identifiant IMDb sans note IMDb | majeure | 47 | 7,8 % | 23,4 | conservation |
| 7 | **VAL-09** | **code pays vide (`''`)** | majeure | 127 | 5,3 % | 15,8 | substitution |
| 8 | COH-05 | film « Released » daté dans le futur | majeure | 29 | 5,0 % | 14,9 | conservation |
| 9 | COM-10 | pays d'origine de société inconnu | info | 127 | 8,8 % | 8,8 | conservation |
| 10 | COM-01 | identifiant IMDb absent | majeure | 17 | 2,7 % | 8,2 | conservation |
| 11 | COM-03 | durée absente | mineure | 23 | 3,7 % | 7,4 | imputation |
| 12 | **COH-03** | **écart de notation faux d'un centième** | majeure | 10 | 1,6 % | 4,9 | correction |
| 13 | COM-06 | film sans genre | mineure | 3 | 0,5 % | 1,0 | conservation |
| 14 | VAL-10 | budget et recettes de 7 USD | majeure | 1 | 0,2 % | 0,7 | substitution |
| 15 | COH-07 | durées TMDB et IMDb très différentes | mineure | 2 | 0,4 % | 0,7 | conservation |
| 16 | COH-08 | années TMDB et IMDb très différentes | mineure | 2 | 0,3 % | 0,7 | conservation |
| 17 | COM-07 | film sans casting | mineure | 2 | 0,3 % | 0,6 | conservation |
| 18 | COM-08 | film sans réalisateur | mineure | 2 | 0,3 % | 0,6 | conservation |
| 19 | UNI-06 | sociétés homonymes | info | 8 | 0,5 % | 0,5 | conservation |
| 20 | COM-02 | date de sortie absente | majeure | 1 | 0,2 % | 0,5 | conservation |
| 21 | VAL-16 | chaînes vides ou espaces parasites | mineure | 875 | 0,2 % | 0,4 | correction |
| 22 | INT-03 | saga reliée à aucun film | mineure | 1 | 0,1 % | 0,1 | suppression |
| 23 | INT-06 | pays de société hors référentiel | info | 1 | 0,1 % | 0,1 | conservation |
| 24 | **COH-09** | **liaisons disparues chez TMDB, restées en base** | majeure | 12 | 0,01 % | 0,04 | suppression |
| 25 | UNI-04 | rôle crédité deux fois | mineure | 1 | < 0,01 % | 0,01 | suppression |
| 26 | UNI-05 | poste crédité deux fois | mineure | 1 | < 0,01 % | 0,00 | suppression |

Les 23 autres contrôles ne relèvent aucune anomalie, dont les huit critiques (clés, notes hors
échelle, compteurs négatifs, liaisons orphelines).

### 2.4 Mesure de l'impact métier

Le taux ne dit pas tout : certaines anomalies rares faussent directement ce que le dashboard
affiche. Les indicateurs ont été recalculés sur le mart et sur les données nettoyées
([resultats/impact_indicateurs.csv](resultats/impact_indicateurs.csv)) :

| Indicateur | Mart | Nettoyé | Écart |
|---|---:|---:|---:|
| Films notés TMDB | 619 | 545 | −74 |
| **Note TMDB moyenne** | **6,565** | **7,457** | **+0,892** |
| Note IMDb moyenne | 7,062 | 7,062 | 0 |
| **Écart TMDB − IMDb moyen** | **0,187** | **0,400** | **+0,213** |
| Films avec durée | 596 | 604 | +8 |
| Films avec budget | 341 | 340 | −1 |

**Les « notes 0 » (COH-01)** sont l'anomalie la plus coûteuse. TMDB affiche 0 quand personne n'a
voté ; le mart l'enregistrait comme une note. 74 films (12 %), dont 61 pas encore sortis,
tiraient les moyennes vers le bas. Par genre ([resultats/impact_genres.csv](resultats/impact_genres.csv)) :

| Genre | Films | Note TMDB (mart) | Note TMDB (nettoyé) | Effet |
|---|---:|---:|---:|---:|
| Documentaire | 11 | 2,08 | 7,62 | +5,54 |
| Téléfilm | 3 | 4,97 | 7,45 | +2,48 |
| Musique | 21 | 6,16 | 8,08 | +1,92 |
| Horreur | 74 | 5,39 | 6,54 | +1,15 |
| Comédie | 147 | 6,19 | 7,17 | +0,98 |

Seize de ces films ont une note IMDb : leur écart TMDB − IMDb valait de −5,6 à −8 (−7 en
moyenne) et masquait l'écart réel. Le document du TP2 annonçait que TMDB note 0,3 à 1 point au-dessus d'IMDb : sur
les données nettoyées, l'écart moyen est bien de **0,40**, contre 0,19 mesuré sur le mart.

**Les codes pays vides (VAL-09)** : 127 sociétés comptaient comme ayant un pays d'origine, alors
que le dictionnaire du TP1 prévoyait NULL. Taux de complétude de `origin_country` réel :
91,2 %, affiché : 100 %.

**Les liaisons obsolètes (COH-09)** : peu nombreuses (12), mais sans limite dans le temps. Le
chargement du TP2 ne retire jamais une liaison : chaque modification de fiche chez TMDB (crédit
recréé, genre reclassé) laisse une trace qui s'accumule. Sur *Avengers : Doomsday*, cinq rôles
retirés du générique par TMDB apparaissaient encore dans le mart.

## 3. Corrections et justifications

Corrections appliquées au schéma `curated` à l'exécution de référence
([resultats/corrections.csv](resultats/corrections.csv)) :

| Traitement | Contrôles | Lignes | Détail |
|---|---:|---:|---|
| substitution | 3 | 27 665 | genre 0 → NULL (27 589), note sans vote → NULL (74), montants de 7 USD → NULL (2) |
| correction | 2 | 901 | textes normalisés (875 : 736 personnages, 127 pays, 11 noms de langue, 1 nom), écarts de notation recalculés (26) |
| suppression | 4 | 17 | liaisons obsolètes (12), personnes (3) et saga (1) devenues orphelines, poste en double (1) |
| imputation | 1 | 8 | durée TMDB inconnue reprise d'IMDb |
| **Total** | **10** | **28 591** | chaque ligne est tracée dans `dq.corrections` (clé, colonne, ancienne et nouvelle valeur) |

Justification de chaque décision, dans l'ordre d'importance :

| Contrôle | Décision | Pourquoi |
|---|---|---|
| VAL-13 genre 0 | **substitution** par NULL | 0 est la valeur TMDB de « non renseigné » : laissé tel quel, il forme une fausse catégorie dans toute répartition. Même règle qu'au TP2 pour les 0 du budget et de la durée. |
| COH-01 note sans vote | **substitution** par NULL | 0 n'est pas une note mais l'absence de note. L'imputer (moyenne du genre...) inventerait un avis que personne n'a donné. |
| VAL-09 / VAL-16 textes vides | **correction** (espaces retirés, `''` → NULL) | Une chaîne vide n'est pas une valeur ; le dictionnaire du TP1 prévoyait déjà NULL. |
| COH-03 écart de notation | **correction** par recalcul | Colonne dérivée : elle doit suivre exactement les notes stockées. 26 lignes recalculées : les 10 écarts d'arrondi, plus les 16 films dont la note est devenue NULL (COH-01). |
| VAL-10 montants de 7 USD | **substitution** par NULL | Probablement 7 millions, mais multiplier serait un pari : « inconnu » est la seule valeur certaine. |
| COH-09 liaisons obsolètes | **suppression** | Comparées au dernier chargement complet du film : elles n'existent plus à la source. |
| INT-03 / INT-04 orphelins | **suppression** | Cardinalité (1,N) du MCD : une saga ou une personne n'existe que par un film. Apparus après le retrait des liaisons obsolètes. |
| UNI-04 / UNI-05 crédits en double | **suppression** du second | Le même rôle ou poste compté deux fois. Le doublon de rôle était un faux doublon, résolu par COH-09 (voir section 6). |
| COM-03 durée absente | **imputation** depuis IMDb | Seule imputation retenue : la durée IMDb mesure exactement la même grandeur, c'est une valeur observée, pas estimée. 8 films sur 23 ; les 15 autres n'ont pas de durée IMDb non plus. |

Anomalies **conservées** et pourquoi :

| Contrôle | Lignes | Pourquoi rien n'est corrigé |
|---|---:|---|
| COM-09 genre non renseigné | 27 586 | donnée personnelle : aucune déduction (par le prénom) n'est acceptable |
| COM-05 budget ou recettes inconnus | 302 | montants confidentiels ; une moyenne fausserait les totaux |
| COM-10 pays de société inconnu | 127 | aucune source de substitution |
| COM-04 synopsis absent | 106 | absent en français chez TMDB ; texte libre non imputable |
| COM-11 non rapproché d'IMDb | 47 | films non sortis, absents de `title.ratings` ; se résorbera seul |
| COH-05 « Released » daté dans le futur | 29 | sortie déjà faite dans un pays, date principale ailleurs : impossible de savoir quelle valeur est fausse |
| COM-01 sans identifiant IMDb | 17 | TMDB est la seule source de l'identifiant |
| COM-03 durée absente (reste) | 15 | ni TMDB ni IMDb ne la connaissent |
| UNI-06 sociétés homonymes | 8 | filiales distinctes chez TMDB (*Sky*, *Orion Pictures*...) : fusionner serait une erreur |
| autres (COM-02, 06, 07, 08, COH-07, 08, INT-06) | 13 | données absentes ou écarts légitimes entre sources (versions longues, festival puis sortie) |

## 4. Résultats avant / après

Les mêmes 49 contrôles, exécutés sur le mart puis sur `curated`
([resultats/avant_apres.csv](resultats/avant_apres.csv)) :

| Dimension | Score avant | Score après | Sous le seuil avant | Sous le seuil après |
|---|---:|---:|---:|---:|
| Cohérence | 98,08 % | 99,44 % | 6/10 | 9/10 |
| Complétude | 87,78 % | 87,88 % | 9/11 | 9/11 |
| Intégrité | 99,98 % | 99,99 % | 5/6 | 6/6 |
| Unicité | 99,92 % | 99,92 % | 4/6 | 6/6 |
| Validité | 96,87 % | **100 %** | 12/16 | **16/16** |
| **Global** | **95,83 %** | **97,15 %** | **36/49** | **46/49** |

Verdicts : **10 contrôles corrigés** (anomalies ramenées à 0), **16 résiduels acceptés**
(conservation justifiée), 23 conformes dès l'origine, **0 à traiter**.

**Preuve de conformité au schéma cible** : les 94 contraintes du schéma cible (13 clés primaires,
13 clés étrangères, 3 unicités, 65 CHECK) sont posées après le nettoyage et vérifient chaque
ligne ; l'exécution échouerait entière à la première ligne non conforme.

**Volumes** : aucun film supprimé ; −12 liaisons, −1 poste en double, −3 personnes, −1 saga
([resultats/volumes.csv](resultats/volumes.csv)).

La complétude bouge peu, et c'est attendu : on ne complète pas ce que personne ne connaît. Trois
contrôles conservés dépassent leur seuil (synopsis 17 % pour 10 % toléré, rapprochement IMDb
7,8 % pour 5 %, dates futures 5 % pour 0 %) : c'est l'effet des films à venir ajoutés à la
collecte, une limite de la source et non du nettoyage. Les seuils n'ont pas été relevés pour
autant : ils restent des objectifs, et leur dépassement reste visible dans les dashboards.

## 5. Correction à la source

Nettoyer en aval ne suffit pas : le mart est réécrit toutes les cinq minutes et le dashboard du
TP2 le lit directement. Chaque anomalie corrigée a donc été **remontée dans le pipeline**
(`spark/aggregate.py`, `spark/load_mart.py`), puis le mart a été audité à nouveau
([resultats/apres-correction-source/](resultats/apres-correction-source/)) :

| Contrôle | Mart du TP2 | Mart corrigé à la source | Où |
|---|---:|---:|---|
| VAL-13 genre 0 | 27 589 | 0 | `load_mart` : `gender` hors {1, 2, 3} → NULL |
| COH-01 note sans vote | 74 | 0 | `aggregate` : note NULL tant que `vote_count = 0` |
| VAL-16 textes vides, dont 127 codes pays (VAL-09) | 875 | 0 | `load_mart` : `blank_to_null` sur codes, crédits, personnes, langues |
| COH-03 écart d'arrondi | 10 | 0 | `aggregate` : note arrondie avant le calcul de l'écart |
| COH-09 liaisons obsolètes | 12 | 0 | `load_mart` : retrait, dans la transaction de chargement, des liaisons perdues par un film rechargé |
| INT-03 / INT-04 orphelins | 1 | 0 | `load_mart` : purge des référentiels et personnes sans film |
| UNI-04 / UNI-05 crédits en double | 2 | 0 | `load_mart` : un seul crédit par rôle ou poste |
| VAL-10 montants symboliques | 1 | 0 | `aggregate` : montant inconnu sous 1 000 USD |
| **Total des anomalies du mart** | **56 951** | **28 258** | |

Le nettoyage SQL n'a plus alors que **8 corrections** à faire, les imputations de durée,
volontairement laissées dans `curated` : elles mêlent deux sources et doivent rester tracées ;
le mart garde la valeur telle que TMDB la fournit. Les indicateurs du dashboard Metabase du TP2,
qui lit le mart, sont désormais justes (note TMDB moyenne 7,46, écart moyen 0,40).

Chaque correction à la source est couverte par un piège ajouté aux fixtures
(`fixtures/README.md`), vérifié en exécutant les jobs Spark sur le mini Data Lake.

## 6. Incident relevé pendant le nettoyage

La relecture du journal `dq.corrections` a révélé un défaut du nettoyage lui-même. Sur
*Avengers : Doomsday*, TMDB a recréé le crédit de Mark Ruffalo (*Bruce Banner / Hulk*) sous un
nouvel identifiant ; le mart contenait l'ancien et le nouveau, d'où un doublon (UNI-04). La
première version du script dédoublonnait **avant** de retirer les liaisons obsolètes : elle
gardait l'ancien crédit, que l'étape suivante supprimait. Le rôle disparaissait des données
nettoyées, sans qu'aucun contrôle ne le signale, puisque les contrôles cherchent des anomalies
présentes et non des lignes perdues.

Correction : les liaisons obsolètes sont retirées d'abord, le doublon disparaît avec elles
(commit `1c97731`). Enseignement : **l'ordre des corrections compte**, et un journal ligne à
ligne est ce qui permet de voir ce qu'un recontrôle ne voit pas.

## 7. Limites et recommandations

- **Contrôle des liaisons obsolètes dépendant du dernier chargement** : juste après un
  redémarrage, les tables de transit sont vides et COH-09 est « non évaluable » jusqu'au cycle
  suivant. Depuis la correction à la source, le cas ne se présente plus dans le mart.
- **Borne haute des dates** (aujourd'hui + 5 ans) vérifiée par contrôle et non par contrainte :
  une contrainte n'est évaluée qu'à l'écriture et deviendrait fausse avec le temps.
- **Score par dimension** : moyenne non pondérée des contrôles ; il résume, il ne remplace pas
  la lecture contrôle par contrôle. La complétude est dominée par des champs de suivi (« info »).
- **Pas d'alerte** : la plateforme n'a toujours pas d'alerting (limite du TP2). Première règle à
  créer : « Contrôles à traiter » > 0, déjà calculé par Grafana et Metabase.
- **COH-05** (statut « Released » et date future) : un nouvel appel à l'endpoint
  `/movie/{id}/release_dates` de TMDB permettrait de trancher pays par pays.
- **Genre des personnes** : 44 % non renseigné ; toute analyse de parité doit l'afficher
  explicitement plutôt que de raisonner sur les seules valeurs connues.
