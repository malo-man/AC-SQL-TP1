# Matrice de contrôles qualité

La matrice est **exécutable** : elle vit dans [`sql/01_matrice_controles.sql`](sql/01_matrice_controles.sql),
qui la charge dans la table `dq.controls`. Chaque ligne y porte, en plus de ce tableau, deux
requêtes paramétrées par le schéma contrôlé : la **population** (combien de lignes sont
contrôlées) et les **anomalies** (lesquelles enfreignent la règle). Le même contrôle s'exécute
ainsi à l'identique sur le mart (avant) et sur le schéma nettoyé `curated` (après).

Ce document en est la version lisible. Il a été produit depuis `dq.controls` : en cas d'écart,
le fichier SQL fait foi.

## Lire la matrice

**49 contrôles**, répartis sur les cinq dimensions demandées :

| Dimension | Question posée | Contrôles |
|---|---|---:|
| Complétude | La valeur est-elle présente ? | 11 |
| Unicité | Une même réalité est-elle représentée une seule fois ? | 6 |
| Validité | La valeur respecte-t-elle son format, sa plage, son domaine ? | 16 |
| Cohérence | Les valeurs s'accordent-elles entre elles et entre les deux sources ? | 10 |
| Intégrité | Les relations du modèle sont-elles respectées (clés, cardinalités) ? | 6 |

**Sévérité** — l'impact d'une anomalie, pas sa fréquence :

| Sévérité | Poids | Définition |
|---|---:|---|
| critique | 4 | casse une clé, une jointure ou un calcul |
| majeure | 3 | fausse un indicateur du dashboard ou le rapprochement TMDB × IMDb |
| mineure | 2 | gêne une analyse secondaire ou la lisibilité |
| info | 1 | caractéristique connue de la source, mesurée pour suivi |

**Importance** d'une anomalie constatée = poids de la sévérité × taux d'anomalies (en %). C'est
la colonne `priority` de `dq.v_results_last`, qui ordonne le rapport d'audit.

**Seuil** — taux d'anomalies toléré. 0 % : aucune anomalie admise. « toléré » : contrôle de
suivi, jamais bloquant. Un contrôle est « sous le seuil » quand son taux ne le dépasse pas.

**Traitements** — la décision prise quand l'anomalie est constatée :

| Traitement | Sens | Contrôles |
|---|---|---:|
| suppression | la ligne est retirée (doublon, liaison obsolète, orphelin) | 7 |
| imputation | la valeur manquante est reprise d'une autre source fiable | 1 |
| substitution | la valeur impossible ou sentinelle est remplacée par NULL | 17 |
| correction | la valeur est recalculée ou normalisée | 5 |
| conservation | l'anomalie est gardée et signalée : rien de fiable pour la corriger | 19 |

Deux principes guident ces décisions :

- **Ne jamais inventer.** Une valeur fausse dont la vraie valeur est inconnue devient NULL
  (substitution), pas une estimation. La seule imputation retenue reprend une valeur
  **mesurée** par la seconde source (durée IMDb), pas une valeur calculée.
- **Ne jamais perdre un film.** Aucune règle ne supprime une ligne de `movies` : les
  suppressions ne portent que sur des doublons, des liaisons ou des référentiels orphelins.

## Les contrôles

### Complétude (COM, 11 contrôles)

| ID | Cible | Règle | Sévérité | Seuil | Traitement | Justification |
|---|---|---|---|---|---|---|
| COM-01 | `movies` · `imdb_id` | Identifiant IMDb renseigné | majeure | 5 % | conservation | Clé du rapprochement avec IMDb. TMDB est la seule source qui la fournit : aucune valeur fiable à imputer. Le film reste exploitable côté TMDB, hors des comparaisons de notes. |
| COM-02 | `movies` · `release_date` | Date de sortie renseignée | majeure | 5 % | conservation | Film annoncé sans date. L'année IMDb ne donne pas une date au jour près : l'imputer inventerait une information. Le film est exclu des analyses par année. |
| COM-03 | `movies` · `runtime` | Durée renseignée | mineure | 5 % | imputation | La durée IMDb (title.basics) mesure la même grandeur : imputation inter-sources quand elle existe et reste plausible. Sans elle, la durée reste inconnue. |
| COM-04 | `movies` · `overview` | Synopsis renseigné | mineure | 10 % | conservation | Synopsis absent dans la langue demandée à TMDB (français). Texte libre : aucune substitution possible sans nouvel appel à l'API. |
| COM-05 | `movies` · `budget, revenue` | Budget et recettes connus | info | toléré | conservation | Montants confidentiels pour une grande partie des films. Imputer une moyenne ou une médiane fausserait les totaux financiers : NULL est la représentation correcte de l'inconnu. |
| COM-06 | `movie_genres` | Tout film a au moins un genre | mineure | 2 % | conservation | Liste de genres vide chez TMDB (films récents ou confidentiels). Déduire un genre serait arbitraire. |
| COM-07 | `movie_cast` | Tout film a un casting | mineure | 5 % | conservation | Générique pas encore publié pour les films annoncés. Donnée absente à la source, rien à reconstituer. |
| COM-08 | `movie_crew` · `job` | Tout film a un réalisateur | mineure | 5 % | conservation | Le dashboard affiche le réalisateur de chaque film. Absent de l'équipe technique publiée par TMDB : rien à reconstituer. |
| COM-09 | `people` · `gender` | Genre de la personne connu | info | toléré | conservation | Non renseigné par la communauté TMDB pour une grande partie des techniciens. Donnée personnelle : aucune déduction (par le prénom par exemple) n'est acceptable. |
| COM-10 | `production_companies` · `origin_country` | Pays d'origine de la société connu | info | toléré | conservation | Non renseigné chez TMDB pour une partie des sociétés. Aucune source de substitution. |
| COM-11 | `movies` · `imdb_average_rating` | Film doté d'un identifiant IMDb rapproché d'une note IMDb | majeure | 5 % | conservation | title.ratings ne contient que les titres ayant reçu des votes : les films pas encore sortis n'y figurent pas. Aucune note à imputer ; le rapprochement se fera de lui-même quand IMDb la publiera. |

### Unicité (UNI, 6 contrôles)

| ID | Cible | Règle | Sévérité | Seuil | Traitement | Justification |
|---|---|---|---|---|---|---|
| UNI-01 | `movies` · `id` | Identifiant TMDB unique | critique | 0 % | suppression | Garanti par la clé primaire : le contrôle prouve que la contrainte tient. Un doublon serait supprimé en gardant la collecte la plus récente. |
| UNI-02 | `movies` · `imdb_id` | Un identifiant IMDb ne désigne qu'un film TMDB | critique | 0 % | correction | Sinon la même note IMDb serait comptée deux fois. Le rapprochement est conservé pour le film le plus voté, retiré aux autres. |
| UNI-03 | `movies` · `original_title, release_date` | Pas deux fiches pour un même film (titre original et année identiques) | majeure | 0 % | conservation | Doublon fonctionnel possible, mais les remakes et homonymes existent : sans identifiant IMDb commun, la fusion relève d'une revue manuelle. |
| UNI-04 | `movie_cast` · `movie_id, person_id, character` | Un rôle n'est crédité qu'une fois | mineure | 0 % | suppression | Même personne, même film, même personnage sous deux credit_id : le rôle serait compté deux fois. On garde le crédit le mieux placé au générique. |
| UNI-05 | `movie_crew` · `movie_id, person_id, job` | Un poste n'est crédité qu'une fois | mineure | 0 % | suppression | Même personne, même film, même poste sous deux credit_id. On garde un seul crédit. |
| UNI-06 | `genres, collections, production_companies` · `name` | Pas deux entrées de référentiel de même nom | info | toléré | conservation | Des sociétés homonymes distinctes existent (filiales, pays différents) : TMDB reste la référence des identifiants, les homonymes sont seulement suivis. |

### Validité (VAL, 16 contrôles)

| ID | Cible | Règle | Sévérité | Seuil | Traitement | Justification |
|---|---|---|---|---|---|---|
| VAL-01 | `movies` · `imdb_id` | Identifiant IMDb au format tt + 7 chiffres ou plus | majeure | 0 % | substitution | Un identifiant mal formé ne peut rien rapprocher et masquerait l'absence réelle : remplacé par NULL. |
| VAL-02 | `movies` · `vote_average` | Note TMDB comprise entre 0 et 10 | critique | 0 % | substitution | Hors de l'échelle de notation, la valeur est fausse par construction : remplacée par NULL. |
| VAL-03 | `movies` · `imdb_average_rating` | Note IMDb comprise entre 1 et 10 | critique | 0 % | substitution | L'échelle IMDb va de 1 à 10 : hors de cette plage, la valeur est fausse et remplacée par NULL. |
| VAL-04 | `movies` · `vote_count, imdb_num_votes` | Nombres de votes positifs ou nuls | critique | 0 % | substitution | Un compteur négatif est impossible : remplacé par NULL. |
| VAL-05 | `movies` · `runtime` | Durée plausible (1 à 600 minutes) | majeure | 0 % | substitution | Au-delà de 10 heures il s'agit d'une erreur de saisie ou d'une œuvre hors du périmètre long métrage : la durée est remplacée par NULL plutôt que de fausser les moyennes. |
| VAL-06 | `movies` · `release_date` | Date de sortie plausible (1888 à aujourd'hui + 5 ans) | majeure | 0 % | substitution | Avant le premier film connu (1888) ou trop loin dans le futur, la date est une valeur par défaut ou une erreur : remplacée par NULL. |
| VAL-07 | `movies` · `status` | Statut dans le domaine TMDB | majeure | 0 % | substitution | Six valeurs documentées (Rumored, Planned, In Production, Post Production, Released, Canceled). Toute autre valeur est remplacée par NULL. |
| VAL-08 | `movies, languages, movie_spoken_languages` · `codes langue` | Codes langue au format ISO 639-1 (2 lettres minuscules) | mineure | 0 % | substitution | Un code mal formé ne correspond à aucune langue : remplacé par NULL (ou la ligne de liaison supprimée). |
| VAL-09 | `countries, movie_production_countries, production_companies` · `codes pays` | Codes pays au format ISO 3166-1 alpha-2 (2 lettres majuscules) | majeure | 0 % | substitution | Le dictionnaire du TP1 prévoit NULL pour un pays inconnu ; un code vide ou mal formé fausse le comptage des pays renseignés. Remplacé par NULL. |
| VAL-10 | `movies` · `budget, revenue` | Montants plausibles (au moins 1 000 USD) | majeure | 0 % | substitution | Un budget de quelques dollars est une saisie en millions ou une valeur symbolique. Le multiplier serait une supposition : la valeur est remplacée par NULL (inconnue). |
| VAL-11 | `movies` · `homepage` | Site officiel au format URL http(s) | mineure | 0 % | substitution | Une adresse qui n'est pas une URL n'est pas cliquable dans le dashboard : remplacée par NULL. |
| VAL-12 | `movies, collections, production_companies, people` · `chemins d'image` | Chemins d'image au format TMDB (/<nom>.<jpg|png|svg>) | mineure | 0 % | substitution | Préfixé par l'URL du CDN TMDB, un chemin mal formé donne une image cassée : remplacé par NULL. |
| VAL-13 | `people` · `gender` | Genre dans le domaine 1, 2, 3 (0 est une valeur sentinelle) | mineure | 0 % | substitution | TMDB code « non renseigné » par 0 : une fausse catégorie qui compte dans les répartitions. Remplacé par NULL, comme les 0 « inconnu » du budget et de la durée au TP2. |
| VAL-14 | `movies, people, movie_cast` · `popularity, cast_order` | Popularité et rang au générique positifs ou nuls | mineure | 0 % | substitution | Valeurs négatives impossibles par définition : remplacées par NULL. |
| VAL-15 | `movie_crew, people` · `department, known_for_department` | Département dans la liste TMDB | mineure | 0 % | substitution | Les départements forment une liste fermée chez TMDB (/configuration/jobs). Une valeur hors liste est remplacée par NULL. |
| VAL-16 | `(toutes)` · `(colonnes texte)` | Texte sans chaîne vide ni espace parasite | mineure | 0 % | correction | Une chaîne vide n'est pas une valeur : elle masque l'absence (complétude surestimée) et casse les comparaisons. Espaces retirés, chaînes vides remplacées par NULL, sur toutes les colonnes texte lues dans le catalogue. |

### Cohérence (COH, 10 contrôles)

| ID | Cible | Règle | Sévérité | Seuil | Traitement | Justification |
|---|---|---|---|---|---|---|
| COH-01 | `movies` · `vote_average, vote_count` | Pas de note TMDB sans vote | majeure | 0 % | substitution | TMDB affiche 0 quand personne n'a voté : ce n'est pas une note. Elle tire vers le bas les moyennes par genre et crée un écart TMDB − IMDb artificiel. Remplacée par NULL. |
| COH-02 | `movies` · `has_imdb_match` | Indicateur de rapprochement conforme aux données IMDb présentes | critique | 0 % | correction | Colonne dérivée : vraie si et seulement si une note IMDb est présente, et jamais sans identifiant IMDb. Recalculée. |
| COH-03 | `movies` · `rating_gap` | Écart de notation = note TMDB − note IMDb | majeure | 0 % | correction | Colonne dérivée, cœur du rapprochement métier : elle doit suivre exactement les deux notes stockées. Recalculée. |
| COH-04 | `movies` · `votes_ratio` | Rapport des votes = votes TMDB / votes IMDb | mineure | 0 % | correction | Colonne dérivée : recalculée à partir des deux compteurs stockés. |
| COH-05 | `movies` · `status, release_date` | Un film « Released » a une date de sortie passée | majeure | 0 % | conservation | La date TMDB est la sortie principale, le statut peut refléter une sortie antérieure dans un autre pays. Sans savoir laquelle des deux valeurs est fausse, on signale sans corriger. |
| COH-06 | `movies` · `status, revenue` | Pas de recettes pour un film non sorti | mineure | 0 % | conservation | Recettes d'une sortie partielle (festival, avant-première) ou statut en retard : signalé, les deux valeurs pouvant être justes. |
| COH-07 | `movies` · `runtime, imdb_runtime_minutes` | Durées TMDB et IMDb proches (écart de 30 minutes au plus) | mineure | 5 % | conservation | Deux montages (version cinéma, version longue) justifient un écart. Signalé pour revue ; TMDB reste la référence. |
| COH-08 | `movies` · `release_date, imdb_start_year` | Années de sortie TMDB et IMDb proches (1 an d'écart au plus) | mineure | 5 % | conservation | Festival puis sortie en salle l'année suivante : un écart d'un an est normal. Au-delà, signalé pour revue ; TMDB reste la référence. |
| COH-09 | `liaisons` | Les liaisons d'un film reflètent son dernier état connu | majeure | 0 % | suppression | Le chargement TP2 ajoute les liaisons sans jamais retirer celles qui ont disparu chez TMDB (genre reclassé, crédit supprimé). Comparées au dernier chargement (tables _stg_), les liaisons obsolètes sont supprimées. |
| COH-10 | `movies` · `source_fetched_at, loaded_at, ingest_date` | Chronologie de traçabilité : collecte, puis agrégation, puis chargement | mineure | 0 % | conservation | Une ligne chargée avant d'avoir été collectée signalerait une horloge fausse ou un mélange d'instantanés. Traçabilité : on signale, on ne réécrit pas l'histoire. |

### Intégrité (INT, 6 contrôles)

| ID | Cible | Règle | Sévérité | Seuil | Traitement | Justification |
|---|---|---|---|---|---|---|
| INT-01 | `liaisons` · `clés étrangères` | Toute liaison pointe vers un film et un élément existants | critique | 0 % | suppression | Garanti par les clés étrangères du mart : le contrôle le prouve. Une liaison orpheline serait supprimée. |
| INT-02 | `movies` · `collection_id` | Toute saga référencée existe | critique | 0 % | substitution | Garanti par la clé étrangère du mart. Une référence cassée serait remplacée par NULL (règle ON DELETE SET NULL du modèle). |
| INT-03 | `genres, collections, production_companies, countries, languages` | Tout élément de référentiel est relié à au moins un film | mineure | 0 % | suppression | Cardinalité (1,N) du MCD : ces lignes n'existent que par un film. Orphelines après la disparition de leurs liaisons, elles gonflent les référentiels du dashboard. Supprimées. |
| INT-04 | `people` | Toute personne a au moins un crédit | mineure | 0 % | suppression | Une personne n'entre dans le modèle que par un crédit de casting ou d'équipe. Sans crédit, elle est orpheline : supprimée. |
| INT-05 | `movies` · `original_language` | Langue originale présente dans le référentiel des langues | info | toléré | conservation | Choix du TP1 : pas de clé étrangère, le référentiel n'étant alimenté que par les langues parlées. Suivi de l'écart, sans correction. |
| INT-06 | `production_companies` · `origin_country` | Pays d'origine des sociétés présent dans le référentiel des pays | info | toléré | conservation | Choix du TP1 : pas de clé étrangère, le référentiel n'étant alimenté que par les pays de production. Suivi de l'écart, sans correction. |
## Des contrôles aux contraintes du schéma cible

Chaque règle de validité ou de cohérence qui peut s'écrire comme une contrainte PostgreSQL le
devient dans [`sql/04_schema_cible.sql`](sql/04_schema_cible.sql). Les contraintes portent le
numéro du contrôle qu'elles garantissent (`val01_imdb_id_format`, `coh03_rating_gap`...) et
sont posées **après** le nettoyage : leur création vérifie chaque ligne, un échec annule toute
l'exécution.

| Contrôles | Contrainte du schéma cible |
|---|---|
| UNI-01, INT-01, INT-02 | clés primaires et étrangères du MLD du TP1 |
| UNI-02 | `UNIQUE (imdb_id)` |
| UNI-04, UNI-05 | `UNIQUE NULLS NOT DISTINCT` sur (film, personne, personnage / poste) |
| VAL-01 à VAL-15 | `CHECK` de format (expressions régulières), de plage et de domaine |
| VAL-16 | un `CHECK` « texte normalisé » par colonne texte, généré depuis le catalogue |
| COH-01 à COH-04 | `CHECK` entre colonnes : pas de note sans vote, colonnes dérivées exactes |

Restent vérifiés par le seul recontrôle, faute de pouvoir s'écrire en contrainte : la
cardinalité (1,N) des référentiels (INT-03, INT-04), les liaisons obsolètes (COH-09), qui
dépendent du dernier chargement, et la borne haute des dates (VAL-06), relative au jour courant.

## Ajouter un contrôle

1. Ajouter une ligne dans `sql/01_matrice_controles.sql`, avec ses deux requêtes écrites sur
   `%1$I` (le schéma contrôlé) et renvoyant `(record_key, observed_value)`.
2. Si le traitement n'est pas « conservation », ajouter la correction dans
   `sql/03_nettoyage.sql` via `dq.fix()` ou `dq.remove()`, et, si la règle s'y prête, la
   contrainte correspondante dans `sql/04_schema_cible.sql`.
3. `quality/run.sh` : le contrôle apparaît dans les résultats, les dashboards et les métriques
   sans autre modification.
