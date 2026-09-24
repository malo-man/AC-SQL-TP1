# TP — Audit & cartographie des données
## Objectif

À partir d’un sujet de votre choix, recherchez un ou plusieurs jeux de données réels et réalisez leur cartographie.

Vous devez être capables de passer de données réelles à une modélisation structurée, puis à une base PostgreSQL.

---
## 1. Choisir un sujet

Choisissez un sujet métier ou sociétal :

- Transport
- Immobilier
- Éducation
- E-commerce
- Banque
- Tourisme
- Sport
- Environnement
- Énergie
- Emploi
- Agriculture
- Autre

Recherchez un ou plusieurs jeux de données correspondant à votre sujet.

> Sujet choisi : le **cinéma**

---
## 2. Identifier les sources

Pour chaque source, documentez :

- Nom de la source
- Organisation / origine
- URL
- Format : CSV, Excel, JSON, SQL, etc.
- Nature : structurée ou semi-structurée
- Description

> Une seule source choisie : [TMDB (The Movie DataBase)](https://www.themoviedb.org/)\
> Elle offre une API permettant de récupérer des fichiers JSON sur les films, acteurs, catégories...

> Nom de la source : The Movie DataBase\
> Organisation / origine : Communautée TMDB\
> URL : [https://www.themoviedb.org/](https://www.themoviedb.org/)\
> Format : JSON\
> Nature : semi-structurée\
> Description : Base de données de films, séries, acteurs, et autres informations en lien avec tout ça

---
## 3. Réaliser le dictionnaire de données

Analysez les principales colonnes de vos données.
Champ 	Description 	Type 	Exemple
... 	... 	... 	...

Le dictionnaire doit permettre de comprendre clairement la signification des données.

> Les données proviennent de deux endpoints de l'API TMDB :
>
> - `GET /movie/{id}?append_to_response=credits` : fiche complète d'un film avec son casting et son équipe technique ;
> - `GET /movie/popular`, `/movie/top_rated`, `/search/movie` : listes paginées servant à trouver les films à importer.
>
> Les identifiants (`id`) sont ceux de TMDB : ils sont réutilisés comme clés primaires pour pouvoir réimporter un film sans créer de doublon.
> Les chemins d'images (`*_path`) sont relatifs : l'URL complète s'obtient en les préfixant par `https://image.tmdb.org/t/p/<taille>` (ex. `w500`).
> Les exemples sont tirés de la base après import (film _Spider-Man : Brand New Day_).

### Films — `movies`

Source : racine du JSON `/movie/{id}`.

| Champ               | Description                                                                                            | Type            | Contraintes                   | Exemple                                                     |
| ------------------- | ------------------------------------------------------------------------------------------------------ | --------------- | ----------------------------- | ----------------------------------------------------------- |
| `id`                | Identifiant TMDB du film                                                                               | `INTEGER`       | PK                            | `969681`                                                    |
| `imdb_id`           | Identifiant du film sur IMDb                                                                           | `TEXT`          |                               | `tt22084616`                                                |
| `title`             | Titre dans la langue demandée (français)                                                               | `TEXT`          | NOT NULL                      | `Spider-Man : Brand New Day`                                |
| `original_title`    | Titre dans la langue d'origine                                                                         | `TEXT`          |                               | `Spider-Man: Brand New Day`                                 |
| `original_language` | Langue d'origine (code ISO 639-1)                                                                      | `CHAR(2)`       |                               | `en`                                                        |
| `overview`          | Synopsis                                                                                               | `TEXT`          |                               | `Quatre ans se sont écoulés, Peter, désormais adulte…`      |
| `tagline`           | Phrase d'accroche de l'affiche                                                                         | `TEXT`          |                               | `Le monde a peut-être oublié Peter Parker…`                 |
| `status`            | Étape de production : `Rumored`, `Planned`, `In Production`, `Post Production`, `Released`, `Canceled` | `TEXT`          |                               | `Released`                                                  |
| `release_date`      | Date de sortie principale                                                                              | `DATE`          | Indexé                        | `2026-07-29`                                                |
| `runtime`           | Durée en minutes                                                                                       | `INTEGER`       |                               | `150`                                                       |
| `budget`            | Budget en dollars US (`0` = inconnu)                                                                   | `BIGINT`        |                               | `225000000`                                                 |
| `revenue`           | Recettes mondiales en dollars US (`0` = inconnu)                                                       | `BIGINT`        |                               | `2478502286`                                                |
| `popularity`        | Score de popularité calculé par TMDB (vues, votes, favoris…)                                           | `NUMERIC(10,3)` |                               | `686.081`                                                   |
| `vote_average`      | Note moyenne des utilisateurs TMDB, sur 10                                                             | `NUMERIC(4,2)`  |                               | `7.86`                                                      |
| `vote_count`        | Nombre de votes                                                                                        | `INTEGER`       |                               | `2854`                                                      |
| `adult`             | Film réservé aux adultes                                                                               | `BOOLEAN`       | NOT NULL, défaut `FALSE`      | `false`                                                     |
| `homepage`          | Site officiel du film                                                                                  | `TEXT`          |                               | `https://www.sonypictures.fr/film/spider-man-brand-new-day` |
| `poster_path`       | Chemin de l'affiche                                                                                    | `TEXT`          |                               | `/sB7wauO5DAoSZlY5Z5cCz3URtn0.jpg`                          |
| `backdrop_path`     | Chemin de l'image de fond                                                                              | `TEXT`          |                               | `/qeQJx07rK2xm8SD2sJxFKhE7gs0.jpg`                          |
| `collection_id`     | Saga à laquelle appartient le film (`belongs_to_collection.id`)                                        | `INTEGER`       | FK → `collections.id`, indexé | `531241`                                                    |
| `fetched_at`        | Date et heure du dernier import (ajouté par l'application, absent de TMDB)                             | `TIMESTAMPTZ`   | NOT NULL, défaut `now()`      | `2026-09-24 09:49:42+00`                                    |

### Genres — `genres`

Source : `genres[]`.

| Champ  | Description               | Type      | Contraintes | Exemple     |
| ------ | ------------------------- | --------- | ----------- | ----------- |
| `id`   | Identifiant TMDB du genre | `INTEGER` | PK          | `16`        |
| `name` | Nom du genre              | `TEXT`    | NOT NULL    | `Animation` |

### Sagas — `collections`

Source : `belongs_to_collection` (objet ou `null`).

| Champ           | Description                          | Type      | Contraintes | Exemple                            |
| --------------- | ------------------------------------ | --------- | ----------- | ---------------------------------- |
| `id`            | Identifiant TMDB de la saga          | `INTEGER` | PK          | `531241`                           |
| `name`          | Nom de la saga                       | `TEXT`    | NOT NULL    | `Spider-Man (MCU) - Saga`          |
| `poster_path`   | Chemin de l'affiche de la saga       | `TEXT`    |             | `/3BVng0lmJyYIUqm5dxLS2eZ2625.jpg` |
| `backdrop_path` | Chemin de l'image de fond de la saga | `TEXT`    |             | `/AvnqpRwlEaYNVL6wzC4RN94EdSd.jpg` |

### Sociétés de production — `production_companies`

Source : `production_companies[]`.

| Champ            | Description                                                                   | Type      | Contraintes | Exemple                            |
| ---------------- | ----------------------------------------------------------------------------- | --------- | ----------- | ---------------------------------- |
| `id`             | Identifiant TMDB de la société                                                | `INTEGER` | PK          | `420`                              |
| `name`           | Nom de la société                                                             | `TEXT`    | NOT NULL    | `Marvel Studios`                   |
| `origin_country` | Pays d'origine (ISO 3166-1). TMDB renvoie `""` si inconnu, converti en `NULL` | `CHAR(2)` |             | `US`                               |
| `logo_path`      | Chemin du logo                                                                | `TEXT`    |             | `/hUzeosd33nzE5MCNsZxCGEKTXaQ.png` |

### Pays — `countries`

Source : `production_countries[]`.

| Champ        | Description                  | Type      | Contraintes | Exemple   |
| ------------ | ---------------------------- | --------- | ----------- | --------- |
| `iso_3166_1` | Code pays ISO 3166-1 alpha-2 | `CHAR(2)` | PK          | `DE`      |
| `name`       | Nom du pays (en anglais)     | `TEXT`    | NOT NULL    | `Germany` |

### Langues — `languages`

Source : `spoken_languages[]`.

| Champ          | Description                        | Type      | Contraintes | Exemple         |
| -------------- | ---------------------------------- | --------- | ----------- | --------------- |
| `iso_639_1`    | Code langue ISO 639-1              | `CHAR(2)` | PK          | `ko`            |
| `name`         | Nom de la langue dans cette langue | `TEXT`    |             | `한국어/조선말` |
| `english_name` | Nom de la langue en anglais        | `TEXT`    |             | `Korean`        |

### Personnes — `people`

Source : `credits.cast[]` et `credits.crew[]`. Une même personne peut être à la fois actrice et membre de l'équipe technique, d'où une table unique.

| Champ                  | Description                                                      | Type            | Contraintes | Exemple                            |
| ---------------------- | ---------------------------------------------------------------- | --------------- | ----------- | ---------------------------------- |
| `id`                   | Identifiant TMDB de la personne                                  | `INTEGER`       | PK          | `103`                              |
| `name`                 | Nom d'usage                                                      | `TEXT`          | NOT NULL    | `Mark Ruffalo`                     |
| `original_name`        | Nom dans l'écriture d'origine                                    | `TEXT`          |             | `Mark Ruffalo`                     |
| `gender`               | Genre : `0` non renseigné, `1` femme, `2` homme, `3` non-binaire | `SMALLINT`      |             | `2`                                |
| `known_for_department` | Métier principal                                                 | `TEXT`          |             | `Acting`                           |
| `popularity`           | Score de popularité TMDB                                         | `NUMERIC(10,3)` |             | `7.795`                            |
| `profile_path`         | Chemin de la photo                                               | `TEXT`          |             | `/5GilHMOt5PAQh6rlUKZzGmaKEI7.jpg` |

### Casting — `movie_cast`

Source : `credits.cast[]`. Relie un acteur à un film, avec le rôle joué.

| Champ        | Description                              | Type      | Contraintes                        | Exemple                     |
| ------------ | ---------------------------------------- | --------- | ---------------------------------- | --------------------------- |
| `credit_id`  | Identifiant TMDB unique du crédit        | `TEXT`    | PK                                 | `630cbd43ede1b00083c3badf`  |
| `movie_id`   | Film concerné                            | `INTEGER` | NOT NULL, FK → `movies.id`, indexé | `969681`                    |
| `person_id`  | Acteur ou actrice                        | `INTEGER` | NOT NULL, FK → `people.id`, indexé | `1136406`                   |
| `character`  | Personnage joué                          | `TEXT`    |                                    | `Peter Parker / Spider-Man` |
| `cast_order` | Rang au générique (`0` = rôle principal) | `INTEGER` |                                    | `0`                         |

### Équipe technique — `movie_crew`

Source : `credits.crew[]`. Relie un membre de l'équipe à un film, avec son poste.

| Champ        | Description                                                                                                                                        | Type      | Contraintes                        | Exemple                    |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---------------------------------- | -------------------------- |
| `credit_id`  | Identifiant TMDB unique du crédit                                                                                                                  | `TEXT`    | PK                                 | `628d559ed48cee2cbfca858a` |
| `movie_id`   | Film concerné                                                                                                                                      | `INTEGER` | NOT NULL, FK → `movies.id`, indexé | `969681`                   |
| `person_id`  | Membre de l'équipe                                                                                                                                 | `INTEGER` | NOT NULL, FK → `people.id`, indexé | `10850`                    |
| `department` | Département : `Directing`, `Writing`, `Production`, `Camera`, `Editing`, `Sound`, `Art`, `Costume & Make-Up`, `Visual Effects`, `Lighting`, `Crew` | `TEXT`    |                                    | `Production`               |
| `job`        | Poste précis dans le département                                                                                                                   | `TEXT`    |                                    | `Producer`                 |

### Tables de liaison (relations N-N)

Ces tables n'ont pas d'attribut propre. Leur clé primaire est le couple des deux clés étrangères, ce qui empêche les doublons. Supprimer un film supprime ses liaisons (`ON DELETE CASCADE`).

| Table                        | Champ         | Description                   | Type      | Contraintes                        | Exemple  |
| ---------------------------- | ------------- | ----------------------------- | --------- | ---------------------------------- | -------- |
| `movie_genres`               | `movie_id`    | Film                          | `INTEGER` | PK, FK → `movies.id`               | `969681` |
|                              | `genre_id`    | Genre du film                 | `INTEGER` | PK, FK → `genres.id`               | `878`    |
| `movie_production_companies` | `movie_id`    | Film                          | `INTEGER` | PK, FK → `movies.id`               | `969681` |
|                              | `company_id`  | Société ayant produit le film | `INTEGER` | PK, FK → `production_companies.id` | `420`    |
| `movie_production_countries` | `movie_id`    | Film                          | `INTEGER` | PK, FK → `movies.id`               | `969681` |
|                              | `country_id`  | Pays de production            | `CHAR(2)` | PK, FK → `countries.iso_3166_1`    | `US`     |
| `movie_spoken_languages`     | `movie_id`    | Film                          | `INTEGER` | PK, FK → `movies.id`               | `969681` |
|                              | `language_id` | Langue parlée dans le film    | `CHAR(2)` | PK, FK → `languages.iso_639_1`     | `en`     |
---
## 4. Identifier les entités, attributs et relations

À partir des données, identifiez :

- les entités ;
- leurs attributs ;
- les relations ;
- les cardinalités.

Vous devez être capables de justifier vos choix.

Exemple :

CLIENT 1 ───── N COMMANDE

---
## 5. Réaliser les modèles

Utilisez un outil de modélisation, par exemple dbdiagram.io.
Modèle conceptuel

Représentez :

- Entités
- Attributs principaux
- Relations
- Cardinalités

Modèle logique

Transformez le modèle conceptuel en :

- Tables
- Colonnes
- Clés primaires
- Clés étrangères
- Relations

---
## 6. Implémenter le modèle dans PostgreSQL

Créez une base de données PostgreSQL correspondant à votre modèle logique.

Vous devez :

- créer les tables ;
- définir les types de données ;
- définir les clés primaires et étrangères ;
- ajouter les contraintes pertinentes ;
- insérer quelques données de test ;
- vérifier que les relations fonctionnent.

Vous pouvez utiliser pgAdmin pour administrer la base.

---
## Livrables

Vous devez remettre :

- Présentation du sujet
  Contexte, problématique et objectif.
- Sources de données
  Origine, URL, formats et nature des données.
- Dictionnaire de données
- Modèle conceptuel
  Diagramme réalisé avec dbdiagram.io ou un outil équivalent.
- Modèle logique
  Tables, attributs, PK, FK et relations.
- Base PostgreSQL
  Script de création de la base et des tables + quelques données de test.
- Cartographie globale
  Vue d’ensemble du cheminement
