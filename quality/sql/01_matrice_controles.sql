-- =====================================================================
-- TP3 — Matrice de contrôles qualité.
--
-- Chaque ligne est un contrôle : dimension, cible, règle, sévérité, seuil
-- toléré, traitement retenu et justification, puis deux requêtes :
--
--   population : combien de lignes (ou de valeurs) sont contrôlées ;
--   anomalies  : lesquelles enfreignent la règle, sous la forme
--                (record_key, observed_value).
--
-- %1$I est remplacé par le schéma contrôlé (mart, puis curated) ; %1$L par
-- son nom entre apostrophes. Un % littéral s'écrit %%.
--
-- Sévérité :
--   critique  casse une clé, une jointure ou un calcul ;
--   majeure   fausse un indicateur du dashboard ou le rapprochement TMDB × IMDb ;
--   mineure   gêne une analyse secondaire ou la lisibilité ;
--   info      caractéristique connue de la source, mesurée pour suivi.
--
-- Ce fichier est la référence : les contrôles qui n'y figurent plus sont
-- retirés du catalogue.
-- =====================================================================

WITH matrix (control_id, dimension, table_name, column_name, rule, severity, threshold,
             treatment, justification, population_sql, anomaly_sql) AS (
VALUES

-- ---------------------------------------------------------------------
-- Complétude
-- ---------------------------------------------------------------------

('COM-01', 'complétude', 'movies', 'imdb_id',
 $$Identifiant IMDb renseigné$$, 'majeure', 0.05, 'conservation',
 $$Clé du rapprochement avec IMDb. TMDB est la seule source qui la fournit : aucune valeur fiable à imputer. Le film reste exploitable côté TMDB, hors des comparaisons de notes.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT id::text, title FROM %1$I.movies WHERE imdb_id IS NULL$a$),

('COM-02', 'complétude', 'movies', 'release_date',
 $$Date de sortie renseignée$$, 'majeure', 0.05, 'conservation',
 $$Film annoncé sans date. L'année IMDb ne donne pas une date au jour près : l'imputer inventerait une information. Le film est exclu des analyses par année.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT id::text, title || ' (' || coalesce(status, '?') || ')' FROM %1$I.movies WHERE release_date IS NULL$a$),

('COM-03', 'complétude', 'movies', 'runtime',
 $$Durée renseignée$$, 'mineure', 0.05, 'imputation',
 $$La durée IMDb (title.basics) mesure la même grandeur : imputation inter-sources quand elle existe et reste plausible. Sans elle, la durée reste inconnue.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT id::text, title FROM %1$I.movies WHERE runtime IS NULL$a$),

('COM-04', 'complétude', 'movies', 'overview',
 $$Synopsis renseigné$$, 'mineure', 0.10, 'conservation',
 $$Synopsis absent dans la langue demandée à TMDB (français). Texte libre : aucune substitution possible sans nouvel appel à l'API.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT id::text, title FROM %1$I.movies WHERE overview IS NULL$a$),

('COM-05', 'complétude', 'movies', 'budget, revenue',
 $$Budget et recettes connus$$, 'info', 1, 'conservation',
 $$Montants confidentiels pour une grande partie des films. Imputer une moyenne ou une médiane fausserait les totaux financiers : NULL est la représentation correcte de l'inconnu.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT id::text, 'budget=' || coalesce(budget::text, 'NULL') || ' revenue=' || coalesce(revenue::text, 'NULL')
      FROM %1$I.movies WHERE budget IS NULL OR revenue IS NULL$a$),

('COM-06', 'complétude', 'movie_genres', NULL,
 $$Tout film a au moins un genre$$, 'mineure', 0.02, 'conservation',
 $$Liste de genres vide chez TMDB (films récents ou confidentiels). Déduire un genre serait arbitraire.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT m.id::text, m.title FROM %1$I.movies m
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movie_genres g WHERE g.movie_id = m.id)$a$),

('COM-07', 'complétude', 'movie_cast', NULL,
 $$Tout film a un casting$$, 'mineure', 0.05, 'conservation',
 $$Générique pas encore publié pour les films annoncés. Donnée absente à la source, rien à reconstituer.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT m.id::text, m.title FROM %1$I.movies m
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movie_cast c WHERE c.movie_id = m.id)$a$),

('COM-08', 'complétude', 'movie_crew', 'job',
 $$Tout film a un réalisateur$$, 'mineure', 0.05, 'conservation',
 $$Le dashboard affiche le réalisateur de chaque film. Absent de l'équipe technique publiée par TMDB : rien à reconstituer.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT m.id::text, m.title FROM %1$I.movies m
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movie_crew c WHERE c.movie_id = m.id AND c.job = 'Director')$a$),

('COM-09', 'complétude', 'people', 'gender',
 $$Genre de la personne connu$$, 'info', 1, 'conservation',
 $$Non renseigné par la communauté TMDB pour une grande partie des techniciens. Donnée personnelle : aucune déduction (par le prénom par exemple) n'est acceptable.$$,
 $p$SELECT count(*) FROM %1$I.people$p$,
 $a$SELECT id::text, name FROM %1$I.people WHERE gender IS NULL OR gender = 0$a$),

('COM-10', 'complétude', 'production_companies', 'origin_country',
 $$Pays d'origine de la société connu$$, 'info', 1, 'conservation',
 $$Non renseigné chez TMDB pour une partie des sociétés. Aucune source de substitution.$$,
 $p$SELECT count(*) FROM %1$I.production_companies$p$,
 $a$SELECT id::text, name FROM %1$I.production_companies
     WHERE origin_country IS NULL OR btrim(origin_country) = ''$a$),

('COM-11', 'complétude', 'movies', 'imdb_average_rating',
 $$Film doté d'un identifiant IMDb rapproché d'une note IMDb$$, 'majeure', 0.05, 'conservation',
 $$title.ratings ne contient que les titres ayant reçu des votes : les films pas encore sortis n'y figurent pas. Aucune note à imputer ; le rapprochement se fera de lui-même quand IMDb la publiera.$$,
 $p$SELECT count(*) FROM %1$I.movies WHERE imdb_id IS NOT NULL$p$,
 $a$SELECT id::text, imdb_id || ' ' || title FROM %1$I.movies
     WHERE imdb_id IS NOT NULL AND imdb_average_rating IS NULL$a$),

-- ---------------------------------------------------------------------
-- Unicité
-- ---------------------------------------------------------------------

('UNI-01', 'unicité', 'movies', 'id',
 $$Identifiant TMDB unique$$, 'critique', 0, 'suppression',
 $$Garanti par la clé primaire : le contrôle prouve que la contrainte tient. Un doublon serait supprimé en gardant la collecte la plus récente.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT id::text, count(*) || ' lignes' FROM %1$I.movies GROUP BY id HAVING count(*) > 1$a$),

('UNI-02', 'unicité', 'movies', 'imdb_id',
 $$Un identifiant IMDb ne désigne qu'un film TMDB$$, 'critique', 0, 'correction',
 $$Sinon la même note IMDb serait comptée deux fois. Le rapprochement est conservé pour le film le plus voté, retiré aux autres.$$,
 $p$SELECT count(imdb_id) FROM %1$I.movies$p$,
 $a$SELECT imdb_id, string_agg(id || ' ' || title, ' | ' ORDER BY id) FROM %1$I.movies
     WHERE imdb_id IS NOT NULL GROUP BY imdb_id HAVING count(*) > 1$a$),

('UNI-03', 'unicité', 'movies', 'original_title, release_date',
 $$Pas deux fiches pour un même film (titre original et année identiques)$$, 'majeure', 0, 'conservation',
 $$Doublon fonctionnel possible, mais les remakes et homonymes existent : sans identifiant IMDb commun, la fusion relève d'une revue manuelle.$$,
 $p$SELECT count(*) FROM %1$I.movies WHERE release_date IS NOT NULL$p$,
 $a$SELECT string_agg(id::text, '/' ORDER BY id), min(coalesce(original_title, title)) || ' (' || year || ')'
      FROM (SELECT id, title, original_title, extract(year FROM release_date)::int AS year,
                   lower(btrim(coalesce(original_title, title))) AS title_key
              FROM %1$I.movies WHERE release_date IS NOT NULL) m
     GROUP BY title_key, year HAVING count(*) > 1$a$),

('UNI-04', 'unicité', 'movie_cast', 'movie_id, person_id, character',
 $$Un rôle n'est crédité qu'une fois$$, 'mineure', 0, 'suppression',
 $$Même personne, même film, même personnage sous deux credit_id : le rôle serait compté deux fois. On garde le crédit le mieux placé au générique.$$,
 $p$SELECT count(*) FROM %1$I.movie_cast$p$,
 $a$SELECT credit_id, movie_id || '/' || person_id || ' ' || coalesce(character, '')
      FROM (SELECT *, row_number() OVER (PARTITION BY movie_id, person_id, character
                                          ORDER BY cast_order NULLS LAST, credit_id) AS rn
              FROM %1$I.movie_cast) c
     WHERE rn > 1$a$),

('UNI-05', 'unicité', 'movie_crew', 'movie_id, person_id, job',
 $$Un poste n'est crédité qu'une fois$$, 'mineure', 0, 'suppression',
 $$Même personne, même film, même poste sous deux credit_id. On garde un seul crédit.$$,
 $p$SELECT count(*) FROM %1$I.movie_crew$p$,
 $a$SELECT credit_id, movie_id || '/' || person_id || ' ' || coalesce(job, '')
      FROM (SELECT *, row_number() OVER (PARTITION BY movie_id, person_id, job ORDER BY credit_id) AS rn
              FROM %1$I.movie_crew) c
     WHERE rn > 1$a$),

('UNI-06', 'unicité', 'genres, collections, production_companies', 'name',
 $$Pas deux entrées de référentiel de même nom$$, 'info', 1, 'conservation',
 $$Des sociétés homonymes distinctes existent (filiales, pays différents) : TMDB reste la référence des identifiants, les homonymes sont seulement suivis.$$,
 $p$SELECT (SELECT count(*) FROM %1$I.genres) + (SELECT count(*) FROM %1$I.collections)
         + (SELECT count(*) FROM %1$I.production_companies)$p$,
 $a$SELECT 'genres:' || string_agg(id::text, '/' ORDER BY id), min(name) FROM %1$I.genres
     GROUP BY lower(btrim(name)) HAVING count(*) > 1
    UNION ALL
    SELECT 'collections:' || string_agg(id::text, '/' ORDER BY id), min(name) FROM %1$I.collections
     GROUP BY lower(btrim(name)) HAVING count(*) > 1
    UNION ALL
    SELECT 'production_companies:' || string_agg(id::text, '/' ORDER BY id), min(name) FROM %1$I.production_companies
     GROUP BY lower(btrim(name)) HAVING count(*) > 1$a$),

-- ---------------------------------------------------------------------
-- Validité
-- ---------------------------------------------------------------------

('VAL-01', 'validité', 'movies', 'imdb_id',
 $$Identifiant IMDb au format tt + 7 chiffres ou plus$$, 'majeure', 0, 'substitution',
 $$Un identifiant mal formé ne peut rien rapprocher et masquerait l'absence réelle : remplacé par NULL.$$,
 $p$SELECT count(imdb_id) FROM %1$I.movies$p$,
 $a$SELECT id::text, imdb_id FROM %1$I.movies WHERE imdb_id !~ '^tt[0-9]{7,}$'$a$),

('VAL-02', 'validité', 'movies', 'vote_average',
 $$Note TMDB comprise entre 0 et 10$$, 'critique', 0, 'substitution',
 $$Hors de l'échelle de notation, la valeur est fausse par construction : remplacée par NULL.$$,
 $p$SELECT count(vote_average) FROM %1$I.movies$p$,
 $a$SELECT id::text, vote_average::text FROM %1$I.movies WHERE vote_average NOT BETWEEN 0 AND 10$a$),

('VAL-03', 'validité', 'movies', 'imdb_average_rating',
 $$Note IMDb comprise entre 1 et 10$$, 'critique', 0, 'substitution',
 $$L'échelle IMDb va de 1 à 10 : hors de cette plage, la valeur est fausse et remplacée par NULL.$$,
 $p$SELECT count(imdb_average_rating) FROM %1$I.movies$p$,
 $a$SELECT id::text, imdb_average_rating::text FROM %1$I.movies WHERE imdb_average_rating NOT BETWEEN 1 AND 10$a$),

('VAL-04', 'validité', 'movies', 'vote_count, imdb_num_votes',
 $$Nombres de votes positifs ou nuls$$, 'critique', 0, 'substitution',
 $$Un compteur négatif est impossible : remplacé par NULL.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT id::text, 'vote_count=' || coalesce(vote_count::text, 'NULL') || ' imdb_num_votes=' || coalesce(imdb_num_votes::text, 'NULL')
      FROM %1$I.movies WHERE vote_count < 0 OR imdb_num_votes < 0$a$),

('VAL-05', 'validité', 'movies', 'runtime',
 $$Durée plausible (1 à 600 minutes)$$, 'majeure', 0, 'substitution',
 $$Au-delà de 10 heures il s'agit d'une erreur de saisie ou d'une œuvre hors du périmètre long métrage : la durée est remplacée par NULL plutôt que de fausser les moyennes.$$,
 $p$SELECT count(runtime) FROM %1$I.movies$p$,
 $a$SELECT id::text, runtime || ' min ' || title FROM %1$I.movies WHERE runtime NOT BETWEEN 1 AND 600$a$),

('VAL-06', 'validité', 'movies', 'release_date',
 $$Date de sortie plausible (1888 à aujourd'hui + 5 ans)$$, 'majeure', 0, 'substitution',
 $$Avant le premier film connu (1888) ou trop loin dans le futur, la date est une valeur par défaut ou une erreur : remplacée par NULL.$$,
 $p$SELECT count(release_date) FROM %1$I.movies$p$,
 $a$SELECT id::text, release_date::text FROM %1$I.movies
     WHERE release_date NOT BETWEEN DATE '1888-01-01' AND current_date + interval '5 years'$a$),

('VAL-07', 'validité', 'movies', 'status',
 $$Statut dans le domaine TMDB$$, 'majeure', 0, 'substitution',
 $$Six valeurs documentées (Rumored, Planned, In Production, Post Production, Released, Canceled). Toute autre valeur est remplacée par NULL.$$,
 $p$SELECT count(status) FROM %1$I.movies$p$,
 $a$SELECT id::text, status FROM %1$I.movies
     WHERE status NOT IN ('Rumored', 'Planned', 'In Production', 'Post Production', 'Released', 'Canceled')$a$),

('VAL-08', 'validité', 'movies, languages, movie_spoken_languages', 'codes langue',
 $$Codes langue au format ISO 639-1 (2 lettres minuscules)$$, 'mineure', 0, 'substitution',
 $$Un code mal formé ne correspond à aucune langue : remplacé par NULL (ou la ligne de liaison supprimée).$$,
 $p$SELECT (SELECT count(original_language) FROM %1$I.movies) + (SELECT count(*) FROM %1$I.languages)
         + (SELECT count(*) FROM %1$I.movie_spoken_languages)$p$,
 $a$SELECT 'movies:' || id, original_language::text FROM %1$I.movies
     WHERE original_language::text !~ '^[a-z]{2}$'
    UNION ALL
    SELECT 'languages:' || iso_639_1, iso_639_1::text FROM %1$I.languages
     WHERE iso_639_1::text !~ '^[a-z]{2}$'
    UNION ALL
    SELECT 'movie_spoken_languages:' || movie_id || '/' || language_id, language_id::text
      FROM %1$I.movie_spoken_languages WHERE language_id::text !~ '^[a-z]{2}$'$a$),

('VAL-09', 'validité', 'countries, movie_production_countries, production_companies', 'codes pays',
 $$Codes pays au format ISO 3166-1 alpha-2 (2 lettres majuscules)$$, 'majeure', 0, 'substitution',
 $$Le dictionnaire du TP1 prévoit NULL pour un pays inconnu ; un code vide ou mal formé fausse le comptage des pays renseignés. Remplacé par NULL.$$,
 $p$SELECT (SELECT count(*) FROM %1$I.countries) + (SELECT count(*) FROM %1$I.movie_production_countries)
         + (SELECT count(origin_country) FROM %1$I.production_companies)$p$,
 $a$SELECT 'countries:' || iso_3166_1, quote_literal(iso_3166_1::text) FROM %1$I.countries
     WHERE iso_3166_1::text !~ '^[A-Z]{2}$'
    UNION ALL
    SELECT 'movie_production_countries:' || movie_id || '/' || country_id, quote_literal(country_id::text)
      FROM %1$I.movie_production_countries WHERE country_id::text !~ '^[A-Z]{2}$'
    UNION ALL
    SELECT 'production_companies:' || id, quote_literal(origin_country::text) FROM %1$I.production_companies
     WHERE origin_country::text !~ '^[A-Z]{2}$'$a$),

('VAL-10', 'validité', 'movies', 'budget, revenue',
 $$Montants plausibles (au moins 1 000 USD)$$, 'majeure', 0, 'substitution',
 $$Un budget de quelques dollars est une saisie en millions ou une valeur symbolique. Le multiplier serait une supposition : la valeur est remplacée par NULL (inconnue).$$,
 $p$SELECT count(*) FROM %1$I.movies WHERE budget IS NOT NULL OR revenue IS NOT NULL$p$,
 $a$SELECT id::text, 'budget=' || coalesce(budget::text, 'NULL') || ' revenue=' || coalesce(revenue::text, 'NULL') || ' ' || title
      FROM %1$I.movies WHERE budget < 1000 OR revenue < 1000$a$),

('VAL-11', 'validité', 'movies', 'homepage',
 $$Site officiel au format URL http(s)$$, 'mineure', 0, 'substitution',
 $$Une adresse qui n'est pas une URL n'est pas cliquable dans le dashboard : remplacée par NULL.$$,
 $p$SELECT count(homepage) FROM %1$I.movies$p$,
 $a$SELECT id::text, homepage FROM %1$I.movies WHERE homepage !~ '^https?://[^[:space:]]+$'$a$),

('VAL-12', 'validité', 'movies, collections, production_companies, people', 'chemins d''image',
 $$Chemins d'image au format TMDB (/<nom>.<jpg|png|svg>)$$, 'mineure', 0, 'substitution',
 $$Préfixé par l'URL du CDN TMDB, un chemin mal formé donne une image cassée : remplacé par NULL.$$,
 $p$SELECT (SELECT count(poster_path) + count(backdrop_path) FROM %1$I.movies)
         + (SELECT count(poster_path) + count(backdrop_path) FROM %1$I.collections)
         + (SELECT count(logo_path) FROM %1$I.production_companies)
         + (SELECT count(profile_path) FROM %1$I.people)$p$,
 $a$SELECT 'movies:' || id, p FROM %1$I.movies, unnest(ARRAY[poster_path, backdrop_path]) AS p
     WHERE p !~ '^/[A-Za-z0-9_-]+\.(jpg|jpeg|png|svg)$'
    UNION ALL
    SELECT 'collections:' || id, p FROM %1$I.collections, unnest(ARRAY[poster_path, backdrop_path]) AS p
     WHERE p !~ '^/[A-Za-z0-9_-]+\.(jpg|jpeg|png|svg)$'
    UNION ALL
    SELECT 'production_companies:' || id, logo_path FROM %1$I.production_companies
     WHERE logo_path !~ '^/[A-Za-z0-9_-]+\.(jpg|jpeg|png|svg)$'
    UNION ALL
    SELECT 'people:' || id, profile_path FROM %1$I.people
     WHERE profile_path !~ '^/[A-Za-z0-9_-]+\.(jpg|jpeg|png|svg)$'$a$),

('VAL-13', 'validité', 'people', 'gender',
 $$Genre dans le domaine 1, 2, 3 (0 est une valeur sentinelle)$$, 'mineure', 0, 'substitution',
 $$TMDB code « non renseigné » par 0 : une fausse catégorie qui compte dans les répartitions. Remplacé par NULL, comme les 0 « inconnu » du budget et de la durée au TP2.$$,
 $p$SELECT count(gender) FROM %1$I.people$p$,
 $a$SELECT id::text, gender || ' ' || name FROM %1$I.people WHERE gender NOT IN (1, 2, 3)$a$),

('VAL-14', 'validité', 'movies, people, movie_cast', 'popularity, cast_order',
 $$Popularité et rang au générique positifs ou nuls$$, 'mineure', 0, 'substitution',
 $$Valeurs négatives impossibles par définition : remplacées par NULL.$$,
 $p$SELECT (SELECT count(popularity) FROM %1$I.movies) + (SELECT count(popularity) FROM %1$I.people)
         + (SELECT count(cast_order) FROM %1$I.movie_cast)$p$,
 $a$SELECT 'movies:' || id, popularity::text FROM %1$I.movies WHERE popularity < 0
    UNION ALL
    SELECT 'people:' || id, popularity::text FROM %1$I.people WHERE popularity < 0
    UNION ALL
    SELECT 'movie_cast:' || credit_id, cast_order::text FROM %1$I.movie_cast WHERE cast_order < 0$a$),

('VAL-15', 'validité', 'movie_crew, people', 'department, known_for_department',
 $$Département dans la liste TMDB$$, 'mineure', 0, 'substitution',
 $$Les départements forment une liste fermée chez TMDB (/configuration/jobs). Une valeur hors liste est remplacée par NULL.$$,
 $p$SELECT (SELECT count(department) FROM %1$I.movie_crew) + (SELECT count(known_for_department) FROM %1$I.people)$p$,
 $a$SELECT 'movie_crew:' || credit_id, department FROM %1$I.movie_crew
     WHERE department NOT IN ('Acting', 'Directing', 'Writing', 'Production', 'Camera', 'Editing', 'Sound',
                              'Art', 'Costume & Make-Up', 'Visual Effects', 'Lighting', 'Crew')
    UNION ALL
    SELECT 'people:' || id, known_for_department FROM %1$I.people
     WHERE known_for_department NOT IN ('Acting', 'Directing', 'Writing', 'Production', 'Camera', 'Editing',
                                        'Sound', 'Art', 'Costume & Make-Up', 'Visual Effects', 'Lighting',
                                        'Crew', 'Creator')$a$),

('VAL-16', 'validité', '(toutes)', '(colonnes texte)',
 $$Texte sans chaîne vide ni espace parasite$$, 'mineure', 0, 'correction',
 $$Une chaîne vide n'est pas une valeur : elle masque l'absence (complétude surestimée) et casse les comparaisons. Espaces retirés, chaînes vides remplacées par NULL, sur toutes les colonnes texte lues dans le catalogue.$$,
 $p$SELECT dq.text_cells(%1$L)$p$,
 $a$SELECT * FROM dq.text_hygiene(%1$L)$a$),

-- ---------------------------------------------------------------------
-- Cohérence
-- ---------------------------------------------------------------------

('COH-01', 'cohérence', 'movies', 'vote_average, vote_count',
 $$Pas de note TMDB sans vote$$, 'majeure', 0, 'substitution',
 $$TMDB affiche 0 quand personne n'a voté : ce n'est pas une note. Elle tire vers le bas les moyennes par genre et crée un écart TMDB − IMDb artificiel. Remplacée par NULL.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT id::text, 'vote_average=' || vote_average || ' vote_count=' || coalesce(vote_count::text, 'NULL') || ' ' || title
      FROM %1$I.movies WHERE coalesce(vote_count, 0) = 0 AND vote_average IS NOT NULL$a$),

('COH-02', 'cohérence', 'movies', 'has_imdb_match',
 $$Indicateur de rapprochement conforme aux données IMDb présentes$$, 'critique', 0, 'correction',
 $$Colonne dérivée : vraie si et seulement si une note IMDb est présente, et jamais sans identifiant IMDb. Recalculée.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT id::text, 'has_imdb_match=' || has_imdb_match || ' imdb_id=' || coalesce(imdb_id, 'NULL')
                    || ' note=' || coalesce(imdb_average_rating::text, 'NULL')
      FROM %1$I.movies
     WHERE has_imdb_match <> (imdb_average_rating IS NOT NULL) OR (has_imdb_match AND imdb_id IS NULL)$a$),

('COH-03', 'cohérence', 'movies', 'rating_gap',
 $$Écart de notation = note TMDB − note IMDb$$, 'majeure', 0, 'correction',
 $$Colonne dérivée, cœur du rapprochement métier : elle doit suivre exactement les deux notes stockées. Recalculée.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT id::text, 'rating_gap=' || coalesce(rating_gap::text, 'NULL') || ' attendu='
                    || coalesce(round(vote_average - imdb_average_rating, 2)::text, 'NULL')
      FROM %1$I.movies
     WHERE rating_gap IS DISTINCT FROM round(vote_average - imdb_average_rating, 2)$a$),

('COH-04', 'cohérence', 'movies', 'votes_ratio',
 $$Rapport des votes = votes TMDB / votes IMDb$$, 'mineure', 0, 'correction',
 $$Colonne dérivée : recalculée à partir des deux compteurs stockés.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT id::text, 'votes_ratio=' || coalesce(votes_ratio::text, 'NULL') || ' attendu='
                    || coalesce(round(vote_count::numeric / nullif(imdb_num_votes, 0), 4)::text, 'NULL')
      FROM %1$I.movies
     WHERE votes_ratio IS DISTINCT FROM round(vote_count::numeric / nullif(imdb_num_votes, 0), 4)$a$),

('COH-05', 'cohérence', 'movies', 'status, release_date',
 $$Un film « Released » a une date de sortie passée$$, 'majeure', 0, 'conservation',
 $$La date TMDB est la sortie principale, le statut peut refléter une sortie antérieure dans un autre pays. Sans savoir laquelle des deux valeurs est fausse, on signale sans corriger.$$,
 $p$SELECT count(*) FROM %1$I.movies WHERE status = 'Released'$p$,
 $a$SELECT id::text, release_date || ' ' || title FROM %1$I.movies
     WHERE status = 'Released' AND release_date > current_date$a$),

('COH-06', 'cohérence', 'movies', 'status, revenue',
 $$Pas de recettes pour un film non sorti$$, 'mineure', 0, 'conservation',
 $$Recettes d'une sortie partielle (festival, avant-première) ou statut en retard : signalé, les deux valeurs pouvant être justes.$$,
 $p$SELECT count(*) FROM %1$I.movies WHERE status <> 'Released'$p$,
 $a$SELECT id::text, status || ' revenue=' || revenue || ' ' || title FROM %1$I.movies
     WHERE status <> 'Released' AND revenue IS NOT NULL$a$),

('COH-07', 'cohérence', 'movies', 'runtime, imdb_runtime_minutes',
 $$Durées TMDB et IMDb proches (écart de 30 minutes au plus)$$, 'mineure', 0.05, 'conservation',
 $$Deux montages (version cinéma, version longue) justifient un écart. Signalé pour revue ; TMDB reste la référence.$$,
 $p$SELECT count(*) FROM %1$I.movies WHERE runtime IS NOT NULL AND imdb_runtime_minutes IS NOT NULL$p$,
 $a$SELECT id::text, runtime || ' / ' || imdb_runtime_minutes || ' min ' || title FROM %1$I.movies
     WHERE abs(runtime - imdb_runtime_minutes) > 30$a$),

('COH-08', 'cohérence', 'movies', 'release_date, imdb_start_year',
 $$Années de sortie TMDB et IMDb proches (1 an d'écart au plus)$$, 'mineure', 0.05, 'conservation',
 $$Festival puis sortie en salle l'année suivante : un écart d'un an est normal. Au-delà, signalé pour revue ; TMDB reste la référence.$$,
 $p$SELECT count(*) FROM %1$I.movies WHERE release_date IS NOT NULL AND imdb_start_year IS NOT NULL$p$,
 $a$SELECT id::text, extract(year FROM release_date) || ' / ' || imdb_start_year || ' ' || title FROM %1$I.movies
     WHERE abs(extract(year FROM release_date) - imdb_start_year) > 1$a$),

('COH-09', 'cohérence', 'liaisons', NULL,
 $$Les liaisons d'un film reflètent son dernier état connu$$, 'majeure', 0, 'suppression',
 $$Le chargement TP2 ajoute les liaisons sans jamais retirer celles qui ont disparu chez TMDB (genre reclassé, crédit supprimé). Comparées au dernier chargement (tables _stg_), les liaisons obsolètes sont supprimées.$$,
 $p$SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM mart._stg_movies) THEN 0 ELSE
           (SELECT count(*) FROM %1$I.movie_genres WHERE movie_id IN (SELECT id FROM mart._stg_movies))
         + (SELECT count(*) FROM %1$I.movie_production_companies WHERE movie_id IN (SELECT id FROM mart._stg_movies))
         + (SELECT count(*) FROM %1$I.movie_production_countries WHERE movie_id IN (SELECT id FROM mart._stg_movies))
         + (SELECT count(*) FROM %1$I.movie_spoken_languages WHERE movie_id IN (SELECT id FROM mart._stg_movies))
         + (SELECT count(*) FROM %1$I.movie_cast WHERE movie_id IN (SELECT id FROM mart._stg_movies))
         + (SELECT count(*) FROM %1$I.movie_crew WHERE movie_id IN (SELECT id FROM mart._stg_movies)) END$p$,
 $a$SELECT 'movie_genres:' || t.movie_id || '/' || t.genre_id, 'absente du dernier chargement'
      FROM %1$I.movie_genres t
     WHERE t.movie_id IN (SELECT id FROM mart._stg_movies)
       AND NOT EXISTS (SELECT 1 FROM mart._stg_movie_genres s WHERE s.movie_id = t.movie_id AND s.genre_id = t.genre_id)
    UNION ALL
    SELECT 'movie_production_companies:' || t.movie_id || '/' || t.company_id, 'absente du dernier chargement'
      FROM %1$I.movie_production_companies t
     WHERE t.movie_id IN (SELECT id FROM mart._stg_movies)
       AND NOT EXISTS (SELECT 1 FROM mart._stg_movie_production_companies s
                        WHERE s.movie_id = t.movie_id AND s.company_id = t.company_id)
    UNION ALL
    SELECT 'movie_production_countries:' || t.movie_id || '/' || t.country_id, 'absente du dernier chargement'
      FROM %1$I.movie_production_countries t
     WHERE t.movie_id IN (SELECT id FROM mart._stg_movies)
       AND NOT EXISTS (SELECT 1 FROM mart._stg_movie_production_countries s
                        WHERE s.movie_id = t.movie_id AND s.country_id = t.country_id)
    UNION ALL
    SELECT 'movie_spoken_languages:' || t.movie_id || '/' || t.language_id, 'absente du dernier chargement'
      FROM %1$I.movie_spoken_languages t
     WHERE t.movie_id IN (SELECT id FROM mart._stg_movies)
       AND NOT EXISTS (SELECT 1 FROM mart._stg_movie_spoken_languages s
                        WHERE s.movie_id = t.movie_id AND s.language_id = t.language_id)
    UNION ALL
    SELECT 'movie_cast:' || t.credit_id, t.movie_id || ' ' || coalesce(t.character, '')
      FROM %1$I.movie_cast t
     WHERE t.movie_id IN (SELECT id FROM mart._stg_movies)
       AND NOT EXISTS (SELECT 1 FROM mart._stg_movie_cast s WHERE s.credit_id = t.credit_id)
    UNION ALL
    SELECT 'movie_crew:' || t.credit_id, t.movie_id || ' ' || coalesce(t.job, '')
      FROM %1$I.movie_crew t
     WHERE t.movie_id IN (SELECT id FROM mart._stg_movies)
       AND NOT EXISTS (SELECT 1 FROM mart._stg_movie_crew s WHERE s.credit_id = t.credit_id)$a$),

('COH-10', 'cohérence', 'movies', 'source_fetched_at, loaded_at, ingest_date',
 $$Chronologie de traçabilité : collecte, puis agrégation, puis chargement$$, 'mineure', 0, 'conservation',
 $$Une ligne chargée avant d'avoir été collectée signalerait une horloge fausse ou un mélange d'instantanés. Traçabilité : on signale, on ne réécrit pas l'histoire.$$,
 $p$SELECT count(*) FROM %1$I.movies$p$,
 $a$SELECT id::text, 'fetched=' || source_fetched_at || ' ingest=' || ingest_date || ' loaded=' || loaded_at
      FROM %1$I.movies
     WHERE source_fetched_at > loaded_at OR ingest_date < (source_fetched_at AT TIME ZONE 'UTC')::date$a$),

-- ---------------------------------------------------------------------
-- Intégrité
-- ---------------------------------------------------------------------

('INT-01', 'intégrité', 'liaisons', 'clés étrangères',
 $$Toute liaison pointe vers un film et un élément existants$$, 'critique', 0, 'suppression',
 $$Garanti par les clés étrangères du mart : le contrôle le prouve. Une liaison orpheline serait supprimée.$$,
 $p$SELECT (SELECT count(*) FROM %1$I.movie_genres) + (SELECT count(*) FROM %1$I.movie_production_companies)
         + (SELECT count(*) FROM %1$I.movie_production_countries) + (SELECT count(*) FROM %1$I.movie_spoken_languages)
         + (SELECT count(*) FROM %1$I.movie_cast) + (SELECT count(*) FROM %1$I.movie_crew)$p$,
 $a$SELECT 'movie_genres:' || l.movie_id || '/' || l.genre_id, 'film ou genre absent' FROM %1$I.movie_genres l
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movies m WHERE m.id = l.movie_id)
        OR NOT EXISTS (SELECT 1 FROM %1$I.genres g WHERE g.id = l.genre_id)
    UNION ALL
    SELECT 'movie_production_companies:' || l.movie_id || '/' || l.company_id, 'film ou société absent'
      FROM %1$I.movie_production_companies l
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movies m WHERE m.id = l.movie_id)
        OR NOT EXISTS (SELECT 1 FROM %1$I.production_companies c WHERE c.id = l.company_id)
    UNION ALL
    SELECT 'movie_production_countries:' || l.movie_id || '/' || l.country_id, 'film ou pays absent'
      FROM %1$I.movie_production_countries l
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movies m WHERE m.id = l.movie_id)
        OR NOT EXISTS (SELECT 1 FROM %1$I.countries c WHERE c.iso_3166_1 = l.country_id)
    UNION ALL
    SELECT 'movie_spoken_languages:' || l.movie_id || '/' || l.language_id, 'film ou langue absent'
      FROM %1$I.movie_spoken_languages l
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movies m WHERE m.id = l.movie_id)
        OR NOT EXISTS (SELECT 1 FROM %1$I.languages x WHERE x.iso_639_1 = l.language_id)
    UNION ALL
    SELECT 'movie_cast:' || l.credit_id, 'film ou personne absent' FROM %1$I.movie_cast l
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movies m WHERE m.id = l.movie_id)
        OR NOT EXISTS (SELECT 1 FROM %1$I.people p WHERE p.id = l.person_id)
    UNION ALL
    SELECT 'movie_crew:' || l.credit_id, 'film ou personne absent' FROM %1$I.movie_crew l
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movies m WHERE m.id = l.movie_id)
        OR NOT EXISTS (SELECT 1 FROM %1$I.people p WHERE p.id = l.person_id)$a$),

('INT-02', 'intégrité', 'movies', 'collection_id',
 $$Toute saga référencée existe$$, 'critique', 0, 'substitution',
 $$Garanti par la clé étrangère du mart. Une référence cassée serait remplacée par NULL (règle ON DELETE SET NULL du modèle).$$,
 $p$SELECT count(collection_id) FROM %1$I.movies$p$,
 $a$SELECT m.id::text, m.collection_id::text FROM %1$I.movies m
     WHERE m.collection_id IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM %1$I.collections c WHERE c.id = m.collection_id)$a$),

('INT-03', 'intégrité', 'genres, collections, production_companies, countries, languages', NULL,
 $$Tout élément de référentiel est relié à au moins un film$$, 'mineure', 0, 'suppression',
 $$Cardinalité (1,N) du MCD : ces lignes n'existent que par un film. Orphelines après la disparition de leurs liaisons, elles gonflent les référentiels du dashboard. Supprimées.$$,
 $p$SELECT (SELECT count(*) FROM %1$I.genres) + (SELECT count(*) FROM %1$I.collections)
         + (SELECT count(*) FROM %1$I.production_companies) + (SELECT count(*) FROM %1$I.countries)
         + (SELECT count(*) FROM %1$I.languages)$p$,
 $a$SELECT 'genres:' || g.id, g.name FROM %1$I.genres g
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movie_genres l WHERE l.genre_id = g.id)
    UNION ALL
    SELECT 'collections:' || c.id, c.name FROM %1$I.collections c
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movies m WHERE m.collection_id = c.id)
    UNION ALL
    SELECT 'production_companies:' || c.id, c.name FROM %1$I.production_companies c
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movie_production_companies l WHERE l.company_id = c.id)
    UNION ALL
    SELECT 'countries:' || c.iso_3166_1, c.name FROM %1$I.countries c
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movie_production_countries l WHERE l.country_id = c.iso_3166_1)
    UNION ALL
    SELECT 'languages:' || x.iso_639_1, x.english_name FROM %1$I.languages x
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movie_spoken_languages l WHERE l.language_id = x.iso_639_1)$a$),

('INT-04', 'intégrité', 'people', NULL,
 $$Toute personne a au moins un crédit$$, 'mineure', 0, 'suppression',
 $$Une personne n'entre dans le modèle que par un crédit de casting ou d'équipe. Sans crédit, elle est orpheline : supprimée.$$,
 $p$SELECT count(*) FROM %1$I.people$p$,
 $a$SELECT p.id::text, p.name FROM %1$I.people p
     WHERE NOT EXISTS (SELECT 1 FROM %1$I.movie_cast c WHERE c.person_id = p.id)
       AND NOT EXISTS (SELECT 1 FROM %1$I.movie_crew c WHERE c.person_id = p.id)$a$),

('INT-05', 'intégrité', 'movies', 'original_language',
 $$Langue originale présente dans le référentiel des langues$$, 'info', 1, 'conservation',
 $$Choix du TP1 : pas de clé étrangère, le référentiel n'étant alimenté que par les langues parlées. Suivi de l'écart, sans correction.$$,
 $p$SELECT count(original_language) FROM %1$I.movies$p$,
 $a$SELECT m.id::text, m.original_language::text FROM %1$I.movies m
     WHERE m.original_language IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM %1$I.languages x WHERE x.iso_639_1 = m.original_language)$a$),

('INT-06', 'intégrité', 'production_companies', 'origin_country',
 $$Pays d'origine des sociétés présent dans le référentiel des pays$$, 'info', 1, 'conservation',
 $$Choix du TP1 : pas de clé étrangère, le référentiel n'étant alimenté que par les pays de production. Suivi de l'écart, sans correction.$$,
 $p$SELECT count(*) FROM %1$I.production_companies WHERE btrim(origin_country) <> ''$p$,
 $a$SELECT c.id::text, c.origin_country::text FROM %1$I.production_companies c
     WHERE btrim(c.origin_country) <> ''
       AND NOT EXISTS (SELECT 1 FROM %1$I.countries x WHERE x.iso_3166_1 = c.origin_country)$a$)
),
removed AS (
    DELETE FROM dq.controls WHERE control_id NOT IN (SELECT control_id FROM matrix)
)
INSERT INTO dq.controls (control_id, dimension, table_name, column_name, rule, severity, threshold,
                         treatment, justification, population_sql, anomaly_sql, updated_at)
SELECT control_id, dimension, table_name, column_name, rule, severity, threshold,
       treatment, justification, population_sql, anomaly_sql, now()
FROM matrix
ON CONFLICT (control_id) DO UPDATE SET
    dimension      = EXCLUDED.dimension,
    table_name     = EXCLUDED.table_name,
    column_name    = EXCLUDED.column_name,
    rule           = EXCLUDED.rule,
    severity       = EXCLUDED.severity,
    threshold      = EXCLUDED.threshold,
    treatment      = EXCLUDED.treatment,
    justification  = EXCLUDED.justification,
    population_sql = EXCLUDED.population_sql,
    anomaly_sql    = EXCLUDED.anomaly_sql,
    updated_at     = EXCLUDED.updated_at;
