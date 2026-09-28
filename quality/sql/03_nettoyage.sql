-- =====================================================================
-- TP3 — Nettoyage : construction du schéma « curated ».
--
-- curated part d'une copie conforme du mart, sur laquelle chaque décision
-- de la matrice est appliquée, dans l'ordre :
--
--   1. normaliser (texte) ;
--   2. invalider les valeurs impossibles (substitution par NULL) ;
--   3. imputer ce qu'une autre source connaît ;
--   4. recalculer les colonnes dérivées ;
--   5. supprimer liaisons obsolètes, doublons puis orphelins.
--
-- Pourquoi un schéma à part plutôt que corriger le mart :
--   - le mart est réécrit par UPSERT à chaque cycle du pipeline, une
--     correction en place serait écrasée au cycle suivant ;
--   - l'état d'origine reste disponible pour la comparaison avant/après,
--     comme la zone raw du Data Lake n'est jamais modifiée.
-- Les corrections durables sont remontées dans le pipeline lui-même
-- (spark/aggregate.py et spark/load_mart.py).
--
-- Chaque correction passe par dq.fix() ou dq.remove(), qui journalisent la
-- clé, l'ancienne et la nouvelle valeur dans dq.corrections. Le schéma est
-- recréé à chaque exécution : il est entièrement dérivé du mart.
-- =====================================================================

DROP SCHEMA IF EXISTS curated CASCADE;
CREATE SCHEMA curated;
COMMENT ON SCHEMA curated IS 'Données nettoyées et conformes au schéma cible (TP3), dérivées du mart';

-- Copie conforme : CREATE TABLE AS conserve les types exacts des colonnes
-- (NUMERIC(4,2), CHAR(2)...). Les contraintes sont posées après nettoyage,
-- par 04_schema_cible.sql.
DO $$
DECLARE
    t RECORD;
BEGIN
    FOR t IN SELECT table_name FROM dq.model_tables ORDER BY load_order LOOP
        EXECUTE format('CREATE TABLE curated.%I AS SELECT * FROM mart.%I', t.table_name, t.table_name);
    END LOOP;
END $$;

-- ---------------------------------------------------------------------
-- 1. Normalisation du texte                                      VAL-16
--
-- Sur toutes les colonnes texte, lues dans le catalogue : espaces retirés,
-- chaîne vide remplacée par NULL. Une chaîne vide n'est pas une valeur :
-- elle fait passer pour renseigné un champ vide (pays de société '',
-- personnage ''). Couvre aussi les codes CHAR(2) vides, que le dictionnaire
-- du TP1 annonçait déjà comme NULL.
-- ---------------------------------------------------------------------

SELECT c.table_name, c.column_name,
       dq.fix('VAL-16', 'correction', c.table_name, c.column_name,
              format('nullif(btrim(t.%I::text), '''')', c.column_name),
              format('t.%1$I IS NOT NULL AND (btrim(t.%1$I::text) = '''' OR t.%1$I::text <> btrim(t.%1$I::text))',
                     c.column_name)) AS corrected
FROM dq.text_columns('curated') c;

-- ---------------------------------------------------------------------
-- 2. Valeurs impossibles remplacées par NULL                VAL-01 à 15
--
-- Une valeur hors domaine est fausse, mais la vraie valeur est inconnue :
-- la remplacer par une estimation (moyenne, multiplication...) serait une
-- supposition présentée comme un fait. NULL dit exactement ce que l'on sait.
-- ---------------------------------------------------------------------

-- Identifiant IMDb mal formé : il ne peut rien rapprocher.
SELECT dq.fix('VAL-01', 'substitution', 'movies', 'imdb_id', 'NULL',
              $$t.imdb_id !~ '^tt[0-9]{7,}$'$$);

-- Notes et compteurs hors échelle.
SELECT dq.fix('VAL-02', 'substitution', 'movies', 'vote_average', 'NULL',
              't.vote_average NOT BETWEEN 0 AND 10');
SELECT dq.fix('VAL-03', 'substitution', 'movies', 'imdb_average_rating', 'NULL',
              't.imdb_average_rating NOT BETWEEN 1 AND 10');
SELECT dq.fix('VAL-04', 'substitution', 'movies', 'vote_count', 'NULL', 't.vote_count < 0');
SELECT dq.fix('VAL-04', 'substitution', 'movies', 'imdb_num_votes', 'NULL', 't.imdb_num_votes < 0');

-- Durée et date hors de toute plausibilité.
SELECT dq.fix('VAL-05', 'substitution', 'movies', 'runtime', 'NULL',
              't.runtime NOT BETWEEN 1 AND 600');
SELECT dq.fix('VAL-06', 'substitution', 'movies', 'release_date', 'NULL',
              $$t.release_date NOT BETWEEN DATE '1888-01-01' AND current_date + interval '5 years'$$);

-- Statut hors domaine TMDB.
SELECT dq.fix('VAL-07', 'substitution', 'movies', 'status', 'NULL',
              $$t.status NOT IN ('Rumored', 'Planned', 'In Production', 'Post Production', 'Released', 'Canceled')$$);

-- Codes langue et pays : un code mal formé ne désigne rien. Dans une table
-- de liaison ou un référentiel, le code est la clé : la ligne est supprimée.
SELECT dq.fix('VAL-08', 'substitution', 'movies', 'original_language', 'NULL',
              $$t.original_language::text !~ '^[a-z]{2}$'$$);
SELECT dq.remove('VAL-08', 'movie_spoken_languages', $$t.language_id::text !~ '^[a-z]{2}$'$$);
SELECT dq.remove('VAL-08', 'languages', $$t.iso_639_1::text !~ '^[a-z]{2}$'$$);
SELECT dq.fix('VAL-09', 'substitution', 'production_companies', 'origin_country', 'NULL',
              $$t.origin_country::text !~ '^[A-Z]{2}$'$$);
SELECT dq.remove('VAL-09', 'movie_production_countries', $$t.country_id::text !~ '^[A-Z]{2}$'$$);
SELECT dq.remove('VAL-09', 'countries', $$t.iso_3166_1::text !~ '^[A-Z]{2}$'$$);

-- Montants symboliques : un budget de 7 USD est une saisie en millions ou
-- une valeur de remplissage. Multiplier par un million serait un pari.
SELECT dq.fix('VAL-10', 'substitution', 'movies', 'budget', 'NULL', 't.budget < 1000');
SELECT dq.fix('VAL-10', 'substitution', 'movies', 'revenue', 'NULL', 't.revenue < 1000');

-- Liens et images inexploitables par le dashboard.
SELECT dq.fix('VAL-11', 'substitution', 'movies', 'homepage', 'NULL',
              $$t.homepage !~ '^https?://[^[:space:]]+$'$$);

SELECT dq.fix('VAL-12', 'substitution', i.table_name, i.column_name, 'NULL',
              format($$t.%I !~ '^/[A-Za-z0-9_-]+\.(jpg|jpeg|png|svg)$'$$, i.column_name)) AS corrected,
       i.table_name, i.column_name
FROM (VALUES ('movies', 'poster_path'), ('movies', 'backdrop_path'),
             ('collections', 'poster_path'), ('collections', 'backdrop_path'),
             ('production_companies', 'logo_path'), ('people', 'profile_path')) AS i (table_name, column_name);

-- Genre : 0 est la valeur sentinelle TMDB de « non renseigné ». Comme les
-- 0 du budget ou de la durée au TP2, elle devient NULL : sinon elle forme
-- une fausse catégorie dans toute répartition par genre.
SELECT dq.fix('VAL-13', 'substitution', 'people', 'gender', 'NULL', 't.gender NOT IN (1, 2, 3)');

SELECT dq.fix('VAL-14', 'substitution', 'movies', 'popularity', 'NULL', 't.popularity < 0');
SELECT dq.fix('VAL-14', 'substitution', 'people', 'popularity', 'NULL', 't.popularity < 0');
SELECT dq.fix('VAL-14', 'substitution', 'movie_cast', 'cast_order', 'NULL', 't.cast_order < 0');

SELECT dq.fix('VAL-15', 'substitution', 'movie_crew', 'department', 'NULL',
              $$t.department NOT IN ('Acting', 'Directing', 'Writing', 'Production', 'Camera', 'Editing',
                                     'Sound', 'Art', 'Costume & Make-Up', 'Visual Effects', 'Lighting', 'Crew')$$);
SELECT dq.fix('VAL-15', 'substitution', 'people', 'known_for_department', 'NULL',
              $$t.known_for_department NOT IN ('Acting', 'Directing', 'Writing', 'Production', 'Camera',
                                               'Editing', 'Sound', 'Art', 'Costume & Make-Up',
                                               'Visual Effects', 'Lighting', 'Crew', 'Creator')$$);

-- Note sans vote : TMDB affiche 0 quand personne n'a voté. Ce 0 n'est pas
-- une note : il tire les moyennes par genre vers le bas et fabrique un
-- écart TMDB − IMDb de plusieurs points.                          COH-01
SELECT dq.fix('COH-01', 'substitution', 'movies', 'vote_average', 'NULL',
              'coalesce(t.vote_count, 0) = 0 AND t.vote_average IS NOT NULL');

-- Un identifiant IMDb partagé par deux films : la note n'est gardée que
-- pour le film le plus voté, retirée aux autres.                  UNI-02
SELECT dq.fix('UNI-02', 'correction', 'movies', 'imdb_id', 'NULL',
              $$EXISTS (SELECT 1 FROM curated.movies o
                         WHERE o.imdb_id = t.imdb_id AND o.id <> t.id
                           AND (coalesce(o.vote_count, 0), o.id) > (coalesce(t.vote_count, 0), t.id))$$);

-- Saga introuvable : même règle que la clé étrangère du modèle
-- (ON DELETE SET NULL).                                           INT-02
SELECT dq.fix('INT-02', 'substitution', 'movies', 'collection_id', 'NULL',
              't.collection_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM curated.collections c WHERE c.id = t.collection_id)');

-- ---------------------------------------------------------------------
-- 3. Imputation inter-sources                                    COM-03
--
-- La durée IMDb (title.basics) mesure la même grandeur que la durée TMDB :
-- quand TMDB l'ignore et qu'IMDb la connaît, elle est reprise. C'est le
-- seul champ où une seconde source indépendante fournit la valeur exacte ;
-- ailleurs, aucune imputation n'est faite.
-- ---------------------------------------------------------------------

SELECT dq.fix('COM-03', 'imputation', 'movies', 'runtime', 't.imdb_runtime_minutes',
              't.runtime IS NULL AND t.imdb_runtime_minutes BETWEEN 1 AND 600');

-- ---------------------------------------------------------------------
-- 4. Colonnes dérivées recalculées                        COH-02 à 04
--
-- has_imdb_match, rating_gap et votes_ratio sont calculés par Spark. Ils
-- doivent suivre exactement les valeurs stockées, y compris après les
-- substitutions ci-dessus. Spark calcule l'écart sur la note avant son
-- arrondi à 2 décimales, d'où des écarts d'un centième avec les notes
-- affichées : le recalcul les aligne.
-- ---------------------------------------------------------------------

-- Sans identifiant IMDb, les données IMDb n'ont plus de support.
SELECT dq.fix('COH-02', 'correction', 'movies', c.column_name, 'NULL',
              format('t.imdb_id IS NULL AND t.%I IS NOT NULL', c.column_name)) AS corrected, c.column_name
FROM (VALUES ('imdb_average_rating'), ('imdb_num_votes'), ('imdb_start_year'),
             ('imdb_runtime_minutes'), ('imdb_genres')) AS c (column_name);

SELECT dq.fix('COH-02', 'correction', 'movies', 'has_imdb_match', '(t.imdb_average_rating IS NOT NULL)',
              't.has_imdb_match IS DISTINCT FROM (t.imdb_average_rating IS NOT NULL)');
SELECT dq.fix('COH-03', 'correction', 'movies', 'rating_gap', 'round(t.vote_average - t.imdb_average_rating, 2)',
              't.rating_gap IS DISTINCT FROM round(t.vote_average - t.imdb_average_rating, 2)');
SELECT dq.fix('COH-04', 'correction', 'movies', 'votes_ratio',
              'round(t.vote_count::numeric / nullif(t.imdb_num_votes, 0), 4)',
              't.votes_ratio IS DISTINCT FROM round(t.vote_count::numeric / nullif(t.imdb_num_votes, 0), 4)');

-- ---------------------------------------------------------------------
-- 5. Suppressions : liaisons obsolètes, doublons, puis orphelins
--
-- L'ordre compte :
--   - les liaisons obsolètes partent avant le dédoublonnage. Quand TMDB
--     recrée un crédit, l'ancien reste dans le mart à côté du nouveau et
--     forme un faux doublon ; dédoublonner d'abord peut garder l'ancien, que
--     l'étape suivante supprime : le rôle disparaîtrait. Cas réel relevé
--     dans le journal des corrections au premier audit ;
--   - retirer une liaison peut rendre orphelin un genre, une saga ou une
--     personne, qui est alors supprimé en dernier.
-- ---------------------------------------------------------------------

-- Liaisons obsolètes : le chargement du TP2 ajoute les liaisons d'un film
-- sans retirer celles qui ont disparu chez TMDB. Le dernier chargement
-- (tables de transit _stg_) donne l'état actuel de chaque film rechargé ;
-- ce qui n'y figure plus est supprimé. Sans chargement disponible (transit
-- vide juste après un redémarrage), rien n'est supprimé.        COH-09
SELECT dq.remove('COH-09', 'movie_genres',
                 $$t.movie_id IN (SELECT id FROM mart._stg_movies)
                   AND NOT EXISTS (SELECT 1 FROM mart._stg_movie_genres s
                                    WHERE s.movie_id = t.movie_id AND s.genre_id = t.genre_id)$$);
SELECT dq.remove('COH-09', 'movie_production_companies',
                 $$t.movie_id IN (SELECT id FROM mart._stg_movies)
                   AND NOT EXISTS (SELECT 1 FROM mart._stg_movie_production_companies s
                                    WHERE s.movie_id = t.movie_id AND s.company_id = t.company_id)$$);
SELECT dq.remove('COH-09', 'movie_production_countries',
                 $$t.movie_id IN (SELECT id FROM mart._stg_movies)
                   AND NOT EXISTS (SELECT 1 FROM mart._stg_movie_production_countries s
                                    WHERE s.movie_id = t.movie_id AND s.country_id = t.country_id)$$);
SELECT dq.remove('COH-09', 'movie_spoken_languages',
                 $$t.movie_id IN (SELECT id FROM mart._stg_movies)
                   AND NOT EXISTS (SELECT 1 FROM mart._stg_movie_spoken_languages s
                                    WHERE s.movie_id = t.movie_id AND s.language_id = t.language_id)$$);
SELECT dq.remove('COH-09', 'movie_cast',
                 $$t.movie_id IN (SELECT id FROM mart._stg_movies)
                   AND NOT EXISTS (SELECT 1 FROM mart._stg_movie_cast s WHERE s.credit_id = t.credit_id)$$);
SELECT dq.remove('COH-09', 'movie_crew',
                 $$t.movie_id IN (SELECT id FROM mart._stg_movies)
                   AND NOT EXISTS (SELECT 1 FROM mart._stg_movie_crew s WHERE s.credit_id = t.credit_id)$$);

-- Crédits en double : même rôle ou même poste sous deux credit_id.  UNI-04/05
SELECT dq.remove('UNI-04', 'movie_cast',
                 $$t.credit_id IN (SELECT credit_id FROM (
                       SELECT credit_id, row_number() OVER (PARTITION BY movie_id, person_id, character
                                                            ORDER BY cast_order NULLS LAST, credit_id) AS rn
                         FROM curated.movie_cast) d WHERE d.rn > 1)$$);
SELECT dq.remove('UNI-05', 'movie_crew',
                 $$t.credit_id IN (SELECT credit_id FROM (
                       SELECT credit_id, row_number() OVER (PARTITION BY movie_id, person_id, job
                                                            ORDER BY credit_id) AS rn
                         FROM curated.movie_crew) d WHERE d.rn > 1)$$);

-- Liaisons orphelines : garanties absentes par les clés étrangères du mart,
-- supprimées si elles apparaissaient.                             INT-01
SELECT dq.remove('INT-01', 'movie_genres',
                 'NOT EXISTS (SELECT 1 FROM curated.movies m WHERE m.id = t.movie_id)
                  OR NOT EXISTS (SELECT 1 FROM curated.genres g WHERE g.id = t.genre_id)');
SELECT dq.remove('INT-01', 'movie_production_companies',
                 'NOT EXISTS (SELECT 1 FROM curated.movies m WHERE m.id = t.movie_id)
                  OR NOT EXISTS (SELECT 1 FROM curated.production_companies c WHERE c.id = t.company_id)');
SELECT dq.remove('INT-01', 'movie_production_countries',
                 'NOT EXISTS (SELECT 1 FROM curated.movies m WHERE m.id = t.movie_id)
                  OR NOT EXISTS (SELECT 1 FROM curated.countries c WHERE c.iso_3166_1 = t.country_id)');
SELECT dq.remove('INT-01', 'movie_spoken_languages',
                 'NOT EXISTS (SELECT 1 FROM curated.movies m WHERE m.id = t.movie_id)
                  OR NOT EXISTS (SELECT 1 FROM curated.languages x WHERE x.iso_639_1 = t.language_id)');
SELECT dq.remove('INT-01', 'movie_cast',
                 'NOT EXISTS (SELECT 1 FROM curated.movies m WHERE m.id = t.movie_id)
                  OR NOT EXISTS (SELECT 1 FROM curated.people p WHERE p.id = t.person_id)');
SELECT dq.remove('INT-01', 'movie_crew',
                 'NOT EXISTS (SELECT 1 FROM curated.movies m WHERE m.id = t.movie_id)
                  OR NOT EXISTS (SELECT 1 FROM curated.people p WHERE p.id = t.person_id)');

-- Référentiels et personnes sans film : la cardinalité (1,N) du MCD n'est
-- plus respectée, ils n'ont plus de raison d'être.            INT-03/04
SELECT dq.remove('INT-03', 'genres',
                 'NOT EXISTS (SELECT 1 FROM curated.movie_genres l WHERE l.genre_id = t.id)');
SELECT dq.remove('INT-03', 'collections',
                 'NOT EXISTS (SELECT 1 FROM curated.movies m WHERE m.collection_id = t.id)');
SELECT dq.remove('INT-03', 'production_companies',
                 'NOT EXISTS (SELECT 1 FROM curated.movie_production_companies l WHERE l.company_id = t.id)');
SELECT dq.remove('INT-03', 'countries',
                 'NOT EXISTS (SELECT 1 FROM curated.movie_production_countries l WHERE l.country_id = t.iso_3166_1)');
SELECT dq.remove('INT-03', 'languages',
                 'NOT EXISTS (SELECT 1 FROM curated.movie_spoken_languages l WHERE l.language_id = t.iso_639_1)');
SELECT dq.remove('INT-04', 'people',
                 'NOT EXISTS (SELECT 1 FROM curated.movie_cast c WHERE c.person_id = t.id)
                  AND NOT EXISTS (SELECT 1 FROM curated.movie_crew c WHERE c.person_id = t.id)');

-- Bilan des corrections de l'exécution
SELECT control_id, action, table_name, column_name, rows_corrected
FROM (
    SELECT control_id, action, table_name, column_name, count(*) AS rows_corrected
    FROM dq.corrections
    WHERE run_id = dq.current_run()
    GROUP BY control_id, action, table_name, column_name
) s
ORDER BY control_id, table_name, column_name;
