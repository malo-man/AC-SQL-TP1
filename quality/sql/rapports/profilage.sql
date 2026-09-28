-- =====================================================================
-- TP3 — Profilage du mart (lecture seule).
--
-- Vue d'ensemble des données avant tout jugement : remplissage de chaque
-- colonne, distributions, valeurs extrêmes. C'est à partir de ces mesures
-- que les règles et les seuils de la matrice ont été calibrés. Script psql,
-- exécuté par quality/run.sh.
-- =====================================================================

\echo '=== 1. Taux de remplissage des colonnes de mart.movies'
-- to_jsonb parcourt toutes les colonnes sans les nommer : une colonne
-- ajoutée au modèle apparaît ici d'elle-même.
SELECT j.key AS colonne,
       count(*) FILTER (WHERE j.value <> 'null'::jsonb)                                     AS renseignees,
       count(*) FILTER (WHERE j.value = '""'::jsonb)                                        AS chaines_vides,
       round(100.0 * count(*) FILTER (WHERE j.value NOT IN ('null'::jsonb, '""'::jsonb)) / count(*), 1) AS pct_utile
FROM mart.movies m, jsonb_each(to_jsonb(m)) AS j
GROUP BY j.key
ORDER BY pct_utile, j.key;

\echo '=== 2. Taux de remplissage des colonnes de mart.people'
SELECT j.key AS colonne,
       count(*) FILTER (WHERE j.value <> 'null'::jsonb)                                     AS renseignees,
       count(*) FILTER (WHERE j.value = '""'::jsonb)                                        AS chaines_vides,
       round(100.0 * count(*) FILTER (WHERE j.value NOT IN ('null'::jsonb, '""'::jsonb)) / count(*), 1) AS pct_utile
FROM mart.people p, jsonb_each(to_jsonb(p)) AS j
GROUP BY j.key
ORDER BY pct_utile, j.key;

\echo '=== 3. Statut de production'
SELECT coalesce(status, '(NULL)') AS status, count(*) AS films,
       count(*) FILTER (WHERE release_date > current_date) AS date_future,
       count(*) FILTER (WHERE coalesce(vote_count, 0) = 0)  AS sans_vote
FROM mart.movies GROUP BY 1 ORDER BY 2 DESC;

\echo '=== 4. Distributions numériques des films'
SELECT 'runtime (min)' AS mesure, count(runtime) AS n, min(runtime) AS min,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY runtime) AS mediane, max(runtime) AS max
FROM mart.movies
UNION ALL
SELECT 'vote_average', count(vote_average), min(vote_average),
       percentile_disc(0.5) WITHIN GROUP (ORDER BY vote_average), max(vote_average)
FROM mart.movies
UNION ALL
SELECT 'vote_count', count(vote_count), min(vote_count),
       percentile_disc(0.5) WITHIN GROUP (ORDER BY vote_count), max(vote_count)
FROM mart.movies
UNION ALL
SELECT 'imdb_average_rating', count(imdb_average_rating), min(imdb_average_rating),
       percentile_disc(0.5) WITHIN GROUP (ORDER BY imdb_average_rating), max(imdb_average_rating)
FROM mart.movies
UNION ALL
SELECT 'budget (USD)', count(budget), min(budget),
       percentile_disc(0.5) WITHIN GROUP (ORDER BY budget), max(budget)
FROM mart.movies
UNION ALL
SELECT 'revenue (USD)', count(revenue), min(revenue),
       percentile_disc(0.5) WITHIN GROUP (ORDER BY revenue), max(revenue)
FROM mart.movies
UNION ALL
SELECT 'release_date (année)', count(release_date), min(extract(year FROM release_date)),
       percentile_disc(0.5) WITHIN GROUP (ORDER BY extract(year FROM release_date)),
       max(extract(year FROM release_date))
FROM mart.movies;

\echo '=== 5. Plus petits budgets et recettes : valeurs symboliques ?'
SELECT id, title, budget, revenue, vote_count
FROM mart.movies
WHERE budget < 100000 OR revenue < 100000
ORDER BY least(coalesce(budget, revenue), coalesce(revenue, budget))
LIMIT 10;

\echo '=== 6. Notes TMDB : la valeur 0 correspond-elle à une absence de vote ?'
SELECT CASE WHEN vote_count = 0 THEN 'aucun vote'
            WHEN vote_count < 10 THEN '1 à 9 votes'
            WHEN vote_count < 100 THEN '10 à 99 votes'
            ELSE '100 votes et plus' END AS votes,
       count(*) AS films,
       count(*) FILTER (WHERE vote_average = 0) AS note_zero,
       round(avg(vote_average), 2) AS note_moyenne
FROM mart.movies
GROUP BY 1 ORDER BY min(vote_count);

\echo '=== 7. Langues originales (top 10) et codes hors référentiel'
SELECT original_language, count(*) AS films,
       original_language IN (SELECT iso_639_1 FROM mart.languages) AS dans_referentiel
FROM mart.movies GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

\echo '=== 8. Personnes : genre et département principal'
SELECT gender, CASE gender WHEN 0 THEN 'non renseigné' WHEN 1 THEN 'femme' WHEN 2 THEN 'homme'
                           WHEN 3 THEN 'non-binaire' END AS libelle,
       count(*) AS personnes, round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM mart.people GROUP BY gender ORDER BY gender;

SELECT coalesce(known_for_department, '(NULL)') AS known_for_department, count(*) AS personnes
FROM mart.people GROUP BY 1 ORDER BY 2 DESC;

\echo '=== 9. Cardinalités observées des relations (par film)'
SELECT 'genres' AS relation, min(n), round(avg(n), 1) AS moyenne, max(n)
FROM (SELECT m.id, count(l.genre_id) AS n FROM mart.movies m
        LEFT JOIN mart.movie_genres l ON l.movie_id = m.id GROUP BY m.id) s
UNION ALL
SELECT 'sociétés', min(n), round(avg(n), 1), max(n)
FROM (SELECT m.id, count(l.company_id) AS n FROM mart.movies m
        LEFT JOIN mart.movie_production_companies l ON l.movie_id = m.id GROUP BY m.id) s
UNION ALL
SELECT 'pays', min(n), round(avg(n), 1), max(n)
FROM (SELECT m.id, count(l.country_id) AS n FROM mart.movies m
        LEFT JOIN mart.movie_production_countries l ON l.movie_id = m.id GROUP BY m.id) s
UNION ALL
SELECT 'langues parlées', min(n), round(avg(n), 1), max(n)
FROM (SELECT m.id, count(l.language_id) AS n FROM mart.movies m
        LEFT JOIN mart.movie_spoken_languages l ON l.movie_id = m.id GROUP BY m.id) s
UNION ALL
SELECT 'casting', min(n), round(avg(n), 1), max(n)
FROM (SELECT m.id, count(l.credit_id) AS n FROM mart.movies m
        LEFT JOIN mart.movie_cast l ON l.movie_id = m.id GROUP BY m.id) s
UNION ALL
SELECT 'équipe technique', min(n), round(avg(n), 1), max(n)
FROM (SELECT m.id, count(l.credit_id) AS n FROM mart.movies m
        LEFT JOIN mart.movie_crew l ON l.movie_id = m.id GROUP BY m.id) s;

\echo '=== 10. Chaînes mal formées par colonne (contrôle VAL-16)'
SELECT split_part(record_key, ':', 1) AS table_name,
       split_part(observed_value, ' = ', 1) AS column_name,
       count(*) AS valeurs,
       min(split_part(observed_value, ' = ', 2)) AS exemple
FROM dq.text_hygiene('mart')
GROUP BY 1, 2 ORDER BY 3 DESC;
