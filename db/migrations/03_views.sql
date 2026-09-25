-- =====================================================================
-- Vues d'exploitation : elles servent à la fois au dashboard Metabase
-- et à la supervision (postgres-exporter les transforme en métriques).
-- CREATE OR REPLACE : rejouables à chaque démarrage.
-- =====================================================================

-- Fiche film « prête à l'emploi » : les deux notations côte à côte.
CREATE OR REPLACE VIEW mart.v_movies_enriched AS
SELECT
    m.id,
    m.title,
    m.original_title,
    m.release_date,
    EXTRACT(YEAR FROM m.release_date)::int AS release_year,
    m.runtime,
    m.budget,
    m.revenue,
    m.popularity,
    m.vote_average         AS tmdb_rating,
    m.vote_count           AS tmdb_votes,
    m.imdb_average_rating  AS imdb_rating,
    m.imdb_num_votes       AS imdb_votes,
    m.rating_gap,
    m.votes_ratio,
    m.has_imdb_match,
    m.imdb_id,
    c.name                 AS collection,
    (SELECT string_agg(g.name, ', ' ORDER BY g.name)
       FROM mart.movie_genres mg
       JOIN mart.genres g ON g.id = mg.genre_id
      WHERE mg.movie_id = m.id)                       AS genres,
    (SELECT string_agg(DISTINCT p.name, ', ')
       FROM mart.movie_crew mc
       JOIN mart.people p ON p.id = mc.person_id
      WHERE mc.movie_id = m.id AND mc.job = 'Director') AS directors,
    m.ingest_date,
    m.loaded_at
FROM mart.movies m
LEFT JOIN mart.collections c ON c.id = m.collection_id;

-- Plus grands désaccords entre les deux sources, sur des films suffisamment notés.
CREATE OR REPLACE VIEW mart.v_rating_gap_top AS
SELECT
    id, title, release_year, genres,
    tmdb_rating, tmdb_votes, imdb_rating, imdb_votes, rating_gap
FROM mart.v_movies_enriched
WHERE has_imdb_match AND tmdb_votes >= 50 AND imdb_votes >= 100
ORDER BY abs(rating_gap) DESC;

-- Comparaison des deux notations par genre.
CREATE OR REPLACE VIEW mart.v_genre_stats AS
SELECT
    g.name                                              AS genre,
    count(*)                                            AS movies,
    round(avg(m.vote_average), 2)                       AS avg_tmdb_rating,
    round(avg(m.imdb_average_rating), 2)                AS avg_imdb_rating,
    round(avg(m.rating_gap), 2)                         AS avg_rating_gap,
    count(*) FILTER (WHERE m.has_imdb_match)            AS with_imdb
FROM mart.movies m
JOIN mart.movie_genres mg ON mg.movie_id = m.id
JOIN mart.genres g        ON g.id = mg.genre_id
GROUP BY g.name;

-- Volumétrie et notation par année de sortie.
CREATE OR REPLACE VIEW mart.v_yearly AS
SELECT
    EXTRACT(YEAR FROM release_date)::int AS release_year,
    count(*)                             AS movies,
    round(avg(vote_average), 2)          AS avg_tmdb_rating,
    round(avg(imdb_average_rating), 2)   AS avg_imdb_rating,
    sum(revenue)                         AS total_revenue,
    sum(budget)                          AS total_budget
FROM mart.movies
WHERE release_date IS NOT NULL
GROUP BY 1;

-- Personnes les plus présentes au casting (exploite le miroir complet du modèle).
CREATE OR REPLACE VIEW mart.v_top_people AS
SELECT
    p.id,
    p.name,
    p.known_for_department,
    count(DISTINCT mc.movie_id)                   AS movies,
    round(avg(m.vote_average), 2)                 AS avg_tmdb_rating,
    round(avg(m.imdb_average_rating), 2)          AS avg_imdb_rating
FROM mart.movie_cast mc
JOIN mart.people p ON p.id = mc.person_id
JOIN mart.movies m ON m.id = mc.movie_id
GROUP BY p.id, p.name, p.known_for_department;

-- ---------------------------------------------------------------------
-- Supervision
-- ---------------------------------------------------------------------

-- Volume de données propres, table par table : source de pipeline_clean_rows.
CREATE OR REPLACE VIEW mart.v_clean_counts AS
SELECT 'movies'                     AS table_name, count(*) AS row_count FROM mart.movies
UNION ALL SELECT 'genres',                 count(*) FROM mart.genres
UNION ALL SELECT 'collections',            count(*) FROM mart.collections
UNION ALL SELECT 'production_companies',   count(*) FROM mart.production_companies
UNION ALL SELECT 'countries',              count(*) FROM mart.countries
UNION ALL SELECT 'languages',              count(*) FROM mart.languages
UNION ALL SELECT 'people',                 count(*) FROM mart.people
UNION ALL SELECT 'movie_genres',           count(*) FROM mart.movie_genres
UNION ALL SELECT 'movie_cast',             count(*) FROM mart.movie_cast
UNION ALL SELECT 'movie_crew',             count(*) FROM mart.movie_crew
UNION ALL SELECT 'movies_with_imdb',       count(*) FROM mart.movies WHERE has_imdb_match;

-- Dernière exécution de chaque job : brut lu vs propre écrit.
CREATE OR REPLACE VIEW mart.v_raw_vs_clean AS
SELECT DISTINCT ON (job)
    job,
    ingest_date,
    raw_rows_in,
    distinct_movies_in,
    clean_rows_out,
    rejected_rows,
    imdb_matched,
    status,
    started_at,
    finished_at,
    duration_seconds,
    EXTRACT(EPOCH FROM (now() - finished_at)) AS seconds_since_finished,
    CASE WHEN raw_rows_in > 0
         THEN round(clean_rows_out::numeric / raw_rows_in, 4)
    END AS clean_ratio
FROM mart.load_runs
ORDER BY job, started_at DESC;
