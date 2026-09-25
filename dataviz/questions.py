"""Questions et dashboard Metabase du projet, définis en SQL sur les vues du mart.

Les garder ici, en Python, plutôt que dans l'interface : le dépôt reste la
référence, et le dashboard se reconstruit à l'identique sur n'importe quelle
machine.
"""

# (nom, description, type de visualisation, SQL, réglages d'affichage, position)
QUESTIONS: list[dict] = [
    {
        "name": "Films chargés dans le mart",
        "description": "Nombre de films propres présents dans PostgreSQL.",
        "display": "scalar",
        "sql": "SELECT count(*) AS films FROM mart.movies",
        "visualization_settings": {},
        "layout": {"row": 0, "col": 0, "size_x": 6, "size_y": 4},
    },
    {
        "name": "Couverture IMDb",
        "description": "Part des films TMDB pour lesquels une note IMDb a été trouvée.",
        "display": "pie",
        "sql": (
            "SELECT CASE WHEN has_imdb_match THEN 'Rapproché IMDb'\n"
            "            ELSE 'Sans correspondance' END AS statut,\n"
            "       count(*) AS films\n"
            "FROM mart.movies\n"
            "GROUP BY 1"
        ),
        "visualization_settings": {
            "pie.dimension": "statut",
            "pie.metric": "films",
        },
        "layout": {"row": 0, "col": 6, "size_x": 8, "size_y": 8},
    },
    {
        "name": "Volume traité par le pipeline",
        "description": "Brut lu, films distincts et lignes propres écrites à la dernière exécution.",
        "display": "table",
        "sql": (
            "SELECT job, status, raw_rows_in, distinct_movies_in,\n"
            "       clean_rows_out, rejected_rows, imdb_matched, finished_at\n"
            "FROM mart.v_raw_vs_clean\n"
            "ORDER BY job"
        ),
        "visualization_settings": {},
        "layout": {"row": 0, "col": 14, "size_x": 10, "size_y": 8},
    },
    {
        "name": "Les deux notations côte à côte",
        "description": "Films où TMDB et IMDb divergent le plus, parmi ceux suffisamment notés.",
        "display": "table",
        "sql": (
            "SELECT title AS titre, release_year AS annee, genres,\n"
            "       tmdb_rating AS note_tmdb, imdb_rating AS note_imdb,\n"
            "       rating_gap AS ecart, tmdb_votes AS votes_tmdb, imdb_votes AS votes_imdb\n"
            "FROM mart.v_rating_gap_top\n"
            "LIMIT 20"
        ),
        "visualization_settings": {},
        "layout": {"row": 4, "col": 0, "size_x": 14, "size_y": 8},
    },
    {
        "name": "Note moyenne par genre",
        "description": "Comparaison des deux notations, genre par genre.",
        "display": "bar",
        "sql": (
            "SELECT genre, avg_tmdb_rating AS note_tmdb, avg_imdb_rating AS note_imdb\n"
            "FROM mart.v_genre_stats\n"
            "ORDER BY movies DESC\n"
            "LIMIT 12"
        ),
        "visualization_settings": {
            "graph.dimensions": ["genre"],
            "graph.metrics": ["note_tmdb", "note_imdb"],
        },
        "layout": {"row": 12, "col": 0, "size_x": 12, "size_y": 8},
    },
    {
        "name": "Personnes les plus présentes au casting",
        "description": "Exploite les tables people et movie_cast du miroir complet du modèle.",
        "display": "bar",
        "sql": (
            "SELECT name AS personne, movies AS films\n"
            "FROM mart.v_top_people\n"
            "ORDER BY movies DESC, personne\n"
            "LIMIT 15"
        ),
        "visualization_settings": {
            "graph.dimensions": ["personne"],
            "graph.metrics": ["films"],
        },
        "layout": {"row": 12, "col": 12, "size_x": 12, "size_y": 8},
    },
]

DASHBOARD_NAME = "Pipeline TMDB × IMDb"
DASHBOARD_DESCRIPTION = (
    "Données produites par le pipeline TP2 : catalogue TMDB nettoyé, enrichi des "
    "notes IMDb, et suivi du volume traité."
)
DATABASE_NAME = "TMDB — schéma mart"
