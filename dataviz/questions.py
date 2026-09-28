"""Questions et dashboards Metabase du projet, définis en SQL.

Les garder ici, en Python, plutôt que dans l'interface : le dépôt reste la
référence, et les dashboards se reconstruisent à l'identique sur n'importe
quelle machine.

Deux dashboards : l'exploitation des données du pipeline (TP2, vues du
schéma mart) et le contrôle qualité (TP3, vues du schéma dq).
"""

# (nom, description, type de visualisation, SQL, réglages d'affichage, position)
PIPELINE_QUESTIONS: list[dict] = [
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
        "description": (
            "Brut lu, films distincts et lignes propres écrites à la dernière exécution."
        ),
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

# Contrôle qualité (TP3). Les vues dq.* décrivent la dernière exécution de
# l'étape quality du pipeline : elles existent dès son premier passage, qui
# suit le premier chargement.
QUALITY_QUESTIONS: list[dict] = [
    {
        "name": "Anomalies détectées dans le mart",
        "description": "Total des anomalies relevées par la matrice sur les données du TP2.",
        "display": "scalar",
        "sql": "SELECT anomalies_before AS anomalies FROM dq.v_last_run",
        "visualization_settings": {},
        "layout": {"row": 0, "col": 0, "size_x": 6, "size_y": 4},
    },
    {
        "name": "Anomalies résiduelles après nettoyage",
        "description": (
            "Anomalies conservées volontairement (donnée absente à la source, "
            "rien de fiable à imputer) : chacune est justifiée dans la matrice."
        ),
        "display": "scalar",
        "sql": "SELECT anomalies_after AS anomalies FROM dq.v_last_run",
        "visualization_settings": {},
        "layout": {"row": 0, "col": 6, "size_x": 6, "size_y": 4},
    },
    {
        "name": "Contrôles à traiter",
        "description": "Contrôles encore en anomalie sans justification : doit rester à 0.",
        "display": "scalar",
        "sql": "SELECT count(*) AS controles FROM dq.v_results_last WHERE verdict = 'à traiter'",
        "visualization_settings": {},
        "layout": {"row": 0, "col": 12, "size_x": 6, "size_y": 4},
    },
    {
        "name": "Décisions de la matrice",
        "description": "Traitement retenu pour chaque contrôle de la matrice.",
        "display": "pie",
        "sql": (
            "SELECT treatment AS traitement, count(*) AS controles\n"
            "FROM dq.controls\n"
            "GROUP BY treatment"
        ),
        "visualization_settings": {
            "pie.dimension": "traitement",
            "pie.metric": "controles",
        },
        "layout": {"row": 0, "col": 18, "size_x": 6, "size_y": 8},
    },
    {
        "name": "Score de conformité par dimension",
        "description": (
            "Moyenne des taux de conformité des contrôles de chaque dimension "
            "(100 = aucune anomalie), avant et après nettoyage."
        ),
        "display": "bar",
        "sql": (
            "SELECT a.dimension, a.score AS avant, p.score AS apres\n"
            "FROM dq.v_dimension_scores a\n"
            "JOIN dq.v_dimension_scores p ON p.dimension = a.dimension AND p.phase = 'après'\n"
            "WHERE a.phase = 'avant'\n"
            "ORDER BY a.dimension"
        ),
        "visualization_settings": {
            "graph.dimensions": ["dimension"],
            "graph.metrics": ["avant", "apres"],
        },
        "layout": {"row": 4, "col": 0, "size_x": 18, "size_y": 8},
    },
    {
        "name": "Contrôles en anomalie, par priorité",
        "description": "Priorité = poids de la sévérité x taux d'anomalies avant nettoyage.",
        "display": "table",
        "sql": (
            "SELECT control_id AS controle, dimension, rule AS regle, severity AS severite,\n"
            "       anomalies_before AS avant, pct_before AS pct_avant,\n"
            "       anomalies_after AS apres, treatment AS traitement, verdict\n"
            "FROM dq.v_results_last\n"
            "WHERE anomalies_before > 0 OR anomalies_after > 0\n"
            "ORDER BY priority DESC, control_id"
        ),
        "visualization_settings": {},
        "layout": {"row": 12, "col": 0, "size_x": 24, "size_y": 10},
    },
    {
        "name": "Note TMDB moyenne par genre, avant et après",
        "description": (
            "Les notes 0 des films sans vote tiraient les moyennes vers le bas : "
            "effet du nettoyage sur l'indicateur du dashboard pipeline."
        ),
        "display": "bar",
        "sql": (
            "SELECT genre, note_tmdb_mart AS mart, note_tmdb_curated AS nettoye\n"
            "FROM dq.v_genre_impact\n"
            "ORDER BY films_mart DESC\n"
            "LIMIT 12"
        ),
        "visualization_settings": {
            "graph.dimensions": ["genre"],
            "graph.metrics": ["mart", "nettoye"],
        },
        "layout": {"row": 22, "col": 0, "size_x": 12, "size_y": 8},
    },
    {
        "name": "Effet du nettoyage sur les indicateurs",
        "description": "Indicateurs du catalogue calculés sur le mart et sur les données nettoyées.",
        "display": "table",
        "sql": "SELECT indicator AS indicateur, mart, curated AS nettoye, delta FROM dq.v_kpi_impact",
        "visualization_settings": {},
        "layout": {"row": 22, "col": 12, "size_x": 12, "size_y": 8},
    },
    {
        "name": "Anomalies du mart au fil des exécutions",
        "description": (
            "Anomalies relevées sur le mart à chaque passage de l'étape qualité : "
            "la baisse montre l'effet des corrections remontées dans le pipeline."
        ),
        "display": "line",
        "sql": (
            "SELECT started_at AS execution, dimension, anomalies\n"
            "FROM dq.v_history\n"
            "WHERE phase = 'avant'\n"
            "ORDER BY started_at"
        ),
        "visualization_settings": {
            "graph.dimensions": ["execution", "dimension"],
            "graph.metrics": ["anomalies"],
        },
        "layout": {"row": 30, "col": 0, "size_x": 14, "size_y": 8},
    },
    {
        "name": "Corrections appliquées",
        "description": "Journal agrégé de la dernière exécution (dq.corrections).",
        "display": "table",
        "sql": (
            "SELECT control_id AS controle, action, table_name AS table_cible,\n"
            "       column_name AS colonne, rows_corrected AS lignes\n"
            "FROM dq.v_corrections_summary\n"
            "ORDER BY rows_corrected DESC"
        ),
        "visualization_settings": {},
        "layout": {"row": 30, "col": 14, "size_x": 10, "size_y": 8},
    },
]

DASHBOARDS: list[dict] = [
    {
        "name": "Pipeline TMDB × IMDb",
        "description": (
            "Données produites par le pipeline TP2 : catalogue TMDB nettoyé, enrichi des "
            "notes IMDb, et suivi du volume traité."
        ),
        "questions": PIPELINE_QUESTIONS,
    },
    {
        "name": "Qualité des données",
        "description": (
            "Contrôle qualité TP3 : anomalies du mart, décisions de nettoyage et "
            "comparaison avant / après, à la dernière exécution de l'étape qualité."
        ),
        "questions": QUALITY_QUESTIONS,
    },
]

DATABASE_NAME = "TMDB — schéma mart"
