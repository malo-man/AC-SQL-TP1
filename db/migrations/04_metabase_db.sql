-- Base applicative de Metabase.
--
-- Metabase stocke sa configuration (comptes, questions, dashboards) dans une
-- base à lui. Par défaut il utilise un fichier H2 dans le conteneur, qui
-- disparaît à la moindre recréation : on lui donne donc une base PostgreSQL,
-- couverte par le volume pgdata et par les sauvegardes.
--
-- PostgreSQL n'a pas de CREATE DATABASE IF NOT EXISTS : on génère l'ordre
-- puis on le fait exécuter par psql avec \gexec, seulement s'il est utile.
SELECT 'CREATE DATABASE metabase OWNER ' || quote_ident(current_user)
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'metabase')
\gexec
