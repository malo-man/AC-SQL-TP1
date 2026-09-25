#!/bin/sh
# Création des topics, rejouée à chaque "docker compose up" : --if-not-exists la rend idempotente.
# L'auto-création de topics est désactivée sur le broker, un topic mal orthographié
# provoque donc une erreur explicite du producer au lieu d'un topic fantôme.
set -e

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
TOPIC="${KAFKA_TOPIC:-tmdb.movies.raw}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-1}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-604800000}"

echo "Topic ${TOPIC} sur ${BOOTSTRAP} : création si absent"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor 1 \
    --config "retention.ms=${RETENTION_MS}" \
    --config cleanup.policy=delete

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
