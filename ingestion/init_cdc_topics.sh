#!/usr/bin/env bash
set -euo pipefail

KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh
for topic in connect-configs connect-offsets connect-status; do
  "$KAFKA_TOPICS" --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1 --config cleanup.policy=compact
done

"$KAFKA_TOPICS" --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic __debezium-heartbeat.marketplace --partitions 1 --replication-factor 1

for table in customer_shipping order_items orders product product_reviews review shipping_details; do
  "$KAFKA_TOPICS" --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "marketplace.public.$table" --partitions 3 --replication-factor 1
done
