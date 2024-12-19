#!/bin/bash

# if there is an argument just delete that topic
if [ $# -gt 0 ]; then
  TOPIC=$1
  echo "Deleting topic: $TOPIC"
  kafka-topics.sh --delete --topic "$TOPIC" --bootstrap-server broker1:9092
  exit 0
fi

# Obtém a lista de todos os tópicos Kafka
TOPICS=$(kafka-topics.sh --bootstrap-server broker1:9092 --list)

# Verifica se a lista de tópicos está vazia
if [ -z "$TOPICS" ]; then
  echo "No topics found."
  exit 0
fi

# Elimina cada tópico
for TOPIC in $TOPICS; do
  echo "Deleting topic: $TOPIC"
  kafka-topics.sh --delete --topic "$TOPIC" --bootstrap-server broker1:9092
done

echo "All topics deleted."
