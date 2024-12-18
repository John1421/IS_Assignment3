#!/bin/bash

# Verifica se o nome do ficheiro foi fornecido como argumento de linha de comando
if [ -z "$1" ]; then
  # Se não houver argumento, faz POST para cada ficheiro que termina com -sink.json
  for file in config/*-sink.json; do
    echo "-----------------------------------------"  # Nova linha para separação
    echo "POST for $file"
    curl -X POST -H "Content-Type: application/json" --data @"$file" http://connect:8083/connectors
    echo ""  # Nova linha para separação
  done
  echo "-----------------------------------------"
else
  # Usa o nome do ficheiro fornecido
  FILENAME=$1
  echo "-----------------------------------------"  # Nova linha para separação
  echo "POST for config/$FILENAME-sink.json"
  curl -X POST -H "Content-Type: application/json" --data @config/"$FILENAME"-sink.json http://connect:8083/connectors
  echo ""  # Nova linha para separação
  echo "-----------------------------------------"
fi
