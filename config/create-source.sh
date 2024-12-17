#!/bin/bash

# Check if the filename is provided as a command-line argument
if [ -z "$1" ]; then
  echo "Usage: $0 <filename>"
  exit 1
fi

# Use the provided filename
FILENAME=$1

# Make the POST request with the provided filename
curl -X POST -H "Content-Type: application/json" --data @config/"$FILENAME"-source.json http://connect:8083/connectors
