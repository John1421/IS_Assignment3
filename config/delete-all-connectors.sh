#!/bin/bash

for connector in $(curl -s -X GET http://host.docker.internal:8083/connectors | tr -d '[]" ' | tr ',' '\n'); do
  curl -X DELETE http://connect:8083/connectors/$connector
done
