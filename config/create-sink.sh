#!/bin/bash

curl -X POST -H "Content-Type: application/json" --data @config/sink.json http://connect:8083/connectors
