#!/bin/bash
curl -H "Accept:application/json" -H "Content-Type:application/json" -X POST http://connect:8083/connectors -d @config/sink.json

# curl -X GET http://connect:8083/connectors/jdbc-sink-suppliers/status