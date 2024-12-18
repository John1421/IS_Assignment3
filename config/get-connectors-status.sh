#!/bin/bash

CONNECTOR_NAME=$1
curl -X GET http://connect:8083/connectors/$CONNECTOR_NAME/status
