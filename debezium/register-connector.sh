#!/bin/sh
set -e

echo "Waiting for Kafka Connect to be ready..."
until curl -s -o /dev/null -w "%{http_code}" http://kafka-connect:8083/connectors | grep -q "200"; do
  sleep 3
done

echo "Kafka Connect is up. Registering Debezium connector..."
http_code=$(curl -s -o /tmp/resp.json -w "%{http_code}" -X POST \
  -H "Accept:application/json" -H "Content-Type:application/json" \
  http://kafka-connect:8083/connectors/ -d @/debezium/register-connector.json)

cat /tmp/resp.json
echo ""

if [ "$http_code" = "201" ] || [ "$http_code" = "409" ]; then
  echo "Connector registered (or already exists). HTTP $http_code"
  exit 0
else
  echo "Failed to register connector. HTTP $http_code"
  exit 1
fi
