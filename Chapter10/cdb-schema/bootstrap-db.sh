#!/bin/bash
set -e

if [[ $# -ne 2 ]]; then 
	echo "Usage: bootstrap-db DB_NAME DB_HOST"
	exit 1
fi

DB_NAME=$1
DB_HOST=$2

# Assemble migration steps
echo "CREATE DATABASE IF NOT EXISTS $1; USE $1;" > /tmp/migrations-all.sql
cat /migrations/*.up* >> /tmp/migrations-all.sql

# Try to apply the schema in a loop so we can wait until the CDB cluster is ready
until cat /tmp/migrations-all.sql | ./cockroach sql --insecure --echo-sql --host $DB_HOST; do 
	sleep 5; 
done
