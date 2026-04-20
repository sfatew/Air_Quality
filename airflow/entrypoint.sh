#!/bin/bash
set -e

export PATH="/home/airflow/.local/bin:$PATH"

# Initialize database
airflow db migrate

# Create admin user if it doesn't exist
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com 2>/dev/null || true

# Register DAGs immediately instead of waiting for scheduler scan
airflow dags reserialize 2>/dev/null || true

exec "$@"