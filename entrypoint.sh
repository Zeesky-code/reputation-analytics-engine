#!/bin/bash
set -e

# Generate data if database doesn't exist
if [ ! -f "data/analytics.duckdb" ]; then
    echo "Database not found. Generating synthetic data..."
    python src/data_gen.py
else
    echo "Database exists. Skipping generation."
fi

# Start the server
# Use shell variable for port or default to 8000 (Render uses PORT env var)
PORT="${PORT:-8000}"
echo "Starting server on port $PORT..."
uvicorn src.main:app --host 0.0.0.0 --port "$PORT"
