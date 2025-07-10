#!/bin/bash
set -e

echo "Stopping all Docker Compose services..."
docker-compose down

echo "Removing all Docker volumes for a clean DB..."
docker volume ls | grep book_recommendation_engine | awk '{print $2}' | xargs -r docker volume rm

echo "Rebuilding all images (no cache)..."
docker-compose build --no-cache

echo "Starting up all services..."
docker-compose up -d

echo "Done. Your database and services have been reset."