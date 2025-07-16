Write-Host "Stopping all Docker Compose services..."
docker-compose down

Write-Host "Removing all Docker volumes for a clean DB..."
$volumes = docker volume ls --format "{{.Name}}" | Select-String "book_recommendation_engine"
foreach ($v in $volumes) { docker volume rm $v }

Write-Host "Rebuilding all images (no cache)..."
docker-compose build --no-cache

Write-Host "Starting up all services..."
docker-compose up -d

Write-Host "Done. Your database and services have been reset."