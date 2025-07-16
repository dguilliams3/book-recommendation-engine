# Book Recommendation Engine - PowerShell Deployment Script
param(
    [Parameter(Mandatory=$true)]
    [string]$Command,
    [string]$Service = ""
)

switch ($Command.ToLower()) {
    "help" { 
        Write-Host "Book Recommendation Engine - Deployment Commands" -ForegroundColor Cyan
        Write-Host "=================================================" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Available Commands:" -ForegroundColor White
        Write-Host "  help        - Show this help message"
        Write-Host "  build       - Build all Docker images"
        Write-Host "  up          - Start base services"
        Write-Host "  reader-up   - Start services with Reader Mode"
        Write-Host "  down        - Stop all services"
        Write-Host "  status      - Show service status"
        Write-Host "  explain     - Explain system architecture"
        Write-Host ""
        Write-Host "Examples:" -ForegroundColor Green
        Write-Host "  .\deploy.ps1 up"
        Write-Host "  .\deploy.ps1 reader-up"
        Write-Host "  .\deploy.ps1 status"
    }
    
    "build" { 
        Write-Host "Building Docker images..." -ForegroundColor Blue
        docker-compose build --parallel
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Build complete!" -ForegroundColor Green
        } else {
            Write-Host "Build failed!" -ForegroundColor Red
        }
    }
    
    "up" { 
        Write-Host "Starting base services..." -ForegroundColor Blue
        docker-compose up -d
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Base services running!" -ForegroundColor Green
            Write-Host "Streamlit UI: http://localhost:8501" -ForegroundColor Cyan
            Write-Host "API: http://localhost:8000" -ForegroundColor Cyan
        }
    }
    
    "reader-up" { 
        Write-Host "Starting Reader Mode services..." -ForegroundColor Blue
        $env:ENABLE_READER_MODE = "true"
        docker-compose -f docker-compose.yml -f docker-compose.reader.yml up -d
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Reader Mode services running!" -ForegroundColor Green
            Write-Host "Streamlit UI: http://localhost:8501" -ForegroundColor Cyan
            Write-Host "User Ingest API: http://localhost:8002" -ForegroundColor Cyan
        }
    }
    
    "down" { 
        Write-Host "Stopping all services..." -ForegroundColor Blue
        docker-compose -f docker-compose.yml -f docker-compose.reader.yml down
        Write-Host "All services stopped!" -ForegroundColor Green
    }
    
    "status" { 
        Write-Host "Service Status:" -ForegroundColor Blue
        docker-compose -f docker-compose.yml -f docker-compose.reader.yml ps
    }
    
    "explain" { 
        Write-Host "Book Recommendation Engine Architecture" -ForegroundColor Cyan
        Write-Host "=======================================" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Infrastructure Services:" -ForegroundColor Yellow
        Write-Host "  • PostgreSQL (port 5432) - Main database"
        Write-Host "  • Redis (port 6379) - Caching"
        Write-Host "  • Kafka (port 9092) - Event streaming"
        Write-Host ""
        Write-Host "Reader Mode Services:" -ForegroundColor Green
        Write-Host "  • user_ingest_service (port 8002) - User book uploads"
        Write-Host "  • feedback_worker - Feedback processing"
        Write-Host ""
        Write-Host "Core Services:" -ForegroundColor Yellow
        Write-Host "  • recommendation_api (port 8000) - FastAPI backend"
        Write-Host "  • streamlit_ui (port 8501) - Streamlit frontend"
    }
    
    default { 
        Write-Host "Unknown command: $Command" -ForegroundColor Red
        Write-Host "Run '.\deploy.ps1 help' for available commands" -ForegroundColor Yellow
    }
} 