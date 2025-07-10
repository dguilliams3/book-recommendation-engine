# Book Recommendation Engine - Deployment Automation
# 
# This Makefile provides standardized deployment commands for the book recommendation system.
# It follows industry best practices for container orchestration and deployment automation.
#
# Educational Notes:
# - Makefiles provide consistent, documented deployment procedures
# - Targets should be idempotent (safe to run multiple times)
# - Dependencies ensure proper startup order
# - Error handling prevents partial deployments

.PHONY: help build up down logs clean test reader-up reader-down reader-logs production-up

# Default target - show help
help: ## Show this help message
	@echo "Book Recommendation Engine - Deployment Commands"
	@echo "================================================="
	@echo ""
	@echo "Basic Commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Environment Variables:"
	@echo "  ENABLE_READER_MODE=true/false  - Enable/disable Reader Mode services"
	@echo "  GOOGLE_BOOKS_API_KEY=<key>     - Google Books API key for enrichment"
	@echo "  OPENAI_API_KEY=<key>           - OpenAI API key for recommendations"
	@echo ""
	@echo "Examples:"
	@echo "  make up                        # Start base services"
	@echo "  make reader-up                 # Start with Reader Mode"
	@echo "  make logs service=recommendation_api  # View specific service logs"

# Build all Docker images with proper tagging
build: ## Build all Docker images with reader-mode-v1 tag
	@echo "🔨 Building Docker images..."
	@docker-compose build --parallel
	@echo "🏷️  Tagging images with reader-mode-v1..."
	@docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.ID}}" | grep book_recommendation_engine | while read repo tag id; do \
		if [ "$$tag" != "reader-mode-v1" ]; then \
			docker tag $$id $${repo}:reader-mode-v1; \
			echo "Tagged $$repo:$$tag -> $$repo:reader-mode-v1"; \
		fi; \
	done
	@echo "✅ Build complete!"

# Start base services (Student Mode only)
up: build ## Start base services (Student Mode only)
	@echo "🚀 Starting base services..."
	@docker-compose up -d
	@echo "⏳ Waiting for services to be ready..."
	@sleep 10
	@make health-check
	@echo "✅ Base services are running!"
	@echo "🌐 Access points:"
	@echo "  Streamlit UI: http://localhost:8501"
	@echo "  React UI: http://localhost:8080"
	@echo "  API: http://localhost:8000"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana: http://localhost:3000 (admin/admin)"

# Start with Reader Mode enabled
reader-up: build ## Start services with Reader Mode enabled
	@echo "📚 Starting services with Reader Mode..."
	@export ENABLE_READER_MODE=true && \
	docker-compose -f docker-compose.yml -f docker-compose.reader.yml up -d
	@echo "⏳ Waiting for Reader Mode services to be ready..."
	@sleep 15
	@make health-check
	@echo "✅ Reader Mode services are running!"
	@echo "🌐 Access points:"
	@echo "  Streamlit UI (Reader Mode): http://localhost:8501"
	@echo "  User Ingest API: http://localhost:8002"
	@echo "  Recommendation API: http://localhost:8000"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana: http://localhost:3000 (admin/admin)"

# Stop all services
down: ## Stop all services
	@echo "🛑 Stopping all services..."
	@docker-compose -f docker-compose.yml -f docker-compose.reader.yml down
	@echo "✅ All services stopped!"

# Stop Reader Mode services only
reader-down: ## Stop Reader Mode services only
	@echo "🛑 Stopping Reader Mode services..."
	@docker-compose -f docker-compose.yml -f docker-compose.reader.yml stop user_ingest_service feedback_worker
	@echo "✅ Reader Mode services stopped!"

# View logs for all services or specific service
logs: ## View logs (use service=<name> for specific service)
ifdef service
	@echo "📋 Viewing logs for $(service)..."
	@docker-compose logs -f $(service)
else
	@echo "📋 Viewing logs for all services..."
	@docker-compose -f docker-compose.yml -f docker-compose.reader.yml logs -f
endif

# View Reader Mode specific logs
reader-logs: ## View Reader Mode service logs
	@echo "📋 Viewing Reader Mode service logs..."
	@docker-compose -f docker-compose.yml -f docker-compose.reader.yml logs -f user_ingest_service feedback_worker

# Health check for all services
health-check: ## Check health of all services
	@echo "🏥 Checking service health..."
	@echo "Redis:"
	@docker-compose exec -T redis redis-cli ping || echo "❌ Redis not ready"
	@echo "PostgreSQL:"
	@docker-compose exec -T postgres pg_isready -U books || echo "❌ PostgreSQL not ready"
	@echo "Kafka:"
	@docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null && echo "✅ Kafka ready" || echo "❌ Kafka not ready"
	@echo "Recommendation API:"
	@curl -s http://localhost:8000/health > /dev/null && echo "✅ Recommendation API ready" || echo "❌ Recommendation API not ready"
	@echo "Streamlit UI:"
	@curl -s http://localhost:8501 > /dev/null && echo "✅ Streamlit UI ready" || echo "❌ Streamlit UI not ready"

# Clean up containers, images, and volumes
clean: down ## Clean up containers, images, and volumes
	@echo "🧹 Cleaning up..."
	@docker-compose -f docker-compose.yml -f docker-compose.reader.yml down -v --remove-orphans
	@docker system prune -f
	@echo "✅ Cleanup complete!"

# Run tests
test: ## Run the test suite
	@echo "🧪 Running tests..."
	@docker-compose exec -T recommendation_api python -m pytest tests/ -v
	@echo "✅ Tests complete!"

# Production deployment (with security hardening)
production-up: ## Start services in production mode
	@echo "🏭 Starting production deployment..."
	@if [ -z "$$OPENAI_API_KEY" ]; then echo "❌ OPENAI_API_KEY not set"; exit 1; fi
	@if [ -z "$$GOOGLE_BOOKS_API_KEY" ]; then echo "⚠️  GOOGLE_BOOKS_API_KEY not set (book enrichment disabled)"; fi
	@export ENABLE_READER_MODE=true && \
	export POSTGRES_PASSWORD=$$(openssl rand -base64 32) && \
	export GF_SECURITY_ADMIN_PASSWORD=$$(openssl rand -base64 16) && \
	docker-compose -f docker-compose.yml -f docker-compose.reader.yml up -d
	@echo "⏳ Waiting for production services..."
	@sleep 20
	@make health-check
	@echo "✅ Production deployment complete!"
	@echo "🔐 Security: Passwords have been randomized"
	@echo "📊 Monitor at: http://localhost:3000"

# Development helpers
dev-setup: ## Set up development environment
	@echo "🛠️  Setting up development environment..."
	@cp .env.template .env
	@echo "ENABLE_READER_MODE=true" >> .env
	@echo "✅ Development environment ready!"
	@echo "📝 Edit .env file to configure API keys"

# Database operations
db-reset: ## Reset database (WARNING: destroys all data)
	@echo "⚠️  This will destroy all database data!"
	@read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	@docker-compose down
	@docker volume rm book_recommendation_engine_pgdata || true
	@echo "✅ Database reset complete!"

# Backup database
db-backup: ## Backup database to ./backups/
	@echo "💾 Creating database backup..."
	@mkdir -p backups
	@docker-compose exec -T postgres pg_dump -U books books > backups/books_$(shell date +%Y%m%d_%H%M%S).sql
	@echo "✅ Database backup created in ./backups/"

# Show service status
status: ## Show status of all services
	@echo "📊 Service Status:"
	@docker-compose -f docker-compose.yml -f docker-compose.reader.yml ps

# Educational target - explain the architecture
explain: ## Explain the system architecture
	@echo "🎓 Book Recommendation Engine Architecture"
	@echo "========================================"
	@echo ""
	@echo "Infrastructure Services:"
	@echo "  • PostgreSQL (port 5432) - Main database with pgvector"
	@echo "  • Redis (port 6379) - Caching and session storage"
	@echo "  • Kafka (port 9092) - Event streaming"
	@echo "  • Zookeeper (port 2181) - Kafka coordination"
	@echo ""
	@echo "Core Application Services:"
	@echo "  • recommendation_api (port 8000) - FastAPI backend"
	@echo "  • streamlit_ui (port 8501) - Streamlit frontend"
	@echo "  • frontend (port 8080) - React frontend"
	@echo "  • ingestion_service - CSV data processing"
	@echo ""
	@echo "Reader Mode Services:"
	@echo "  • user_ingest_service (port 8002) - User book uploads"
	@echo "  • feedback_worker - Feedback processing"
	@echo ""
	@echo "Background Workers:"
	@echo "  • book_enrichment_worker - Metadata enrichment"
	@echo "  • graph_refresher - Similarity computation"
	@echo "  • Various embedding workers - Vector processing"
	@echo ""
	@echo "Monitoring:"
	@echo "  • Prometheus (port 9090) - Metrics collection"
	@echo "  • Grafana (port 3000) - Dashboards"
	@echo ""
	@echo "Data Flow:"
	@echo "  1. Users upload books via Streamlit UI"
	@echo "  2. user_ingest_service processes uploads"
	@echo "  3. Events flow through Kafka"
	@echo "  4. Workers enrich and process data"
	@echo "  5. recommendation_api serves personalized recommendations"
	@echo "  6. Users provide feedback via thumbs up/down"
	@echo "  7. feedback_worker updates recommendation scores" 