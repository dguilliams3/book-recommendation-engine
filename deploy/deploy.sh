#!/bin/bash
set -e

echo "🚀 Deploying Book Recommendation Engine to Production"

# Check if .env.prod exists
if [ ! -f "deploy/.env.prod" ]; then
    echo "❌ Error: deploy/.env.prod not found"
    echo "Copy deploy/.env.prod.template and fill in your values"
    exit 1
fi

# Check if OpenAI API key is set
if ! grep -q "sk-" deploy/.env.prod; then
    echo "❌ Error: OpenAI API key not set in deploy/.env.prod"
    echo "Please add your OPENAI_API_KEY to deploy/.env.prod"
    exit 1
fi

# Copy production environment file
cp deploy/.env.prod .env.prod

echo "📦 Building production images..."
docker-compose -f deploy/docker-compose.prod.yml build --parallel

echo "🔄 Starting production services..."
docker-compose -f deploy/docker-compose.prod.yml up -d

echo "⏳ Waiting for services to be healthy..."
sleep 30

# Check service health
echo "🔍 Checking service health..."

# Check if PostgreSQL is ready
if docker-compose -f deploy/docker-compose.prod.yml exec postgres pg_isready -U books; then
    echo "✅ PostgreSQL is ready"
else
    echo "❌ PostgreSQL is not ready"
    exit 1
fi

# Check if Redis is ready
if docker-compose -f deploy/docker-compose.prod.yml exec redis redis-cli ping | grep -q PONG; then
    echo "✅ Redis is ready"
else
    echo "❌ Redis is not ready"
    exit 1
fi

# Check if Kafka is ready
if docker-compose -f deploy/docker-compose.prod.yml exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo "✅ Kafka is ready"
else
    echo "❌ Kafka is not ready"
    exit 1
fi

# Check if Streamlit UI is responding
sleep 10
if curl -f http://localhost:8501/_stcore/health > /dev/null 2>&1; then
    echo "✅ Streamlit UI is ready"
else
    echo "❌ Streamlit UI is not ready"
fi

# Check if API is responding
if curl -f http://localhost:8000/health > /dev/null 2>&1; then
    echo "✅ Recommendation API is ready"
else
    echo "❌ Recommendation API is not ready"
fi

echo ""
echo "🎉 Deployment complete!"
echo ""
echo "📊 Streamlit UI: http://$(curl -s http://checkip.amazonaws.com):8501"
echo "📖 API Docs: http://$(curl -s http://checkip.amazonaws.com):8000/docs"
echo ""
echo "🔧 Management commands:"
echo "  View logs: docker-compose -f deploy/docker-compose.prod.yml logs -f"
echo "  Stop services: docker-compose -f deploy/docker-compose.prod.yml down"
echo "  Restart: docker-compose -f deploy/docker-compose.prod.yml restart"
echo ""
echo "📈 Monitor with: docker-compose -f deploy/docker-compose.prod.yml ps" 