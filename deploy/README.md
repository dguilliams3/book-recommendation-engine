# Production Deployment Guide

This guide shows how to deploy the Book Recommendation Engine to AWS EC2 for live demo purposes.

## 🚀 Quick EC2 Deployment

### Prerequisites
- AWS EC2 instance (t3.large or larger recommended)
- Docker and Docker Compose installed
- OpenAI API key

### Step 1: Launch EC2 Instance

```bash
# Choose Ubuntu 22.04 LTS
# Instance type: t3.large (2 vCPU, 8GB RAM minimum)
# Security group: Allow ports 22, 80, 8501, 8000
# Storage: 20GB+ SSD
```

### Step 2: Install Dependencies

```bash
# Connect to your EC2 instance
ssh -i your-key.pem ubuntu@your-ec2-ip

# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker ubuntu

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Logout and login again for docker group to take effect
exit
```

### Step 3: Deploy Application

```bash
# Clone your repository
git clone <your-repo-url>
cd book_recommendation_engine

# Configure production environment
cp deploy/.env.prod.template deploy/.env.prod
nano deploy/.env.prod  # Add your OpenAI API key

# Make deployment script executable
chmod +x deploy/deploy.sh

# Deploy!
./deploy/deploy.sh
```

### Step 4: Verify Deployment

After deployment completes, you should see:

```
🎉 Deployment complete!

📊 Streamlit UI: http://YOUR-EC2-IP:8501
📖 API Docs: http://YOUR-EC2-IP:8000/docs
```

Test the endpoints:
- **Main App**: http://YOUR-EC2-IP:8501
- **API Documentation**: http://YOUR-EC2-IP:8000/docs
- **Health Check**: http://YOUR-EC2-IP:8000/health

## 🔧 Production Configuration

### Environment Variables

The most critical setting is your OpenAI API key in `deploy/.env.prod`:

```bash
OPENAI_API_KEY=sk-your_actual_key_here
```

### Security Groups

Ensure your EC2 security group allows:
- **Port 22**: SSH access
- **Port 80**: HTTP access (if using NGINX)
- **Port 8501**: Streamlit UI
- **Port 8000**: FastAPI documentation

### Resource Requirements

| Component | CPU | RAM | Notes |
|-----------|-----|-----|-------|
| **Minimum** | 2 vCPU | 8GB | For demo purposes |
| **Recommended** | 4 vCPU | 16GB | Better performance |
| **Production** | 8+ vCPU | 32GB+ | For scale |

## 📊 Monitoring

### Check Service Status
```bash
# View all services
docker-compose -f deploy/docker-compose.prod.yml ps

# View logs
docker-compose -f deploy/docker-compose.prod.yml logs -f

# Check specific service
docker-compose -f deploy/docker-compose.prod.yml logs streamlit_ui
```

### Health Checks
```bash
# API health
curl http://localhost:8000/health

# Streamlit health
curl http://localhost:8501/_stcore/health

# Database connection
docker-compose -f deploy/docker-compose.prod.yml exec postgres pg_isready -U books
```

## 🛠️ Troubleshooting

### Common Issues

**Services not starting:**
```bash
# Check Docker logs
docker-compose -f deploy/docker-compose.prod.yml logs

# Restart specific service
docker-compose -f deploy/docker-compose.prod.yml restart recommendation_api
```

**Out of memory:**
```bash
# Check memory usage
docker stats

# Increase EC2 instance size or add swap
sudo fallocate -l 4G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

**OpenAI API errors:**
- Verify API key is correct in `.env.prod`
- Check API quota and billing
- Monitor API usage in OpenAI dashboard

### Useful Commands

```bash
# Stop all services
docker-compose -f deploy/docker-compose.prod.yml down

# Rebuild and restart
docker-compose -f deploy/docker-compose.prod.yml up --build -d

# View resource usage
docker stats

# Clean up old images
docker system prune -f
```

## 🌐 Custom Domain (Optional)

To use a custom domain like `books-demo.danguilliams.com`:

1. **Add DNS A record** pointing to your EC2 IP
2. **Update NGINX config** with your domain name
3. **Add SSL certificate** (Let's Encrypt recommended)

```bash
# Install certbot
sudo apt install certbot python3-certbot-nginx

# Get SSL certificate
sudo certbot --nginx -d books-demo.danguilliams.com
```

## 🎯 Demo Performance Tips

For the best demo experience:
- Use `t3.large` or larger EC2 instance
- Pre-warm the system by running a few test recommendations
- Have the Streamlit UI loaded before your presentation
- Keep the API docs open in another tab to show the OpenAPI integration

---

*This deployment showcases production-ready distributed systems deployment with Docker containerization and AWS cloud infrastructure.* 