# Deployment Guide

This guide covers deploying the BSE Stock Tracker to various platforms.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Heroku Deployment](#heroku-deployment)
- [Docker Deployment](#docker-deployment)
- [AWS Deployment](#aws-deployment)
- [Production Checklist](#production-checklist)

## Prerequisites

- Git repository with latest code
- Python 3.11+
- Redis 6.0+ instance
- Environment variables configured

## Heroku Deployment

### Step 1: Install Heroku CLI

```bash
# macOS
brew install heroku/brew/heroku

# Other platforms
# Download from https://devcenter.heroku.com/articles/heroku-cli
```

### Step 2: Login and Create App

```bash
heroku login
heroku create your-app-name
```

### Step 3: Add Redis Addon

```bash
# Production (paid)
heroku addons:create heroku-redis:premium-0

# Development/Testing (free tier may be limited)
heroku addons:create heroku-redis:mini
```

### Step 4: Configure Environment Variables

```bash
heroku config:set ENVIRONMENT=production
heroku config:set LOG_LEVEL=INFO
heroku config:set ENABLE_BACKGROUND_JOBS=true
heroku config:set ENABLE_METRICS=true
heroku config:set TOP_N_STOCKS=10
heroku config:set SEARCH_RESULT_LIMIT=100
```

### Step 5: Deploy Application

```bash
git push heroku main
```

### Step 6: Scale Dynos

```bash
# Scale web and worker dynos
heroku ps:scale web=1 worker=1

# For production, scale up:
heroku ps:scale web=2 worker=2
```

### Step 7: Verify Deployment

```bash
# Open application
heroku open

# Check health endpoint
curl https://your-app-name.herokuapp.com/health

# View logs
heroku logs --tail
```

### Step 8: Load Initial Data

```bash
# Trigger data refresh
curl -X POST https://your-app-name.herokuapp.com/refresh
```

## Docker Deployment

### Local Development with Docker Compose

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Rebuild after code changes
docker-compose up -d --build
```

### Production Docker Deployment

1. **Build production image**
   ```bash
   docker build -t bse-stock-tracker:latest .
   ```

2. **Tag for registry**
   ```bash
   docker tag bse-stock-tracker:latest your-registry/bse-stock-tracker:latest
   ```

3. **Push to registry**
   ```bash
   docker push your-registry/bse-stock-tracker:latest
   ```

4. **Deploy to server**
   ```bash
   # On production server
   docker pull your-registry/bse-stock-tracker:latest
   docker run -d \
     --name bse-tracker-web \
     -p 5000:5000 \
     -e REDIS_URL=redis://redis-host:6379/0 \
     -e ENVIRONMENT=production \
     your-registry/bse-stock-tracker:latest
   ```

## AWS Deployment

### Option 1: AWS Elastic Beanstalk

1. **Install EB CLI**
   ```bash
   pip install awsebcli
   ```

2. **Initialize EB**
   ```bash
   eb init -p python-3.11 bse-stock-tracker
   ```

3. **Create environment**
   ```bash
   eb create production-env
   ```

4. **Set environment variables**
   ```bash
   eb setenv REDIS_URL=your-redis-url ENVIRONMENT=production
   ```

5. **Deploy**
   ```bash
   eb deploy
   ```

### Option 2: AWS ECS (Fargate)

1. **Create ECR repository**
   ```bash
   aws ecr create-repository --repository-name bse-stock-tracker
   ```

2. **Build and push image**
   ```bash
   aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com
   docker build -t bse-stock-tracker .
   docker tag bse-stock-tracker:latest <account-id>.dkr.ecr.us-east-1.amazonaws.com/bse-stock-tracker:latest
   docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/bse-stock-tracker:latest
   ```

3. **Create ElastiCache Redis cluster**
   ```bash
   aws elasticache create-cache-cluster \
     --cache-cluster-id bse-redis \
     --engine redis \
     --cache-node-type cache.t3.micro \
     --num-cache-nodes 1
   ```

4. **Create ECS task definition and service** (use AWS Console or CLI)

### Option 3: AWS Lambda + API Gateway

For serverless deployment (requires additional refactoring):
- Use AWS Lambda for API endpoints
- Use EventBridge for scheduled data refresh
- Use ElastiCache for Redis
- Use CloudWatch for logging

## Production Checklist

### Before Deployment

- [ ] All tests passing: `pytest`
- [ ] Code quality checks passed: `flake8 . && black --check .`
- [ ] Environment variables configured
- [ ] Redis instance provisioned and accessible
- [ ] SSL/TLS certificates configured
- [ ] Domain name configured (if applicable)
- [ ] Secrets stored securely (not in code)
- [ ] Database backup strategy in place

### After Deployment

- [ ] Health check endpoint responding: `/health`
- [ ] Metrics endpoint accessible: `/metrics_endpoint`
- [ ] Data refresh working: `/refresh`
- [ ] Search functionality working
- [ ] Background worker processing jobs
- [ ] Logs being collected
- [ ] Monitoring alerts configured
- [ ] Performance testing completed
- [ ] Security scan completed

### Security Checklist

- [ ] HTTPS enabled
- [ ] Environment variables used for secrets
- [ ] Dependencies updated: `pip list --outdated`
- [ ] Security audit: `safety check`
- [ ] Rate limiting configured
- [ ] CORS configured (if needed)
- [ ] Input validation in place
- [ ] Error messages don't leak sensitive data
- [ ] Logging doesn't include sensitive data

### Monitoring Setup

1. **Application Monitoring**
   - Set up Prometheus to scrape `/metrics_endpoint`
   - Configure Grafana dashboards
   - Set up alerts for error rates, latency, etc.

2. **Log Aggregation**
   - Configure log shipping to centralized service
   - Set up log-based alerts
   - Configure log retention policies

3. **Uptime Monitoring**
   - Configure external uptime monitor (Pingdom, UptimeRobot, etc.)
   - Monitor `/health` endpoint
   - Set up alert notifications

4. **Performance Monitoring**
   - Configure APM tool (New Relic, Datadog, etc.)
   - Monitor database performance
   - Track response times

## Scaling Recommendations

### Vertical Scaling
- Start with 1 web dyno, 1 worker
- Monitor CPU and memory usage
- Scale up dyno size if needed

### Horizontal Scaling
- Add more web dynos for increased traffic
- Add more workers for faster data processing
- Scale Redis if needed (cluster mode)

### Performance Optimization
- Enable Redis persistence
- Implement caching for frequently accessed data
- Use CDN for static assets
- Enable HTTP/2
- Implement connection pooling (already done)

## Troubleshooting Deployment

### Common Issues

**Issue**: Application crashes on startup
- Check logs for errors
- Verify all environment variables are set
- Ensure Redis is accessible

**Issue**: Can't connect to Redis
- Verify Redis URL is correct
- Check network/firewall rules
- Ensure Redis is running

**Issue**: Background jobs not processing
- Verify worker dyno is running
- Check worker logs
- Ensure Redis Queue is configured

**Issue**: High memory usage
- Reduce connection pool size
- Lower result limits
- Enable Redis eviction policies

## Rollback Procedure

### Heroku
```bash
# List releases
heroku releases

# Rollback to previous version
heroku rollback v<version-number>
```

### Docker
```bash
# Revert to previous image
docker pull your-registry/bse-stock-tracker:previous-tag
docker-compose up -d
```

## Maintenance

### Regular Tasks

**Daily**
- Monitor error rates
- Check application health
- Review logs for anomalies

**Weekly**
- Review performance metrics
- Check dependency updates
- Verify backups

**Monthly**
- Update dependencies: `pip install --upgrade -r requirements.txt`
- Security audit: `safety check`
- Performance testing
- Capacity planning review

### Backup Strategy

**Redis Data**
```bash
# Heroku
heroku redis:backups:capture --app your-app-name

# Manual backup
redis-cli --rdb /path/to/backup.rdb
```

**Application Code**
- Use Git for version control
- Tag releases: `git tag v2.0.0`
- Maintain changelog

## Support

For deployment issues:
- Check application logs
- Review monitoring dashboards
- Contact support team
- Create GitHub issue for bugs

## Additional Resources

- [Heroku Python Documentation](https://devcenter.heroku.com/categories/python-support)
- [Docker Documentation](https://docs.docker.com/)
- [AWS Documentation](https://docs.aws.amazon.com/)
- [Redis Documentation](https://redis.io/documentation)
