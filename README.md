# BSE Stock Tracker

A production-ready Python web application for tracking and searching BSE (Bombay Stock Exchange) stock data.

## Features

### Core Functionality
- ✅ Automated scraping of BSE Bhavcopy data
- ✅ Real-time stock search by name
- ✅ Display top stocks sorted by close price
- ✅ Responsive web interface (mobile-friendly)

### Production Features
- ✅ Comprehensive error handling and logging
- ✅ Background job processing for data refresh
- ✅ Health check and monitoring endpoints
- ✅ Prometheus metrics integration
- ✅ Input validation and security hardening
- ✅ Connection pooling and retry logic
- ✅ Comprehensive test coverage
- ✅ Configuration management via environment variables

## Architecture

```
┌─────────────┐
│   Browser   │
└──────┬──────┘
       │
       ▼
┌─────────────┐     ┌──────────────┐
│  CherryPy   │────▶│ Redis Queue  │
│  Web Server │     │   (RQ Jobs)  │
└──────┬──────┘     └──────┬───────┘
       │                   │
       │                   ▼
       │            ┌─────────────┐
       └───────────▶│    Redis    │
                    │  (Database) │
                    └─────────────┘
```

### Components

- **app.py**: CherryPy web application with RESTful endpoints
- **crawler.py**: BSE data scraper with robust error handling
- **redis_client.py**: Redis abstraction layer with connection pooling
- **jobs.py**: Background job definitions for async processing
- **config.py**: Configuration management with validation
- **logger.py**: Structured logging setup
- **metrics.py**: Prometheus metrics definitions

## Installation

### Prerequisites

- Python 3.11+
- Redis 6.0+
- pip

### Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd jo-zdha
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt  # For development
   ```

4. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

5. **Start Redis** (if not already running)
   ```bash
   redis-server
   ```

6. **Run the application**
   ```bash
   python app.py
   ```

7. **Start background worker** (in separate terminal)
   ```bash
   rq worker --url redis://localhost:6379/0 default
   ```

8. **Access the application**
   - Web interface: http://localhost:5000
   - Health check: http://localhost:5000/health
   - Metrics: http://localhost:5000/metrics_endpoint

## Configuration

All configuration is managed through environment variables. See `.env.example` for available options.

### Key Configuration Options

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_URL` | Redis connection URL | `redis://127.0.0.1:6379/0` |
| `SERVER_PORT` | Web server port | `5000` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `ENVIRONMENT` | Environment (development/staging/production) | `development` |
| `ENABLE_BACKGROUND_JOBS` | Enable async job processing | `true` |
| `ENABLE_METRICS` | Enable Prometheus metrics | `true` |
| `TOP_N_STOCKS` | Number of top stocks to display | `10` |
| `SEARCH_RESULT_LIMIT` | Max search results | `100` |

## API Endpoints

### Web Pages

- `GET /` - Home page with top stocks
- `GET /search?name=<query>` - Search stocks by name

### API Endpoints

- `GET /health` - Health check (returns JSON)
- `POST /refresh` - Trigger data refresh (queues background job)
- `GET /job_status/<job_id>` - Check background job status
- `GET /metrics_endpoint` - Prometheus metrics

### Health Check Response

```json
{
  "status": "healthy",
  "redis": "up",
  "queue": "up",
  "environment": "production",
  "version": "2.0.0"
}
```

### Refresh Response

```json
{
  "status": "queued",
  "job_id": "abc123",
  "message": "Stock data refresh has been queued",
  "check_status_url": "/job_status/abc123"
}
```

## Testing

### Run all tests
```bash
pytest
```

### Run with coverage
```bash
pytest --cov=. --cov-report=html
```

### Run specific test file
```bash
pytest tests/test_redis_client.py
```

### View coverage report
```bash
open htmlcov/index.html  # On macOS
# Or navigate to htmlcov/index.html in your browser
```

## Deployment

### Heroku Deployment

1. **Create Heroku app**
   ```bash
   heroku create your-app-name
   ```

2. **Add Redis addon**
   ```bash
   heroku addons:create heroku-redis:mini
   ```

3. **Configure environment variables**
   ```bash
   heroku config:set ENVIRONMENT=production
   heroku config:set LOG_LEVEL=INFO
   heroku config:set ENABLE_BACKGROUND_JOBS=true
   ```

4. **Deploy**
   ```bash
   git push heroku main
   ```

5. **Scale workers**
   ```bash
   heroku ps:scale web=1 worker=1
   ```

6. **View logs**
   ```bash
   heroku logs --tail
   ```

### Docker Deployment

1. **Build image**
   ```bash
   docker build -t bse-stock-tracker .
   ```

2. **Run with docker-compose**
   ```bash
   docker-compose up -d
   ```

## Monitoring

### Prometheus Metrics

The application exposes Prometheus-compatible metrics at `/metrics_endpoint`:

- `http_requests_total` - Total HTTP requests by method, endpoint, status
- `http_request_duration_seconds` - Request latency histogram
- `stocks_in_database` - Current number of stocks
- `stock_refresh_total` - Total refresh operations
- `stock_searches_total` - Total search operations
- `background_jobs_total` - Background job counts by type and status
- `redis_operations_total` - Redis operation counts

### Logging

Structured JSON logs are written to stdout and can be aggregated using:
- ELK Stack (Elasticsearch, Logstash, Kibana)
- Datadog
- CloudWatch
- Splunk

Log format:
```json
{
  "event": "stock_search_completed",
  "query": "reliance",
  "results": 1,
  "duration": 0.045,
  "timestamp": "2025-01-04T12:34:56.789Z",
  "level": "info"
}
```

## Security

### Implemented Security Measures

- ✅ Input validation and sanitization
- ✅ Protection against path traversal (zip slip)
- ✅ Redis injection prevention
- ✅ File size limits to prevent DoS
- ✅ Timeout configuration for all HTTP requests
- ✅ Secure dependency versions
- ✅ HTML auto-escaping in templates
- ✅ No secrets in code (environment variables only)

### Security Best Practices

- Keep dependencies updated: `pip list --outdated`
- Use environment variables for sensitive data
- Enable HTTPS in production
- Implement rate limiting for public endpoints
- Regular security audits: `pip install safety && safety check`

## Performance

### Optimizations Implemented

- Connection pooling for Redis
- Batch operations using Redis pipelines
- Background job processing for heavy tasks
- HTTP request retry logic with exponential backoff
- Indexed search using Redis sets
- Static file caching headers

### Expected Performance

- Search response time: < 100ms
- Top stocks query: < 50ms
- Data refresh: 30-60 seconds (async)
- Handles 100+ concurrent users

## Troubleshooting

### Common Issues

**Issue**: Application won't start
- **Solution**: Check Redis is running: `redis-cli ping`
- **Solution**: Verify environment variables are set

**Issue**: Background jobs not processing
- **Solution**: Ensure RQ worker is running: `rq worker`
- **Solution**: Check Redis connection in worker logs

**Issue**: No data showing
- **Solution**: Trigger manual refresh: Visit `/refresh`
- **Solution**: Check crawler logs for errors

**Issue**: High memory usage
- **Solution**: Reduce `REDIS_MAX_CONNECTIONS`
- **Solution**: Lower `SEARCH_RESULT_LIMIT`

### Debug Mode

Enable debug logging:
```bash
export LOG_LEVEL=DEBUG
python app.py
```

## Development

### Code Quality Tools

```bash
# Format code
black .

# Sort imports
isort .

# Lint
flake8 .
pylint *.py

# Type checking
mypy .
```

### Project Structure

```
.
├── app.py                  # Main web application
├── crawler.py              # BSE scraper
├── redis_client.py         # Redis client
├── jobs.py                 # Background jobs
├── config.py               # Configuration
├── logger.py               # Logging setup
├── metrics.py              # Prometheus metrics
├── requirements.txt        # Dependencies
├── requirements-dev.txt    # Dev dependencies
├── Procfile                # Heroku configuration
├── runtime.txt             # Python version
├── pytest.ini              # Test configuration
├── templates/              # Jinja2 templates
│   ├── index.jinja
│   └── error.jinja
├── public/                 # Static files
│   └── style.css
└── tests/                  # Test suite
    ├── test_redis_client.py
    └── test_crawler.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Run test suite: `pytest`
5. Run code quality checks: `black . && flake8 .`
6. Submit pull request

## License

MIT License - See LICENSE file for details

## Support

For issues and questions:
- GitHub Issues: [repository-url]/issues
- Email: your-email@example.com

## Changelog

### Version 2.0.0 (2025-01-04)
- ✅ Complete production-ready refactor
- ✅ Added background job processing
- ✅ Implemented comprehensive logging
- ✅ Added health checks and metrics
- ✅ Improved security and error handling
- ✅ Added extensive test coverage
- ✅ Updated to Python 3.11
- ✅ Modern dependency versions

### Version 1.0.0 (2019)
- Initial prototype release
