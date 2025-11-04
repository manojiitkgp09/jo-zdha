# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2025-01-04

### Added
- **Configuration Management**: Environment-based configuration with validation using pydantic-settings
- **Structured Logging**: Comprehensive logging using structlog with JSON output for production
- **Background Jobs**: Asynchronous data refresh using RQ (Redis Queue)
- **Health Checks**: `/health` endpoint for monitoring application and dependency status
- **Metrics**: Prometheus metrics endpoint (`/metrics_endpoint`) for monitoring
- **Job Status API**: `/job_status/<job_id>` endpoint to check background job progress
- **Error Handling**: Comprehensive error handling with proper exception classes
- **Input Validation**: Input sanitization to prevent injection attacks
- **Security Improvements**:
  - Protection against path traversal (zip slip vulnerability)
  - Redis pattern injection prevention
  - File size limits to prevent DoS
  - HTTP timeout configuration
  - Secure dependency versions
- **Testing**: Comprehensive unit test suite with pytest
- **CI/CD**: GitHub Actions workflow for automated testing
- **Documentation**:
  - Comprehensive README with setup and usage instructions
  - Deployment guide for multiple platforms
  - API documentation
  - Docker support with docker-compose
- **Code Quality**: Type hints, code formatting, and linting configuration
- **Development Tools**: Development requirements with testing and linting tools

### Changed
- **Python Version**: Upgraded from 3.6.7 to 3.11.7 (security and performance)
- **Dependencies**: Updated all dependencies to latest secure versions
  - CherryPy 18.1.2 → 18.9.0
  - Redis 3.2.1 → 5.0.1
  - Jinja2 2.10.1 → 3.1.3
  - Requests 2.22.0 → 2.31.0
  - Removed legacy dependencies
- **Architecture**: Complete refactor with separation of concerns
  - `webapp.py` → `app.py` (renamed and refactored)
  - `run.py` → `crawler.py` (improved and renamed)
  - `myredis.py` → `redis_client.py` (enhanced and renamed)
  - Added `config.py`, `logger.py`, `metrics.py`, `jobs.py`
- **Redis Data Model**: Improved data structure with proper indexing
  - Stock key pattern: `stock:{code}` (simplified)
  - Name index pattern: `name_index:{name}` for efficient search
  - Top stocks now sorted by close price (not arbitrary code)
  - Added set of all stocks for efficient counting
- **Error Pages**: Custom 404 and 500 error pages
- **Templates**: Enhanced with proper HTML5 structure and better UX
- **CSS**: Improved styling with modern design

### Fixed
- **Bare Exception Handling**: Replaced with specific exception types
- **File Cleanup**: Guaranteed cleanup using context managers and finally blocks
- **Race Conditions**: Proper data clearing before refresh
- **Memory Leaks**: Proper connection pooling and resource management
- **Search Performance**: O(N) → Indexed search using Redis sets
- **Top Stocks Logic**: Now actually returns top stocks by close price
- **Zip Extraction**: Security fixes for path validation
- **CSV Processing**: Proper error handling and validation

### Removed
- Bare `except:` blocks (replaced with specific exceptions)
- Hard-coded configuration (moved to environment variables)
- Code duplication between `run.py` and `webapp.py`
- Outdated and vulnerable dependencies
- Global state and class-level shared instances

### Security
- All dependencies updated to versions without known CVEs
- Input validation and sanitization implemented
- Protection against common web vulnerabilities
- Secure file handling with path validation
- No secrets in code (environment variables only)

### Performance
- Connection pooling for Redis (up to 50 connections)
- Batch operations using Redis pipelines
- HTTP retry logic with exponential backoff
- Background job processing for heavy operations
- Static file caching headers

### Deprecated
- `myredis.py`: Use `redis_client.py` instead
- `run.py`: Use `crawler.py` instead
- `webapp.py`: Use `app.py` instead
- Old API endpoint responses (now returns structured JSON)

## [1.0.0] - 2019-07-25

### Added
- Initial prototype release
- BSE Bhavcopy data scraping
- Redis data storage
- CherryPy web application
- Basic search functionality
- Top 10 stocks display
- Responsive CSS design
- Heroku deployment support

### Known Issues (Fixed in 2.0.0)
- No error handling
- No logging
- No tests
- Security vulnerabilities
- Hard-coded configuration
- Synchronous operations blocking UI
- No monitoring or health checks

---

## Migration Guide: 1.0.0 → 2.0.0

### Breaking Changes

1. **Python Version**: Requires Python 3.11+
2. **Entry Point**: Run `python app.py` instead of `python webapp.py`
3. **Environment Variables**: Required configuration via `.env` file
4. **Redis Data**: Data will be cleared and reloaded on first refresh

### Migration Steps

1. Update Python to 3.11+
2. Install new dependencies: `pip install -r requirements.txt`
3. Create `.env` file from `.env.example`
4. Update any deployment scripts to use `app.py`
5. Enable worker dyno for background jobs: `heroku ps:scale worker=1`
6. Trigger data refresh: Visit `/refresh` endpoint

### Backward Compatibility

The old modules (`myredis.py`, `run.py`, `webapp.py`) are kept for reference but are deprecated. The new `redis_client.py` includes a `MyRedis` wrapper class for basic backward compatibility, but it's recommended to migrate to the new APIs.

[2.0.0]: https://github.com/manojiitkgp09/jo-zdha/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/manojiitkgp09/jo-zdha/releases/tag/v1.0.0
