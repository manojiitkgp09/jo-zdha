"""
Prometheus metrics for monitoring application health and performance.
"""
from prometheus_client import Counter, Histogram, Gauge, Info
from config import settings

# Application info
app_info = Info('app', 'Application information')
app_info.info({
    'name': settings.app_name,
    'environment': settings.environment
})

# HTTP request metrics
http_requests_total = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status']
)

http_request_duration_seconds = Histogram(
    'http_request_duration_seconds',
    'HTTP request latency',
    ['method', 'endpoint']
)

# Stock data metrics
stocks_in_db = Gauge(
    'stocks_in_database',
    'Number of stocks currently in database'
)

stock_refresh_total = Counter(
    'stock_refresh_total',
    'Total number of stock data refreshes',
    ['status']  # success or failure
)

stock_refresh_duration_seconds = Histogram(
    'stock_refresh_duration_seconds',
    'Duration of stock data refresh operations'
)

stocks_processed_total = Counter(
    'stocks_processed_total',
    'Total number of stocks processed'
)

# Search metrics
stock_searches_total = Counter(
    'stock_searches_total',
    'Total number of stock searches'
)

stock_search_duration_seconds = Histogram(
    'stock_search_duration_seconds',
    'Duration of stock search operations'
)

stock_search_results = Histogram(
    'stock_search_results',
    'Number of results returned from searches'
)

# Redis metrics
redis_operations_total = Counter(
    'redis_operations_total',
    'Total Redis operations',
    ['operation', 'status']
)

redis_operation_duration_seconds = Histogram(
    'redis_operation_duration_seconds',
    'Redis operation duration',
    ['operation']
)

# Background job metrics
background_jobs_total = Counter(
    'background_jobs_total',
    'Total background jobs',
    ['job_type', 'status']
)

background_job_duration_seconds = Histogram(
    'background_job_duration_seconds',
    'Background job duration',
    ['job_type']
)
