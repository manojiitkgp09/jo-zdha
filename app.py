"""
Production-ready CherryPy web application for BSE stock data.

Features:
- Health checks and monitoring
- Prometheus metrics
- Background job processing
- Proper error handling and logging
- Input validation and security
"""
import os
import time
from typing import Optional

import cherrypy
import jinja2
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from rq import Queue
from rq.job import Job
from redis import Redis

from config import settings
from logger import configure_logging, get_logger
from redis_client import RedisClient
from jobs import refresh_stock_data
import metrics

# Configure logging
configure_logging()
logger = get_logger(__name__)

# Jinja2 template configuration
root_path = os.path.dirname(__file__)
template_loader = jinja2.FileSystemLoader(os.path.join(root_path, 'templates'))
jinja_env = jinja2.Environment(
    loader=template_loader,
    autoescape=jinja2.select_autoescape(['html', 'xml', 'jinja'])
)


def render_template(template_name: str, **context) -> str:
    """
    Render Jinja2 template with context.

    Args:
        template_name: Name of template file (without .jinja extension)
        **context: Template context variables

    Returns:
        Rendered HTML string
    """
    template = jinja_env.get_template(f'{template_name}.jinja')
    return template.render(**context)


class StockApplication:
    """Main web application for stock data."""

    def __init__(self):
        """Initialize application components."""
        self.redis_client = RedisClient()

        # Initialize job queue if background jobs are enabled
        self.queue = None
        if settings.enable_background_jobs:
            try:
                redis_conn = Redis.from_url(settings.redis_url)
                self.queue = Queue('default', connection=redis_conn)
                logger.info("job_queue_initialized")
            except Exception as e:
                logger.error("job_queue_initialization_failed", error=str(e))

        logger.info("application_initialized")

    @cherrypy.expose
    def index(self):
        """
        Home page - displays top N stocks.

        Returns:
            Rendered HTML page
        """
        start_time = time.time()
        method = cherrypy.request.method
        endpoint = 'index'

        try:
            logger.info("index_page_requested")

            # Get top stocks
            top_stocks = self.redis_client.get_top_stocks()
            stock_count = self.redis_client.get_stock_count()

            # Update metrics
            if settings.enable_metrics:
                metrics.http_requests_total.labels(
                    method=method,
                    endpoint=endpoint,
                    status='200'
                ).inc()

            duration = time.time() - start_time
            if settings.enable_metrics:
                metrics.http_request_duration_seconds.labels(
                    method=method,
                    endpoint=endpoint
                ).observe(duration)

            logger.info(
                "index_page_rendered",
                stocks_count=len(top_stocks),
                total_stocks=stock_count,
                duration=round(duration, 3)
            )

            return render_template(
                'index',
                stocks=top_stocks,
                columns=RedisClient.COLUMNS,
                total_stocks=stock_count
            )

        except Exception as e:
            logger.error("index_page_error", error=str(e))

            if settings.enable_metrics:
                metrics.http_requests_total.labels(
                    method=method,
                    endpoint=endpoint,
                    status='500'
                ).inc()

            raise cherrypy.HTTPError(500, "Internal server error")

    @cherrypy.expose
    def search(self, name: Optional[str] = None):
        """
        Search page - search stocks by name.

        Args:
            name: Stock name to search for

        Returns:
            Rendered HTML page with search results
        """
        start_time = time.time()
        method = cherrypy.request.method
        endpoint = 'search'

        try:
            if not name:
                logger.warning("search_empty_query")
                return render_template(
                    'index',
                    stocks=[],
                    columns=RedisClient.COLUMNS,
                    error="Please enter a search term",
                    search_query=""
                )

            # Validate input length
            if len(name) > 100:
                logger.warning("search_query_too_long", length=len(name))
                name = name[:100]

            logger.info("stock_search_requested", query=name)

            # Search stocks
            search_start = time.time()
            stocks = self.redis_client.search_stocks_by_name(name)
            search_duration = time.time() - search_start

            # Update metrics
            if settings.enable_metrics:
                metrics.stock_searches_total.inc()
                metrics.stock_search_duration_seconds.observe(search_duration)
                metrics.stock_search_results.observe(len(stocks))
                metrics.http_requests_total.labels(
                    method=method,
                    endpoint=endpoint,
                    status='200'
                ).inc()

            duration = time.time() - start_time
            if settings.enable_metrics:
                metrics.http_request_duration_seconds.labels(
                    method=method,
                    endpoint=endpoint
                ).observe(duration)

            logger.info(
                "search_completed",
                query=name,
                results=len(stocks),
                duration=round(duration, 3)
            )

            return render_template(
                'index',
                stocks=stocks,
                columns=RedisClient.COLUMNS,
                search_query=name,
                result_count=len(stocks)
            )

        except Exception as e:
            logger.error("search_error", error=str(e), query=name)

            if settings.enable_metrics:
                metrics.http_requests_total.labels(
                    method=method,
                    endpoint=endpoint,
                    status='500'
                ).inc()

            raise cherrypy.HTTPError(500, "Search failed")

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def refresh(self):
        """
        Trigger stock data refresh.

        If background jobs are enabled, queues a job and returns immediately.
        Otherwise, runs synchronously (not recommended for production).

        Returns:
            JSON response with job status
        """
        start_time = time.time()
        method = cherrypy.request.method
        endpoint = 'refresh'

        try:
            logger.info("refresh_requested")

            if settings.enable_background_jobs and self.queue:
                # Queue background job
                job = self.queue.enqueue(
                    refresh_stock_data,
                    job_timeout='10m',
                    result_ttl=3600,
                    failure_ttl=3600
                )

                logger.info("refresh_job_queued", job_id=job.id)

                if settings.enable_metrics:
                    metrics.background_jobs_total.labels(
                        job_type='refresh',
                        status='queued'
                    ).inc()
                    metrics.http_requests_total.labels(
                        method=method,
                        endpoint=endpoint,
                        status='202'
                    ).inc()

                cherrypy.response.status = 202  # Accepted

                return {
                    'status': 'queued',
                    'job_id': job.id,
                    'message': 'Stock data refresh has been queued',
                    'check_status_url': f'/job_status/{job.id}'
                }

            else:
                # Run synchronously (fallback)
                logger.warning("refresh_running_synchronously")

                from crawler import BSEBhavcopyCrawler
                crawler = BSEBhavcopyCrawler(self.redis_client)

                try:
                    result = crawler.run()

                    if settings.enable_metrics:
                        metrics.stock_refresh_total.labels(status='success').inc()
                        metrics.http_requests_total.labels(
                            method=method,
                            endpoint=endpoint,
                            status='200'
                        ).inc()

                    return {
                        'status': 'completed',
                        'message': 'Stock data refreshed successfully',
                        **result
                    }

                finally:
                    crawler.close()

        except Exception as e:
            logger.error("refresh_error", error=str(e))

            if settings.enable_metrics:
                metrics.stock_refresh_total.labels(status='failure').inc()
                metrics.http_requests_total.labels(
                    method=method,
                    endpoint=endpoint,
                    status='500'
                ).inc()

            raise cherrypy.HTTPError(500, f"Refresh failed: {str(e)}")

        finally:
            duration = time.time() - start_time
            if settings.enable_metrics:
                metrics.http_request_duration_seconds.labels(
                    method=method,
                    endpoint=endpoint
                ).observe(duration)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def job_status(self, job_id: str):
        """
        Check status of a background job.

        Args:
            job_id: Job identifier

        Returns:
            JSON response with job status
        """
        if not self.queue:
            raise cherrypy.HTTPError(501, "Background jobs not enabled")

        try:
            job = Job.fetch(job_id, connection=self.queue.connection)

            response = {
                'job_id': job.id,
                'status': job.get_status(),
                'created_at': job.created_at.isoformat() if job.created_at else None,
                'started_at': job.started_at.isoformat() if job.started_at else None,
                'ended_at': job.ended_at.isoformat() if job.ended_at else None,
            }

            if job.is_finished:
                response['result'] = job.result

            if job.is_failed:
                response['error'] = str(job.exc_info) if job.exc_info else 'Unknown error'

            return response

        except Exception as e:
            logger.error("job_status_error", job_id=job_id, error=str(e))
            raise cherrypy.HTTPError(404, f"Job not found: {job_id}")

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def health(self):
        """
        Health check endpoint.

        Returns:
            JSON response with health status
        """
        try:
            # Check Redis connection
            redis_healthy = self.redis_client.health_check()

            # Check job queue
            queue_healthy = True
            if settings.enable_background_jobs and self.queue:
                try:
                    self.queue.connection.ping()
                except Exception:
                    queue_healthy = False

            # Overall health
            healthy = redis_healthy and queue_healthy

            status_code = 200 if healthy else 503
            cherrypy.response.status = status_code

            # Update metrics
            if settings.enable_metrics:
                stock_count = self.redis_client.get_stock_count()
                metrics.stocks_in_db.set(stock_count)

            return {
                'status': 'healthy' if healthy else 'unhealthy',
                'redis': 'up' if redis_healthy else 'down',
                'queue': 'up' if queue_healthy else 'down',
                'environment': settings.environment,
                'version': '2.0.0'
            }

        except Exception as e:
            logger.error("health_check_error", error=str(e))
            cherrypy.response.status = 503
            return {
                'status': 'unhealthy',
                'error': str(e)
            }

    @cherrypy.expose
    def metrics_endpoint(self):
        """
        Prometheus metrics endpoint.

        Returns:
            Prometheus metrics in text format
        """
        if not settings.enable_metrics:
            raise cherrypy.HTTPError(404, "Metrics not enabled")

        cherrypy.response.headers['Content-Type'] = CONTENT_TYPE_LATEST
        return generate_latest()


def error_page_404(status, message, traceback, version):
    """Custom 404 error page."""
    return render_template('error', error_code=404, error_message="Page not found")


def error_page_500(status, message, traceback, version):
    """Custom 500 error page."""
    logger.error("internal_server_error", message=message)
    return render_template('error', error_code=500, error_message="Internal server error")


def configure_cherrypy():
    """Configure CherryPy server settings."""

    cherrypy.config.update({
        'server.socket_host': settings.server_host,
        'server.socket_port': settings.server_port,
        'server.thread_pool': 10,
        'server.thread_pool_max': 50,
        'log.screen': False,  # Use structlog instead
        'log.access_file': '',
        'log.error_file': '',
        'engine.autoreload.on': settings.is_development,
    })

    app_config = {
        '/': {
            'tools.encode.on': True,
            'tools.encode.encoding': 'utf-8',
            'tools.staticdir.root': os.path.abspath(os.getcwd()),
            'error_page.404': error_page_404,
            'error_page.500': error_page_500,
        },
        '/static': {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': './public',
            'tools.expires.on': True,
            'tools.expires.secs': 3600,
        }
    }

    return app_config


def main():
    """Main application entry point."""
    logger.info(
        "application_starting",
        environment=settings.environment,
        port=settings.server_port
    )

    app_config = configure_cherrypy()
    app = StockApplication()

    cherrypy.quickstart(app, '/', app_config)


if __name__ == '__main__':
    main()
