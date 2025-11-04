"""
Background job definitions for asynchronous task processing.
"""
from typing import Dict
from redis_client import RedisClient
from crawler import BSEBhavcopyCrawler
from logger import get_logger

logger = get_logger(__name__)


def refresh_stock_data() -> Dict[str, any]:
    """
    Background job to refresh stock data from BSE.

    This function is designed to run asynchronously via RQ (Redis Queue).

    Returns:
        Dictionary with job execution results
    """
    logger.info("refresh_job_started")

    redis_client = None
    crawler = None

    try:
        # Initialize clients
        redis_client = RedisClient()
        crawler = BSEBhavcopyCrawler(redis_client)

        # Execute crawler
        result = crawler.run()

        logger.info("refresh_job_completed", **result)
        return result

    except Exception as e:
        logger.error("refresh_job_failed", error=str(e))
        raise

    finally:
        # Cleanup
        if crawler:
            crawler.close()
        if redis_client:
            redis_client.close()
