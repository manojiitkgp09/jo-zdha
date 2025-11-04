"""
Redis client with improved error handling, logging, and performance.
"""
import re
from typing import List, Dict, Optional
from contextlib import contextmanager
import redis
from redis.connection import ConnectionPool
from redis.exceptions import RedisError, ConnectionError, TimeoutError

from config import settings
from logger import get_logger

logger = get_logger(__name__)


class RedisClient:
    """
    Redis client for stock data management.

    Implements proper connection pooling, error handling,
    and efficient data access patterns.
    """

    # Field names for stock data
    COLUMNS = ['code', 'name', 'open', 'high', 'low', 'close']

    # Redis key patterns
    STOCK_KEY_PATTERN = "stock:{code}"
    STOCK_NAME_INDEX_PATTERN = "name_index:{name}"
    TOP_STOCKS_KEY = "top_stocks_by_close"
    ALL_STOCKS_SET = "all_stocks"

    def __init__(self):
        """Initialize Redis client with connection pooling."""
        try:
            self.pool = ConnectionPool.from_url(
                settings.redis_url,
                max_connections=settings.redis_max_connections,
                socket_timeout=settings.redis_socket_timeout,
                socket_connect_timeout=settings.redis_socket_connect_timeout,
                decode_responses=True,
                encoding='utf-8',
            )
            self.client = redis.Redis(connection_pool=self.pool)
            logger.info("redis_initialized", url=settings.redis_url)
        except RedisError as e:
            logger.error("redis_initialization_failed", error=str(e))
            raise

    @contextmanager
    def pipeline(self):
        """
        Context manager for Redis pipeline operations.

        Yields:
            Redis pipeline object
        """
        pipe = self.client.pipeline()
        try:
            yield pipe
            pipe.execute()
        except RedisError as e:
            logger.error("pipeline_execution_failed", error=str(e))
            raise
        finally:
            pipe.reset()

    def health_check(self) -> bool:
        """
        Check if Redis connection is healthy.

        Returns:
            True if healthy, False otherwise
        """
        try:
            return self.client.ping()
        except RedisError as e:
            logger.error("redis_health_check_failed", error=str(e))
            return False

    def clear_all_data(self) -> int:
        """
        Clear all stock data from Redis.

        Returns:
            Number of keys deleted

        Raises:
            RedisError: If deletion fails
        """
        try:
            keys_to_delete = []

            # Get all stock keys
            for key in self.client.scan_iter(match="stock:*", count=100):
                keys_to_delete.append(key)

            # Get all name index keys
            for key in self.client.scan_iter(match="name_index:*", count=100):
                keys_to_delete.append(key)

            # Add sorted set and set keys
            keys_to_delete.extend([self.TOP_STOCKS_KEY, self.ALL_STOCKS_SET])

            # Delete all keys in a pipeline for efficiency
            if keys_to_delete:
                with self.pipeline() as pipe:
                    for key in keys_to_delete:
                        pipe.delete(key)

            deleted_count = len(keys_to_delete)
            logger.info("redis_data_cleared", keys_deleted=deleted_count)
            return deleted_count

        except RedisError as e:
            logger.error("redis_clear_failed", error=str(e))
            raise

    def create_stock(self, data: Dict[str, str]) -> None:
        """
        Create or update a stock entry in Redis.

        Args:
            data: Dictionary with stock fields (code, name, open, high, low, close)

        Raises:
            ValueError: If required fields are missing
            RedisError: If Redis operation fails
        """
        # Validate required fields
        required_fields = {'code', 'name', 'open', 'high', 'low', 'close'}
        if not required_fields.issubset(data.keys()):
            missing = required_fields - set(data.keys())
            raise ValueError(f"Missing required fields: {missing}")

        try:
            code = data['code'].strip()
            name = data['name'].strip().lower()
            close_price = float(data['close'])

            stock_key = self.STOCK_KEY_PATTERN.format(code=code)
            name_index_key = self.STOCK_NAME_INDEX_PATTERN.format(name=name)

            with self.pipeline() as pipe:
                # Store stock data as hash
                pipe.hset(stock_key, mapping=data)

                # Add to name index for efficient searching
                pipe.sadd(name_index_key, stock_key)

                # Add to sorted set (sorted by close price for "top stocks")
                pipe.zadd(self.TOP_STOCKS_KEY, {stock_key: close_price})

                # Add to set of all stocks
                pipe.sadd(self.ALL_STOCKS_SET, stock_key)

        except ValueError as e:
            logger.error("invalid_stock_data", error=str(e), data=data)
            raise
        except RedisError as e:
            logger.error("stock_creation_failed", error=str(e), code=code)
            raise

    def get_top_stocks(self, limit: Optional[int] = None) -> List[Dict[str, str]]:
        """
        Get top N stocks sorted by close price (descending).

        Args:
            limit: Number of stocks to return (default from settings)

        Returns:
            List of stock dictionaries
        """
        if limit is None:
            limit = settings.top_n_stocks

        try:
            # Get top N stock keys from sorted set (highest scores first)
            stock_keys = self.client.zrevrange(
                self.TOP_STOCKS_KEY,
                0,
                limit - 1
            )

            return self._get_stocks_by_keys(stock_keys)

        except RedisError as e:
            logger.error("get_top_stocks_failed", error=str(e), limit=limit)
            return []

    def search_stocks_by_name(self, name: str) -> List[Dict[str, str]]:
        """
        Search for stocks by name (case-insensitive, partial match).

        Args:
            name: Stock name to search for

        Returns:
            List of matching stock dictionaries
        """
        if not name or not name.strip():
            logger.warning("empty_search_query")
            return []

        # Sanitize input to prevent injection
        sanitized_name = self._sanitize_search_input(name.strip().lower())

        if not sanitized_name:
            logger.warning("invalid_search_query", original=name)
            return []

        try:
            stock_keys = set()

            # Search through name index keys
            pattern = f"name_index:*{sanitized_name}*"
            for index_key in self.client.scan_iter(
                match=pattern,
                count=100
            ):
                # Get all stock keys from this index
                keys = self.client.smembers(index_key)
                stock_keys.update(keys)

                # Limit results
                if len(stock_keys) >= settings.search_result_limit:
                    break

            # Limit final results
            limited_keys = list(stock_keys)[:settings.search_result_limit]

            logger.info(
                "stock_search_completed",
                query=sanitized_name,
                results=len(limited_keys)
            )

            return self._get_stocks_by_keys(limited_keys)

        except RedisError as e:
            logger.error(
                "stock_search_failed",
                error=str(e),
                query=sanitized_name
            )
            return []

    def get_stock_count(self) -> int:
        """
        Get total number of stocks in database.

        Returns:
            Number of stocks
        """
        try:
            return self.client.scard(self.ALL_STOCKS_SET)
        except RedisError as e:
            logger.error("get_stock_count_failed", error=str(e))
            return 0

    def _get_stocks_by_keys(self, stock_keys: List[str]) -> List[Dict[str, str]]:
        """
        Retrieve stock data for given keys using pipeline for efficiency.

        Args:
            stock_keys: List of Redis stock keys

        Returns:
            List of stock dictionaries
        """
        if not stock_keys:
            return []

        try:
            # Use pipeline to fetch all stocks in one roundtrip
            with self.client.pipeline() as pipe:
                for key in stock_keys:
                    pipe.hgetall(key)
                results = pipe.execute()

            # Filter out empty results
            stocks = [stock for stock in results if stock]

            return stocks

        except RedisError as e:
            logger.error("get_stocks_by_keys_failed", error=str(e))
            return []

    @staticmethod
    def _sanitize_search_input(search_query: str) -> str:
        """
        Sanitize search input to prevent Redis pattern injection.

        Args:
            search_query: Raw search query

        Returns:
            Sanitized search query
        """
        # Remove Redis special characters that could be used for injection
        # Allow only alphanumeric, spaces, hyphens, and basic punctuation
        sanitized = re.sub(r'[^a-z0-9\s\-\.]', '', search_query)

        # Limit length
        max_length = 100
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length]

        return sanitized

    def close(self):
        """Close Redis connection pool."""
        try:
            self.pool.disconnect()
            logger.info("redis_connection_closed")
        except Exception as e:
            logger.error("redis_close_failed", error=str(e))


# For backward compatibility
class MyRedis(RedisClient):
    """Deprecated: Use RedisClient instead."""

    def __init__(self):
        logger.warning("MyRedis is deprecated, use RedisClient instead")
        super().__init__()

    @property
    def columns(self):
        """For backward compatibility."""
        return self.COLUMNS

    def get_top_ten(self):
        """For backward compatibility."""
        return self.get_top_stocks(10)

    def get_stock_by_name(self, name: str):
        """For backward compatibility."""
        return self.search_stocks_by_name(name)
