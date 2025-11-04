"""Tests for Redis client functionality."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from redis.exceptions import RedisError

from redis_client import RedisClient


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    with patch('redis_client.redis.Redis') as mock:
        yield mock


@pytest.fixture
def redis_client(mock_redis):
    """Create RedisClient instance with mocked Redis."""
    with patch('redis_client.ConnectionPool'):
        client = RedisClient()
        client.client = MagicMock()
        return client


class TestRedisClient:
    """Test RedisClient class."""

    def test_initialization(self, mock_redis):
        """Test Redis client initialization."""
        with patch('redis_client.ConnectionPool'):
            client = RedisClient()
            assert client is not None

    def test_health_check_success(self, redis_client):
        """Test successful health check."""
        redis_client.client.ping.return_value = True
        assert redis_client.health_check() is True

    def test_health_check_failure(self, redis_client):
        """Test failed health check."""
        redis_client.client.ping.side_effect = RedisError("Connection failed")
        assert redis_client.health_check() is False

    def test_create_stock_valid_data(self, redis_client):
        """Test creating stock with valid data."""
        stock_data = {
            'code': '500325',
            'name': 'RELIANCE',
            'open': '2450.00',
            'high': '2480.00',
            'low': '2440.00',
            'close': '2470.00'
        }

        # Mock pipeline
        mock_pipe = MagicMock()
        redis_client.client.pipeline.return_value.__enter__.return_value = mock_pipe
        redis_client.client.pipeline.return_value.__exit__.return_value = None

        redis_client.create_stock(stock_data)

        # Verify pipeline operations were called
        mock_pipe.hset.assert_called_once()
        mock_pipe.sadd.assert_called()
        mock_pipe.zadd.assert_called_once()

    def test_create_stock_missing_fields(self, redis_client):
        """Test creating stock with missing required fields."""
        incomplete_data = {
            'code': '500325',
            'name': 'RELIANCE'
            # Missing open, high, low, close
        }

        with pytest.raises(ValueError, match="Missing required fields"):
            redis_client.create_stock(incomplete_data)

    def test_sanitize_search_input(self):
        """Test search input sanitization."""
        # Test normal input
        assert RedisClient._sanitize_search_input("reliance") == "reliance"

        # Test input with special characters
        assert RedisClient._sanitize_search_input("rel*ance?") == "relance"

        # Test input with SQL injection attempt
        assert RedisClient._sanitize_search_input("'; DROP TABLE stocks; --") == " drop table stocks "

        # Test long input
        long_input = "a" * 150
        result = RedisClient._sanitize_search_input(long_input)
        assert len(result) <= 100

    def test_get_top_stocks(self, redis_client):
        """Test getting top stocks."""
        mock_keys = [b'stock:500325', b'stock:500312']
        redis_client.client.zrevrange.return_value = mock_keys

        mock_stocks = [
            {'code': '500325', 'name': 'RELIANCE', 'close': '2470.00'},
            {'code': '500312', 'name': 'TCS', 'close': '3500.00'}
        ]

        # Mock pipeline for batch fetch
        mock_pipe = MagicMock()
        mock_pipe.execute.return_value = mock_stocks
        redis_client.client.pipeline.return_value.__enter__.return_value = mock_pipe
        redis_client.client.pipeline.return_value.__exit__.return_value = None

        stocks = redis_client.get_top_stocks(10)

        assert len(stocks) == 2
        redis_client.client.zrevrange.assert_called_once()

    def test_search_stocks_by_name(self, redis_client):
        """Test searching stocks by name."""
        search_query = "reliance"

        # Mock scan_iter
        redis_client.client.scan_iter.return_value = [
            'name_index:reliance'
        ]

        # Mock smembers
        redis_client.client.smembers.return_value = {'stock:500325'}

        # Mock pipeline for batch fetch
        mock_stock = {'code': '500325', 'name': 'RELIANCE', 'close': '2470.00'}
        mock_pipe = MagicMock()
        mock_pipe.execute.return_value = [mock_stock]
        redis_client.client.pipeline.return_value.__enter__.return_value = mock_pipe
        redis_client.client.pipeline.return_value.__exit__.return_value = None

        stocks = redis_client.search_stocks_by_name(search_query)

        assert len(stocks) == 1

    def test_search_stocks_empty_query(self, redis_client):
        """Test search with empty query."""
        stocks = redis_client.search_stocks_by_name("")
        assert stocks == []

        stocks = redis_client.search_stocks_by_name("   ")
        assert stocks == []

    def test_get_stock_count(self, redis_client):
        """Test getting stock count."""
        redis_client.client.scard.return_value = 100
        count = redis_client.get_stock_count()
        assert count == 100

    def test_clear_all_data(self, redis_client):
        """Test clearing all data."""
        # Mock scan_iter to return some keys
        redis_client.client.scan_iter.side_effect = [
            ['stock:1', 'stock:2'],
            ['name_index:reliance']
        ]

        # Mock pipeline
        mock_pipe = MagicMock()
        redis_client.client.pipeline.return_value.__enter__.return_value = mock_pipe
        redis_client.client.pipeline.return_value.__exit__.return_value = None

        deleted_count = redis_client.clear_all_data()

        assert deleted_count > 0
        mock_pipe.delete.assert_called()
