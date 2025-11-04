"""Tests for BSE Bhavcopy crawler."""
import pytest
from unittest.mock import Mock, patch, MagicMock, mock_open
from pathlib import Path
from zipfile import ZipFile, BadZipFile
from io import BytesIO
import csv

from crawler import (
    BSEBhavcopyCrawler,
    DownloadError,
    ParseError,
    DataProcessingError
)
from redis_client import RedisClient


@pytest.fixture
def mock_redis_client():
    """Mock Redis client."""
    return MagicMock(spec=RedisClient)


@pytest.fixture
def crawler(mock_redis_client):
    """Create crawler instance with mocked Redis."""
    return BSEBhavcopyCrawler(mock_redis_client)


class TestBSEBhavcopyCrawler:
    """Test BSEBhavcopyCrawler class."""

    def test_initialization(self, mock_redis_client):
        """Test crawler initialization."""
        crawler = BSEBhavcopyCrawler(mock_redis_client)
        assert crawler.redis_client == mock_redis_client
        assert crawler.session is not None

    @patch('crawler.requests.Session')
    def test_fetch_download_page_success(self, mock_session, crawler):
        """Test successful page fetch."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "<html>Test Page</html>"
        mock_response.raise_for_status = Mock()

        crawler.session.get = Mock(return_value=mock_response)

        html = crawler.fetch_download_page()

        assert html == "<html>Test Page</html>"
        crawler.session.get.assert_called_once()

    @patch('crawler.requests.Session')
    def test_fetch_download_page_failure(self, mock_session, crawler):
        """Test failed page fetch."""
        import requests
        crawler.session.get = Mock(side_effect=requests.RequestException("Network error"))

        with pytest.raises(DownloadError, match="Failed to fetch download page"):
            crawler.fetch_download_page()

    def test_extract_download_url_success(self, crawler):
        """Test successful URL extraction."""
        html = '''
        <html>
            <a id="ContentPlaceHolder1_btnhylZip" href="/download/BhavCopy/Equity/EQ010125_CSV.ZIP">Download</a>
        </html>
        '''

        url = crawler.extract_download_url(html)

        assert "download/BhavCopy/Equity/EQ010125_CSV.ZIP" in url

    def test_extract_download_url_missing_element(self, crawler):
        """Test URL extraction with missing element."""
        html = '<html><body>No download link</body></html>'

        with pytest.raises(ParseError, match="Download link element"):
            crawler.extract_download_url(html)

    def test_extract_download_url_empty_href(self, crawler):
        """Test URL extraction with empty href."""
        html = '<html><a id="ContentPlaceHolder1_btnhylZip" href="">Download</a></html>'

        with pytest.raises(ParseError, match="href attribute is empty"):
            crawler.extract_download_url(html)

    @patch('crawler.requests.Session')
    def test_download_zip_file_success(self, mock_session, crawler):
        """Test successful ZIP download."""
        # Create a valid ZIP file in memory
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, 'w') as zf:
            zf.writestr('test.csv', 'test data')
        zip_content = zip_buffer.getvalue()

        mock_response = Mock()
        mock_response.headers = {'content-length': str(len(zip_content))}
        mock_response.iter_content = Mock(return_value=[zip_content])
        mock_response.raise_for_status = Mock()

        crawler.session.get = Mock(return_value=mock_response)

        zip_file = crawler.download_zip_file("http://example.com/test.zip")

        assert isinstance(zip_file, ZipFile)
        assert 'test.csv' in zip_file.namelist()

    @patch('crawler.requests.Session')
    def test_download_zip_file_too_large(self, mock_session, crawler):
        """Test download with file too large."""
        mock_response = Mock()
        mock_response.headers = {'content-length': str(crawler.MAX_FILE_SIZE + 1)}
        mock_response.raise_for_status = Mock()

        crawler.session.get = Mock(return_value=mock_response)

        with pytest.raises(DownloadError, match="File too large"):
            crawler.download_zip_file("http://example.com/huge.zip")

    @patch('crawler.requests.Session')
    def test_download_zip_file_invalid_zip(self, mock_session, crawler):
        """Test download with invalid ZIP."""
        mock_response = Mock()
        mock_response.headers = {}
        mock_response.iter_content = Mock(return_value=[b'not a zip file'])
        mock_response.raise_for_status = Mock()

        crawler.session.get = Mock(return_value=mock_response)

        with pytest.raises(DownloadError, match="Invalid ZIP file"):
            crawler.download_zip_file("http://example.com/invalid.zip")

    @patch('crawler.tempfile.mkdtemp')
    @patch('builtins.open', new_callable=mock_open)
    def test_extract_csv_from_zip_success(self, mock_file, mock_mkdtemp, crawler):
        """Test successful CSV extraction."""
        mock_mkdtemp.return_value = '/tmp/test'

        # Create a valid ZIP with CSV
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, 'w') as zf:
            zf.writestr('EQ010125.CSV', 'csv data')

        zip_file = ZipFile(zip_buffer)

        csv_path = crawler.extract_csv_from_zip(zip_file)

        assert isinstance(csv_path, Path)
        assert csv_path.name == 'EQ010125.CSV'

    def test_extract_csv_from_zip_empty(self, crawler):
        """Test extraction from empty ZIP."""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, 'w') as zf:
            pass  # Empty ZIP

        zip_file = ZipFile(zip_buffer)

        with pytest.raises(ParseError, match="ZIP file is empty"):
            crawler.extract_csv_from_zip(zip_file)

    def test_extract_csv_from_zip_no_csv(self, crawler):
        """Test extraction from ZIP without CSV."""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, 'w') as zf:
            zf.writestr('data.txt', 'not a csv')

        zip_file = ZipFile(zip_buffer)

        with pytest.raises(ParseError, match="No CSV file found"):
            crawler.extract_csv_from_zip(zip_file)

    def test_extract_csv_from_zip_path_traversal(self, crawler):
        """Test protection against path traversal attack."""
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, 'w') as zf:
            zf.writestr('../../../etc/passwd.csv', 'malicious')

        zip_file = ZipFile(zip_buffer)

        with pytest.raises(ParseError, match="Suspicious file path"):
            crawler.extract_csv_from_zip(zip_file)

    @patch('builtins.open', new_callable=mock_open, read_data='SC_CODE,SC_NAME,OPEN,HIGH,LOW,CLOSE\n500325,RELIANCE,2450,2480,2440,2470\n')
    def test_process_csv_to_db_success(self, mock_file, crawler, mock_redis_client):
        """Test successful CSV processing."""
        csv_path = Path('/tmp/test.csv')

        stats = crawler.process_csv_to_db(csv_path)

        assert stats['processed'] >= 0
        assert stats['errors'] == 0

    @patch('builtins.open', new_callable=mock_open, read_data='')
    def test_process_csv_to_db_empty_file(self, mock_file, crawler):
        """Test processing empty CSV."""
        csv_path = Path('/tmp/empty.csv')

        with pytest.raises(DataProcessingError):
            crawler.process_csv_to_db(csv_path)

    @patch('builtins.open', new_callable=mock_open, read_data='WRONG,HEADERS\n1,2\n')
    def test_process_csv_to_db_wrong_headers(self, mock_file, crawler):
        """Test processing CSV with wrong headers."""
        csv_path = Path('/tmp/wrong.csv')

        with pytest.raises(DataProcessingError, match="Missing required columns"):
            crawler.process_csv_to_db(csv_path)

    @patch('pathlib.Path.exists')
    @patch('pathlib.Path.unlink')
    def test_cleanup_file_success(self, mock_unlink, mock_exists, crawler):
        """Test successful file cleanup."""
        mock_exists.return_value = True
        file_path = Path('/tmp/test.csv')

        crawler.cleanup_file(file_path)

        mock_unlink.assert_called_once()

    @patch('pathlib.Path.exists')
    def test_cleanup_file_not_exists(self, mock_exists, crawler):
        """Test cleanup of non-existent file."""
        mock_exists.return_value = False
        file_path = Path('/tmp/nonexistent.csv')

        # Should not raise error
        crawler.cleanup_file(file_path)

    def test_close(self, crawler):
        """Test crawler cleanup."""
        crawler.session = MagicMock()
        crawler.close()
        crawler.session.close.assert_called_once()
