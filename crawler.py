"""
BSE Bhavcopy crawler with robust error handling and security.
"""
import os
import csv
import tempfile
from pathlib import Path
from typing import Dict, Optional
from io import BytesIO
from zipfile import ZipFile, BadZipFile
import time

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup

from config import settings
from logger import get_logger
from redis_client import RedisClient

logger = get_logger(__name__)


class CrawlerError(Exception):
    """Base exception for crawler errors."""
    pass


class DownloadError(CrawlerError):
    """Raised when download fails."""
    pass


class ParseError(CrawlerError):
    """Raised when parsing fails."""
    pass


class DataProcessingError(CrawlerError):
    """Raised when data processing fails."""
    pass


class BSEBhavcopyCrawler:
    """
    Crawler for BSE Bhavcopy data with improved reliability and security.
    """

    # CSV field mapping
    FIELD_CSV_MAP = {
        'code': 'SC_CODE',
        'name': 'SC_NAME',
        'open': 'OPEN',
        'high': 'HIGH',
        'low': 'LOW',
        'close': 'CLOSE'
    }

    # Allowed CSV file extensions (security)
    ALLOWED_EXTENSIONS = {'.csv', '.CSV'}

    # Maximum file size (10MB) to prevent memory exhaustion
    MAX_FILE_SIZE = 10 * 1024 * 1024

    def __init__(self, redis_client: RedisClient):
        """
        Initialize crawler.

        Args:
            redis_client: RedisClient instance for data storage
        """
        self.redis_client = redis_client
        self.session = self._create_session()
        logger.info("crawler_initialized")

    def _create_session(self) -> requests.Session:
        """
        Create requests session with retry logic and timeout.

        Returns:
            Configured requests session
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=settings.max_retries,
            backoff_factor=settings.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set default timeout
        session.request = lambda *args, **kwargs: (
            requests.Session.request(
                session,
                *args,
                timeout=kwargs.pop('timeout', settings.request_timeout),
                **kwargs
            )
        )

        return session

    def fetch_download_page(self) -> str:
        """
        Fetch the BSE Bhavcopy download page.

        Returns:
            HTML content of the page

        Raises:
            DownloadError: If page fetch fails
        """
        url = f"{settings.bse_base_url}{settings.bse_homepage_path}"

        try:
            logger.info("fetching_download_page", url=url)
            response = self.session.get(url)
            response.raise_for_status()

            logger.info(
                "download_page_fetched",
                status_code=response.status_code,
                content_length=len(response.content)
            )

            return response.text

        except requests.RequestException as e:
            logger.error("download_page_fetch_failed", error=str(e), url=url)
            raise DownloadError(f"Failed to fetch download page: {e}")

    def extract_download_url(self, page_html: str) -> str:
        """
        Extract download URL from page HTML.

        Args:
            page_html: HTML content of the download page

        Returns:
            Full download URL

        Raises:
            ParseError: If URL extraction fails
        """
        try:
            logger.info("parsing_download_page")
            soup = BeautifulSoup(page_html, 'html.parser')

            download_element = soup.find(id=settings.bse_download_link_id)

            if not download_element:
                raise ParseError(
                    f"Download link element '{settings.bse_download_link_id}' not found"
                )

            download_path = download_element.get('href')

            if not download_path:
                raise ParseError("Download link href attribute is empty")

            # Handle relative URLs
            if download_path.startswith('http'):
                download_url = download_path
            else:
                download_url = f"{settings.bse_base_url}{download_path}"

            logger.info("download_url_extracted", url=download_url)
            return download_url

        except Exception as e:
            logger.error("download_url_extraction_failed", error=str(e))
            raise ParseError(f"Failed to extract download URL: {e}")

    def download_zip_file(self, url: str) -> ZipFile:
        """
        Download and validate ZIP file.

        Args:
            url: URL of the ZIP file

        Returns:
            ZipFile object

        Raises:
            DownloadError: If download or validation fails
        """
        try:
            logger.info("downloading_zip", url=url)
            response = self.session.get(url, stream=True)
            response.raise_for_status()

            # Check content length
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > self.MAX_FILE_SIZE:
                raise DownloadError(
                    f"File too large: {content_length} bytes "
                    f"(max: {self.MAX_FILE_SIZE})"
                )

            # Download content
            content = BytesIO()
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                downloaded += len(chunk)
                if downloaded > self.MAX_FILE_SIZE:
                    raise DownloadError(
                        f"Downloaded size exceeds limit: {self.MAX_FILE_SIZE}"
                    )
                content.write(chunk)

            content.seek(0)

            # Validate ZIP file
            try:
                zip_file = ZipFile(content)
                # Test ZIP integrity
                zip_file.testzip()

                logger.info(
                    "zip_downloaded",
                    size=downloaded,
                    files=len(zip_file.namelist())
                )

                return zip_file

            except BadZipFile as e:
                raise DownloadError(f"Invalid ZIP file: {e}")

        except requests.RequestException as e:
            logger.error("zip_download_failed", error=str(e), url=url)
            raise DownloadError(f"Failed to download ZIP: {e}")

    def extract_csv_from_zip(self, zip_file: ZipFile) -> Path:
        """
        Safely extract CSV file from ZIP archive.

        Args:
            zip_file: ZipFile object

        Returns:
            Path to extracted CSV file

        Raises:
            ParseError: If extraction fails
        """
        try:
            file_list = zip_file.namelist()

            if not file_list:
                raise ParseError("ZIP file is empty")

            # Find CSV file
            csv_filename = None
            for filename in file_list:
                if Path(filename).suffix in self.ALLOWED_EXTENSIONS:
                    csv_filename = filename
                    break

            if not csv_filename:
                raise ParseError(
                    f"No CSV file found in ZIP. Files: {file_list}"
                )

            # Security: Check for path traversal (zip slip vulnerability)
            csv_path = Path(csv_filename)
            if csv_path.is_absolute() or ".." in csv_path.parts:
                raise ParseError(
                    f"Suspicious file path detected: {csv_filename}"
                )

            # Extract to temporary directory
            temp_dir = tempfile.mkdtemp(prefix="bse_")
            extract_path = Path(temp_dir) / csv_path.name

            logger.info(
                "extracting_csv",
                filename=csv_filename,
                destination=str(extract_path)
            )

            # Extract file
            with zip_file.open(csv_filename) as source:
                with open(extract_path, 'wb') as target:
                    target.write(source.read())

            logger.info("csv_extracted", path=str(extract_path))
            return extract_path

        except Exception as e:
            logger.error("csv_extraction_failed", error=str(e))
            raise ParseError(f"Failed to extract CSV: {e}")
        finally:
            zip_file.close()

    def process_csv_to_db(self, csv_path: Path) -> Dict[str, int]:
        """
        Process CSV file and store data in Redis.

        Args:
            csv_path: Path to CSV file

        Returns:
            Dictionary with processing statistics

        Raises:
            DataProcessingError: If processing fails
        """
        stats = {
            'processed': 0,
            'errors': 0,
            'skipped': 0
        }

        try:
            logger.info("processing_csv", path=str(csv_path))

            with open(csv_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)

                # Validate headers
                if not csv_reader.fieldnames:
                    raise DataProcessingError("CSV file has no headers")

                # Check if required CSV columns exist
                missing_cols = set(self.FIELD_CSV_MAP.values()) - set(csv_reader.fieldnames)
                if missing_cols:
                    raise DataProcessingError(
                        f"Missing required columns: {missing_cols}"
                    )

                # Process rows
                for row_num, row in enumerate(csv_reader, start=1):
                    try:
                        # Extract and map fields
                        data = {}
                        for field, csv_col in self.FIELD_CSV_MAP.items():
                            value = row.get(csv_col, '').strip()
                            if not value:
                                logger.warning(
                                    "empty_field",
                                    row=row_num,
                                    field=field
                                )
                                stats['skipped'] += 1
                                break
                            data[field] = value
                        else:
                            # All fields present, create stock
                            self.redis_client.create_stock(data)
                            stats['processed'] += 1

                            # Log progress every 1000 records
                            if stats['processed'] % 1000 == 0:
                                logger.info(
                                    "processing_progress",
                                    processed=stats['processed']
                                )

                    except Exception as e:
                        logger.error(
                            "row_processing_failed",
                            row=row_num,
                            error=str(e)
                        )
                        stats['errors'] += 1

            logger.info("csv_processing_completed", **stats)
            return stats

        except Exception as e:
            logger.error("csv_processing_failed", error=str(e))
            raise DataProcessingError(f"Failed to process CSV: {e}")

    def cleanup_file(self, file_path: Path) -> None:
        """
        Safely delete temporary file and its directory.

        Args:
            file_path: Path to file to delete
        """
        try:
            if file_path.exists():
                file_path.unlink()
                logger.info("file_deleted", path=str(file_path))

                # Remove parent directory if empty
                parent_dir = file_path.parent
                if parent_dir.exists() and not any(parent_dir.iterdir()):
                    parent_dir.rmdir()
                    logger.info("directory_deleted", path=str(parent_dir))

        except Exception as e:
            logger.error(
                "cleanup_failed",
                path=str(file_path),
                error=str(e)
            )

    def run(self) -> Dict[str, any]:
        """
        Execute complete crawling workflow.

        Returns:
            Dictionary with execution results

        Raises:
            CrawlerError: If any step fails
        """
        start_time = time.time()
        csv_path = None

        try:
            logger.info("crawler_run_started")

            # Step 1: Clear old data
            logger.info("clearing_old_data")
            deleted = self.redis_client.clear_all_data()
            logger.info("old_data_cleared", keys_deleted=deleted)

            # Step 2: Fetch download page
            page_html = self.fetch_download_page()

            # Step 3: Extract download URL
            download_url = self.extract_download_url(page_html)

            # Step 4: Download ZIP file
            zip_file = self.download_zip_file(download_url)

            # Step 5: Extract CSV
            csv_path = self.extract_csv_from_zip(zip_file)

            # Step 6: Process CSV to database
            stats = self.process_csv_to_db(csv_path)

            duration = time.time() - start_time

            result = {
                'success': True,
                'duration': round(duration, 2),
                'stocks_processed': stats['processed'],
                'errors': stats['errors'],
                'skipped': stats['skipped'],
                'total_stocks': self.redis_client.get_stock_count()
            }

            logger.info("crawler_run_completed", **result)
            return result

        except CrawlerError as e:
            duration = time.time() - start_time
            logger.error(
                "crawler_run_failed",
                error=str(e),
                duration=round(duration, 2)
            )
            raise

        finally:
            # Always cleanup temporary files
            if csv_path:
                self.cleanup_file(csv_path)

    def close(self):
        """Close session and cleanup resources."""
        self.session.close()
        logger.info("crawler_closed")
