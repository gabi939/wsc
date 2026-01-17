"""
Web scraper with robust error handling and retry logic.
"""

import asyncio
import logging
from typing import List, Dict, Optional, Callable
import aiohttp
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

logger = logging.getLogger(__name__)


class Scrapper:
    """
    Asynchronous web scraper with rate limiting and retry logic.

    Features:
    - Configurable timeout and rate limiting
    - Automatic retry on transient failures
    - Session reuse for better performance
    - Proper cleanup via context manager or explicit close

    Attributes:
        scrape_timeout (int): HTTP request timeout in seconds
        scrape_rate_limit (int): Delay between requests in seconds
        session (Optional[aiohttp.ClientSession]): Reusable HTTP session
    """

    def __init__(self, scrape_timeout: int, scrape_rate_limit: int):
        """
        Initialize the scraper.

        Args:
            scrape_timeout (int): Maximum time to wait for HTTP responses (seconds)
            scrape_rate_limit (int): Minimum delay between requests (seconds)
        """
        logger.info(
            f"Initializing Scrapper (timeout={scrape_timeout}s, "
            f"rate_limit={scrape_rate_limit}s)"
        )

        self.scrape_timeout = scrape_timeout
        self.scrape_rate_limit = scrape_rate_limit
        self.session: Optional[aiohttp.ClientSession] = None

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        }

        logger.debug("Scrapper initialized")

    async def __aenter__(self):
        """
        Async context manager entry - creates HTTP session.

        Returns:
            Scrapper: Self for context manager usage
        """
        logger.debug("Entering Scrapper context, creating session")
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.scrape_timeout),
            headers=self.headers,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - closes HTTP session."""
        logger.debug("Exiting Scrapper context, closing session")
        await self.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True,
    )
    async def _fetch_page(self, url: str) -> str:
        """
        Fetch page content with automatic retry on transient failures.

        Args:
            url (str): URL to fetch

        Returns:
            str: HTML content of the page

        Raises:
            aiohttp.ClientError: On HTTP errors (after retries)
            asyncio.TimeoutError: On timeout (after retries)
            Exception: On unexpected errors

        Note:
            Retries up to 3 times with exponential backoff (2-10 seconds)
        """
        # Create session if not already created
        if not self.session:
            logger.debug("Creating new session in _fetch_page")
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.scrape_timeout),
                headers=self.headers,
            )

        try:
            logger.debug(f"Fetching URL: {url}")
            async with self.session.get(url) as response:
                response.raise_for_status()
                content = await response.text()
                logger.debug(
                    f"Successfully fetched {url} ({len(content)} bytes, "
                    f"status={response.status})"
                )
                return content

        except aiohttp.ClientError as e:
            logger.warning(f"HTTP error fetching {url}: {e}")
            raise

        except asyncio.TimeoutError as e:
            logger.warning(f"Timeout fetching {url} after {self.scrape_timeout}s")
            raise

        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}", exc_info=True)
            raise

    async def scrape(
        self, url: str, extractor: Callable[[str], List[Dict[str, str]]]
    ) -> Optional[List[Dict[str, str]]]:
        """
        Scrape a URL and extract structured data.

        Args:
            url (str): URL to scrape
            extractor (Callable): Function to extract data from HTML
                Should accept HTML string and return list of dicts

        Returns:
            Optional[List[Dict[str, str]]]: Extracted data, or None on error

        Note:
            - Respects rate_limit by sleeping before each request
            - Logs warnings if no data is extracted
            - Returns None instead of raising on extraction errors
        """
        try:
            logger.debug(
                f"Rate limiting: sleeping {self.scrape_rate_limit}s before scrape"
            )
            await asyncio.sleep(self.scrape_rate_limit)

            html = await self._fetch_page(url)
            logger.debug(f"Extracting data from {url}")

            data = extractor(html)

            if not data:
                logger.warning(f"No data extracted from {url}")
            else:
                logger.info(f"Extracted {len(data)} items from {url}")

            return data

        except Exception as e:
            logger.error(f"Scraping failed for {url}: {e}", exc_info=True)
            return None

    async def close(self) -> None:
        """
        Close the HTTP session and clean up resources.

        Should be called when scraping is complete, either manually
        or automatically via context manager.
        """
        if self.session:
            logger.debug("Closing aiohttp session")
            await self.session.close()
            self.session = None
            logger.info("Scrapper session closed")
        else:
            logger.debug("No session to close")
