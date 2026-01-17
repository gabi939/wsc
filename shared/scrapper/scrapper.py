# producer/scraper.py
"""
Web scraper for WSC Sports careers page with robust error handling.
"""

import asyncio
import logging
from typing import List, Dict, Optional
import aiohttp
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

logger = logging.getLogger(__name__)


class Scrapper:
    def __init__(self, scrape_timeout: int, scrape_rate_limit: int):
        self.scrape_timeout = scrape_timeout
        self.scrape_rate_limit = scrape_rate_limit

        self.session: Optional[aiohttp.ClientSession] = None

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        }

    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.scrape_timeout),
            headers=self.headers,
        )
        return self

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True,
    )
    async def _fetch_page(self, url: str) -> str:
        """Fetch page content with retry logic."""
        if not self.session:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.scrape_timeout),
                headers=self.headers,
            )

        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                content = await response.text()
                logger.debug(f"Successfully fetched {url} ({len(content)} bytes)")
                return content

        except aiohttp.ClientError as e:
            logger.warning(f"HTTP error fetching {url}: {e}")
            raise
        except asyncio.TimeoutError as e:
            logger.warning(f"Timeout fetching {url}")
            raise

        except Exception as e:
            logger.error(f"Unexpected error {e}")
            raise

    async def scrape(self, url: str, extractor) -> List[Dict[str, str]]:
        """Main scraping method."""
        try:
            await asyncio.sleep(self.scrape_rate_limit)

            html = await self._fetch_page(url)

            data = extractor(html)

            if not data:
                logger.warning("No positions found on page")

            return data

        except Exception as e:
            logger.error(f"Scraping failed: {e}")

    async def close(self):
        """Close the aiohttp session."""
        if self.session:
            return await self.session.close()
