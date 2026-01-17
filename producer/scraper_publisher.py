"""
Orchestrator that scrapes position data and publishes to EventHub.

Example:
    >>> publisher = ScraperPublisher(
    ...     scrape_url="https://careers.company.com/positions",
    ...     connection_string="Endpoint=sb://...",
    ...     eventhub_name="positions",
    ...     scrape_timeout=30,
    ...     scrape_rate_limit=1,
    ...     schema={"id": "string", "title": "string"}
    ... )
    >>> await publisher.run()
"""

import logging
from typing import List, Dict
from datetime import datetime

from shared import Scrapper, extract_positions, create_parquet
from producer.producer import PositionProducer


logger = logging.getLogger(__name__)


class ScraperPublisher:
    """
    Orchestrates scraping job positions and publishing to EventHub.

    Coordinates scraping, Parquet conversion, and EventHub publishing.
    """

    def __init__(
        self,
        scrape_url: str,
        connection_string: str,
        eventhub_name: str,
        scrape_timeout: int,
        scrape_rate_limit: int,
        schema: Dict[str, str],
    ):
        """
        Initialize ScraperPublisher.

        Args:
            scrape_url: URL to scrape for position data
            connection_string: EventHub connection string
            eventhub_name: EventHub name
            scrape_timeout: Timeout for scraping requests in seconds
            scrape_rate_limit: Delay between requests in seconds
            schema: Field name to Parquet type mapping
        """
        logger.info("Initializing ScraperPublisher for %s", scrape_url)

        self.scrape_url = scrape_url
        self.schema = schema

        self.scraper = Scrapper(
            scrape_timeout=scrape_timeout,
            scrape_rate_limit=scrape_rate_limit,
        )

        self.producer = PositionProducer(
            connection_string=connection_string,
            eventhub_name=eventhub_name,
        )

        logger.info("ScraperPublisher initialized successfully")

    async def _scrape_positions(self) -> List[Dict[str, str]]:
        """
        Scrape position data from configured URL.

        Returns:
            List of position dictionaries

        Raises:
            Exception: If scraping fails
        """
        logger.info("Scraping positions from %s", self.scrape_url)

        try:
            positions = await self.scraper.scrape(
                url=self.scrape_url,
                extractor=extract_positions,
            )

            logger.info("Scraped %d positions", len(positions))
            return positions

        except Exception as e:
            logger.error("Scraping failed: %s", e, exc_info=True)
            raise

    def _publish_positions(self, positions: List[Dict[str, str]]) -> None:
        """
        Convert positions to Parquet and publish to EventHub.

        Args:
            positions: List of position dictionaries

        Raises:
            Exception: If publishing fails
        """
        if not positions:
            logger.warning("No positions to publish")
            return

        logger.info("Publishing %d positions", len(positions))

        try:
            # Convert to Parquet
            parquet_data = create_parquet(positions, schema=self.schema)

            if not parquet_data:
                logger.error("Failed to create Parquet data")
                raise ValueError("Parquet conversion returned empty data")

            logger.debug("Created Parquet: %.2f KB", len(parquet_data) / 1024)

            # Publish to EventHub
            self.producer.publish(parquet_data)

            logger.info("Successfully published %d positions", len(positions))

        except Exception as e:
            logger.error("Publishing failed: %s", e, exc_info=True)
            raise

    async def run(self) -> None:
        """
        Execute the scrape and publish pipeline.

        Raises:
            Exception: If any step fails
        """
        start_time = datetime.now()
        logger.info("Starting pipeline")

        try:
            # Scrape
            positions = await self._scrape_positions()

            # Publish
            self._publish_positions(positions)

            duration = (datetime.now() - start_time).total_seconds()
            logger.info("Pipeline completed in %.2fs", duration)

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error("Pipeline failed after %.2fs: %s", duration, e, exc_info=True)
            raise

    def close(self) -> None:
        """Close producer connection."""
        logger.info("Closing ScraperPublisher")
        self.producer.close()

    async def __aenter__(self):
        """Context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
