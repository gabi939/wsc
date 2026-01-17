"""
Orchestrator class that scrapes position data and publishes to EventHub.
"""

import asyncio
import logging
from typing import List, Dict

from shared import Scrapper, extract_positions, create_parquet
from producer.producer import PositionProducer

logger = logging.getLogger(__name__)


class ScraperPublisher:
    """
    Orchestrates scraping position data and publishing to EventHub.

    This class coordinates between the Scrapper and PositionProducer
    to fetch job positions, process them, and publish to EventHub.
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
            conn_str: EventHub connection string
            eventhub_name: EventHub name
            scrape_timeout: Timeout for scraping requests in seconds
            scrape_rate_limit: Rate limit delay in seconds between requests
        """
        self.scrape_url = scrape_url
        self.scraper = Scrapper(
            scrape_timeout=scrape_timeout,
            scrape_rate_limit=scrape_rate_limit,
        )
        self.producer = PositionProducer(
            connection_string=connection_string,
            eventhub_name=eventhub_name,
        )
        self.schema = schema

    async def _scrape_positions(self) -> List[Dict[str, str]]:
        """
        Scrape position data from the configured URL.

        Returns:
            List of position dictionaries containing scraped data

        Raises:
            Exception: If scraping fails
        """
        try:
            logger.info(f"Starting to scrape positions from {self.scrape_url}")
            positions = await self.scraper.scrape(
                url=self.scrape_url,
                extractor=extract_positions,
            )
            logger.info(f"Successfully scraped {len(positions)} positions")
            return positions

        except Exception as e:
            logger.error(f"Failed to scrape positions: {e}", exc_info=True)
            raise

    def _publish_positions(self, positions: List[Dict[str, str]]) -> None:
        """
        Convert scraped positions to Parquet and publish to EventHub.

        Args:
            positions: List of position dictionaries to publish

        Raises:
            Exception: If publishing fails
        """
        try:
            if not positions:
                logger.warning("No positions to publish")
                return

            logger.info(f"Publishing {len(positions)} positions to EventHub")

            # Create Parquet data
            parquet_data = create_parquet(positions, schema=self.schema)

            if not parquet_data:
                logger.warning("Failed to create Parquet data")
                return

            # Publish to EventHub
            self.producer.publish(parquet_data)
            logger.info(
                f"Successfully published {len(positions)} positions to EventHub"
            )

        except Exception as e:
            logger.error(f"Failed to publish positions: {e}", exc_info=True)
            raise

    async def run(self) -> None:
        """
        Execute the full pipeline: scrape and publish.

        Raises:
            Exception: If any step in the pipeline fails
        """
        try:
            logger.info("Starting ScraperPublisher pipeline")

            positions = await self._scrape_positions()

            self._publish_positions(positions)

            logger.info("ScraperPublisher pipeline completed successfully")

        except Exception as e:
            logger.error(f"ScraperPublisher pipeline failed: {e}", exc_info=True)
            raise


async def main():
    """Main entry point for the ScraperPublisher."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Initialize with example URL (update as needed)
    publisher = ScraperPublisher(
        scrape_url="https://careers.wscsports.com/positions",  # Update with actual URL
        scrape_timeout=30,
        scrape_rate_limit=1,
    )

    # Run the pipeline
    await publisher.run()


if __name__ == "__main__":
    asyncio.run(main())
