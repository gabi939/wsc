"""
Orchestrator class that scrapes position data and publishes to EventHub.
"""

import logging
from typing import List, Dict
import uuid

from consumer.events_consumer import EventsConsumer
from shared import Scrapper, extract_job_info, read_parquet

from .storage_client import StorageClient


logger = logging.getLogger(__name__)


class DataProcessor:
    """
    Orchestrates the data processing pipeline: consuming events, scraping job data,
    and uploading to storage.

    The processor:
    1. Consumes position events from EventHub
    2. Scrapes detailed job information from web pages
    3. Uploads scraped data and metadata to Azure Blob Storage

    Attributes:
        consumer (EventsConsumer): EventHub consumer for position events
        scraper (Scrapper): Web scraper for job information
    """

    def __init__(self, config: dict):
        """
        Initialize the DataProcessor with configuration.

        Args:
            config (dict): Configuration dictionary containing:
                - eventhub: EventHub connection parameters
                - consumer: Consumer settings (scrapper, base_url, bucket)
                - storage: Storage client configuration

        Example:
            config = {
                "eventhub": {"connection_string": "...", "eventhub_name": "..."},
                "consumer": {
                    "consumer_details": {"consumer_group": "..."},
                    "scrapper": {"scrape_timeout": 30, "scrape_rate_limit": 1},
                    "base_url": "https://example.com/jobs/",
                    "bucket": "positions-data"
                },
                "storage": {"connection_string": "..."}
            }
        """
        logger.info("Initializing DataProcessor")

        self.consumer = EventsConsumer(
            **config["eventhub"], **config["consumer"]["consumer_details"]
        )
        logger.debug("EventsConsumer initialized")

        self.scraper = Scrapper(**config["consumer"]["scrapper"])
        logger.debug("Scrapper initialized")

        self._storage_client = StorageClient(**config["storage"])
        logger.debug("StorageClient initialized")

        self._base_url = config["consumer"]["base_url"]
        self._bucket = config["consumer"]["bucket"]

        logger.info(
            f"DataProcessor initialized with base_url={self._base_url}, bucket={self._bucket}"
        )

    async def _scrape_job_info(
        self, data: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """
        Scrape detailed job information for each position event.

        Args:
            data (List[Dict[str, str]]): List of position events, each containing
                at minimum a 'position_title' field

        Returns:
            List[Dict[str, str]]: List of scraped metadata for each position

        Raises:
            Exception: If scraping fails for any reason

        Note:
            - URLs are constructed by converting position titles to lowercase
              and replacing spaces with hyphens
            - All positions are processed sequentially to respect rate limits
        """
        logger.info(f"Starting scrape for {len(data)} positions")

        try:
            positions_metadata = []

            for idx, event in enumerate(data):
                position_title = event.get("position_title", "unknown")
                url = f"{self._base_url}{position_title.lower().replace(' ', '-')}/"

                logger.debug(
                    f"Scraping position {idx + 1}/{len(data)}: {position_title} from {url}"
                )

                metadata = await self.scraper.scrape(
                    url=url,
                    extractor=extract_job_info,
                )

                if metadata:
                    positions_metadata.append(metadata)
                    logger.debug(f"Successfully scraped metadata for {position_title}")
                else:
                    logger.warning(f"No metadata extracted for {position_title}")

            logger.info(
                f"Successfully scraped {len(positions_metadata)}/{len(data)} positions"
            )
            return positions_metadata

        except Exception as e:
            logger.error(f"Failed to scrape positions: {e}", exc_info=True)
            raise

    async def run(self) -> None:
        """
        Execute the main data processing pipeline.

        The pipeline:
        1. Consumes position events from EventHub in batches
        2. Reads parquet data for each batch
        3. Scrapes detailed job information
        4. Uploads data and metadata to blob storage

        Raises:
            Exception: If any step in the pipeline fails

        Note:
            The scraper session is always closed in the finally block,
            even if an error occurs during processing.
        """
        try:
            logger.info("Starting DataProcessor pipeline")

            batch_count = 0
            for positions_parquet in self.consumer.consume():
                batch_count += 1
                logger.info(f"Processing batch {batch_count}")

                logger.debug("Reading parquet data")
                data = read_parquet(positions_parquet)
                logger.info(f"Read {len(data)} position records from parquet")

                logger.debug("Scraping job information")
                positions_metadata = await self._scrape_job_info(data)

                file_id = uuid.uuid4()
                logger.info(f"Uploading batch {batch_count} with file_id={file_id}")

                self._storage_client.upload(
                    data=positions_parquet,
                    meta=positions_metadata,
                    container_name=self._bucket,
                    filename=str(file_id),
                )

                logger.info(f"Batch {batch_count} uploaded successfully")

            logger.info(
                f"DataProcessor pipeline completed successfully. Processed {batch_count} batches"
            )

        except Exception as e:
            logger.error(f"DataProcessor pipeline failed: {e}", exc_info=True)
            raise

        finally:
            logger.info("Closing scraper session")
            await self.scraper.close()
