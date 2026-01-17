"""
Orchestrator class that scrapes position data and publishes to EventHub.
"""

import logging
from typing import List, Dict

from consumer.events_consumer import EventsConsumer
from shared import Scrapper, extract_job_info, read_parquet

from .storage_client import StorageClient

import uuid


logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self, config: dict):
        self.consumer = EventsConsumer(
            **config["eventhub"], **config["consumer"]["consumer_details"]
        )
        self.scraper = Scrapper(**config["consumer"]["scrapper"])

        self._storage_client = StorageClient(**config["storage"])
        self._base_url = config["consumer"]["base_url"]
        self._bucket = config["consumer"]["bucket"]

    async def _scrape_job_info(self, data: Dict[str, str]) -> List[Dict[str, str]]:
        try:
            positions_metadata = []
            for event in data:
                url = f"{self._base_url}{event['position_title'].lower().replace(' ', '-')}/"
                metadata = await self.scraper.scrape(
                    url=url,
                    extractor=extract_job_info,
                )
                positions_metadata.append(metadata)
            logger.info(f"Successfully scraped {len(positions_metadata)} positions")
            return positions_metadata

        except Exception as e:
            logger.error(f"Failed to scrape positions: {e}", exc_info=True)
            raise

    async def run(self) -> None:
        try:
            logger.info("Starting ScraperPublisher pipeline")

            for positions_parquet in self.consumer.consume():
                data = read_parquet(positions_parquet)

                positions_metadata = await self._scrape_job_info(data)

                self._storage_client.upload(
                    data=positions_parquet,
                    meta=positions_metadata,
                    container_name=self._bucket,
                    filename=uuid.uuid4(),
                )

            logger.info("ScraperPublisher pipeline completed successfully")

        except Exception as e:
            logger.error(f"ScraperPublisher pipeline failed: {e}", exc_info=True)
            raise

        finally:
            await self.scraper.close()
