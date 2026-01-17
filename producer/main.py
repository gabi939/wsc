import asyncio
from producer.scraper_publisher import ScraperPublisher
from shared import config
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


async def main():
    publisher = ScraperPublisher(
        **config["eventhub"],
        **config["producer"]["scrapper"],
        schema={"position_title": "string", "index": "string"},
    )
    await publisher.run()


if __name__ == "__main__":
    asyncio.run(main())
