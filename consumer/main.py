import asyncio
from shared import config
import logging
import sys
from .data_processor import DataProcessor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


async def main():
    processor = DataProcessor(config)
    await processor.run()


if __name__ == "__main__":
    asyncio.run(main())
