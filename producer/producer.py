# producer/app.py
"""
Production-grade producer for position data pipeline.
Scrapes job positions, enriches data, and publishes to Kafka.
"""

import logging
from azure.eventhub import EventHubProducerClient, EventData


logger = logging.getLogger(__name__)


class PositionProducer:
    """Main producer class for position data pipeline."""

    def __init__(self, connection_string: str = None, eventhub_name: str = None):
        self.producer = self._create_producer(
            connection_string=connection_string, eventhub_name=eventhub_name
        )

    def _create_producer(self, connection_string: str, eventhub_name: str):
        try:
            producer = EventHubProducerClient.from_connection_string(
                conn_str=connection_string,
                eventhub_name=eventhub_name,
            )
            return producer
        except Exception as e:
            logger.error(f"Failed to create EventHub producer: {e}")
            raise

    def publish(self, data: bytes):
        try:
            with self.producer:
                self.producer.send_batch([EventData(data)])

        except Exception as e:
            logger.error(f"Error publishing to EventHub: {e}", exc_info=True)
            raise
