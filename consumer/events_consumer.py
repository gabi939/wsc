from azure.eventhub import EventHubConsumerClient, EventData, PartitionContext
import logging


logger = logging.getLogger(__name__)


class EventsConsumer:
    """
    Azure EventHub consumer for position data events.

    Consumes events in batches from EventHub and collects them as raw bytes.
    The consumer automatically closes after reaching max_events or when the
    batch is complete.

    Attributes:
        collected_data (List[bytes]): Accumulated event data from the current batch
    """

    def __init__(
        self,
        connection_string: str,
        eventhub_name: str,
        consumer_group: str,
        max_events: int,
        max_wait_time: int,
    ):
        """
        Initialize the EventHub consumer.

        Args:
            connection_string (str): Azure EventHub connection string
            eventhub_name (str): Name of the EventHub
            consumer_group (str): Consumer group name (e.g., "$Default")
            max_events (int): Maximum number of events to consume per batch
            max_wait_time (int): Maximum wait time in seconds for events
        """
        logger.info(
            f"Initializing EventsConsumer for eventhub={eventhub_name}, "
            f"consumer_group={consumer_group}, max_events={max_events}"
        )

        self._client = self._build_client(
            connection_string, eventhub_name, consumer_group
        )
        self._max_events = max_events
        self._max_wait_time = max_wait_time
        self.collected_data = []

        logger.debug("EventsConsumer initialized successfully")

    def _build_client(
        self, conn_string: str, eventhub_name: str, consumer_group: str
    ) -> EventHubConsumerClient:
        """
        Build the EventHub consumer client.

        Args:
            conn_string (str): EventHub connection string
            eventhub_name (str): Name of the EventHub
            consumer_group (str): Consumer group name

        Returns:
            EventHubConsumerClient: Configured EventHub consumer client
        """
        logger.debug(f"Building EventHub client for {eventhub_name}")
        return EventHubConsumerClient.from_connection_string(
            conn_str=conn_string,
            eventhub_name=eventhub_name,
            consumer_group=consumer_group,
        )

    def _consume_callback(
        self, context: PartitionContext, events: list[EventData]
    ) -> None:
        """
        Callback function invoked for each batch of events.

        Args:
            context (PartitionContext): Partition context (unused)
            events (List[EventData]): Batch of events to process

        Note:
            - Collects event bodies as bytes
            - Automatically closes client when max_events is reached
        """
        logger.debug(f"Consuming batch of {len(events)} events")

        for event in events:
            event_body = b"".join(event.body)
            self.collected_data.append(event_body)
            logger.debug(f"Collected event ({len(event_body)} bytes)")

        # Close client when we've reached our goal
        if len(self.collected_data) >= self._max_events:
            logger.info(f"Reached max_events ({self._max_events}), closing client")
            self._client.close()

    def consume(self) -> list[bytes]:
        """
        Consume events from EventHub in batches.

        Returns:
            List[bytes]: List of event bodies as raw bytes

        Note:
            - Resets collected_data before consuming
            - Starts from the beginning of the stream (position "-1")
            - Blocks until max_events is reached or max_wait_time expires
        """
        logger.info("Starting event consumption")
        self.collected_data = []

        try:
            with self._client:
                logger.debug(
                    f"Receiving batch (max_batch_size={self._max_events}, "
                    f"max_wait_time={self._max_wait_time}s)"
                )

                self._client.receive_batch(
                    on_event_batch=self._consume_callback,
                    max_batch_size=self._max_events,
                    starting_position="@latest",  # From the beginning of the stream
                    max_wait_time=self._max_wait_time,
                )

            logger.info(
                f"Consumption complete. Collected {len(self.collected_data)} events"
            )

        except Exception as e:
            logger.error(f"Error during event consumption: {e}", exc_info=True)
            raise

        return self.collected_data
