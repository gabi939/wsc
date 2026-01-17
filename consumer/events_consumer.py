from azure.eventhub import EventHubConsumerClient, EventData, PartitionContext
from shared.parquet_tools import read_parquet


class EventsConsumer:
    def __init__(
        self,
        connection_string,
        eventhub_name,
        consumer_group,
        max_events,
        max_wait_time,
    ):
        self._client = self._build_client(
            connection_string, eventhub_name, consumer_group
        )
        self._max_events = max_events
        self._max_wait_time = max_wait_time
        self.collected_data = []

    def _build_client(self, conn_string, eventhub_name, consumer_group):
        return EventHubConsumerClient.from_connection_string(
            conn_str=conn_string,
            eventhub_name=eventhub_name,
            consumer_group=consumer_group,
        )

    def _consume_callback(self, context, events: list[EventData]):
        for event in events:
            self.collected_data.append(b"".join(event.body))

        # Once we have reached our goal or finished the batch,
        # we can close the client to stop receive_batch from blocking
        if len(self.collected_data) >= self._max_events:
            self._client.close()

    def consume(self):
        self.collected_data = []
        with self._client:
            self._client.receive_batch(
                on_event_batch=self._consume_callback,
                max_batch_size=self._max_events,
                starting_position="-1",  # "-1" is from the beginning of the stream
                max_wait_time=self._max_wait_time,
            )

        return self.collected_data
