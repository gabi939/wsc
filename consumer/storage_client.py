from azure.storage.blob import BlobServiceClient
import logging
import json


logger = logging.getLogger(__name__)


class StorageClient:
    """Main producer class for position data pipeline."""

    def __init__(
        self,
        connection_string: str = None,
    ):
        self.client = self._create_client(connection_string=connection_string)

    def _create_client(self, connection_string: str):
        try:
            client = BlobServiceClient.from_connection_string(connection_string)
            return client
        except Exception as e:
            logger.error(f"Failed to create BlobServiceClient: {e}")
            raise

    def upload(self, data: bytes, meta: dict, container_name: str, filename: str):
        try:
            blob_client = self.client.get_blob_client(
                container=container_name, blob=f"{filename}"
            )
            blob_client.upload_blob(data, overwrite=True)

            blob_client = self.client.get_blob_client(
                container=container_name, blob=f"{filename}_meta.json"
            )

            blob_client.upload_blob(json.dumps(meta), overwrite=True)
        except Exception as e:
            logger.error(f"Error uploading to Azure Blob Storage: {e}", exc_info=True)
            raise
