from azure.storage.blob import BlobServiceClient
import logging
import json


logger = logging.getLogger(__name__)


class StorageClient:
    """
    Azure Blob Storage client for uploading position data and metadata.

    Handles uploading of parquet data files and their associated JSON metadata
    to Azure Blob Storage containers.
    """

    def __init__(self, connection_string: str):
        """
        Initialize the Storage client.

        Args:
            connection_string (str): Azure Blob Storage connection string

        Raises:
            Exception: If client creation fails
        """
        logger.info("Initializing StorageClient")
        self.client = self._create_client(connection_string=connection_string)
        logger.debug("StorageClient initialized successfully")

    def _create_client(self, connection_string: str) -> BlobServiceClient:
        """
        Create Azure Blob Service client.

        Args:
            connection_string (str): Azure connection string

        Returns:
            BlobServiceClient: Configured blob service client

        Raises:
            Exception: If client creation fails
        """
        try:
            logger.debug("Creating BlobServiceClient")
            client = BlobServiceClient.from_connection_string(connection_string)
            logger.info("BlobServiceClient created successfully")
            return client

        except Exception as e:
            logger.error(f"Failed to create BlobServiceClient: {e}", exc_info=True)
            raise

    def upload(
        self, data: bytes, meta: dict, container_name: str, filename: str
    ) -> None:
        """
        Upload data and metadata to Azure Blob Storage.

        Uploads two blobs:
        1. {filename} - The raw parquet data
        2. {filename}_meta.json - The metadata as JSON

        Args:
            data (bytes): Raw parquet data to upload
            meta (dict): Metadata dictionary to upload as JSON
            container_name (str): Target container name
            filename (str): Base filename (without extension)

        Raises:
            Exception: If upload fails for either blob

        Note:
            Both uploads use overwrite=True to replace existing files
        """
        logger.info(f"Uploading to container={container_name}, filename={filename}")

        try:
            # Upload parquet data
            data_blob_name = f"{filename}"
            logger.debug(f"Uploading data blob: {data_blob_name} ({len(data)} bytes)")

            blob_client = self.client.get_blob_client(
                container=container_name, blob=data_blob_name
            )
            blob_client.upload_blob(data, overwrite=True)
            logger.info(f"Data blob uploaded: {data_blob_name}")

            # Upload metadata
            meta_blob_name = f"{filename}_meta.json"
            meta_json = json.dumps(meta)
            logger.debug(
                f"Uploading metadata blob: {meta_blob_name} ({len(meta_json)} bytes)"
            )

            blob_client = self.client.get_blob_client(
                container=container_name, blob=meta_blob_name
            )
            blob_client.upload_blob(meta_json, overwrite=True)
            logger.info(f"Metadata blob uploaded: {meta_blob_name}")

            logger.info(f"Upload complete for {filename}")

        except Exception as e:
            logger.error(f"Error uploading to Azure Blob Storage: {e}", exc_info=True)
            raise
