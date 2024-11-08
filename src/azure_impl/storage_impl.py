import config
import os
import logging
import datetime
import azure.batch.models as batchmodels
from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas
)
from azure.core.exceptions import ResourceExistsError


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


logger_blob = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
logger_blob.disabled = True


BLOB_SERVICE_CLIENT =  BlobServiceClient(
        account_url=f"https://{config.STORAGE_ACCOUNT_NAME}.{config.STORAGE_ACCOUNT_DOMAIN}/",
        credential=config.STORAGE_ACCOUNT_KEY,
        logger=logger_blob
    )


def create_container_if_not_exists(container_name: str):
    """
    Creates a container if it does not already exist.

    Args:
        container_name (str): The name of the container to create.

    Returns:
        None
    """
    try:
        BLOB_SERVICE_CLIENT.create_container(container_name)
        logger.info(f'Container [{container_name}] created.')
    except ResourceExistsError:
        logger.info(f'Container [{container_name}] already exists.')


def upload_file_to_container(container_name: str, file_path: str) -> batchmodels.ResourceFile:
    """
    Uploads a file to the specified container.

    Args:
        container_name (str): The name of the container to upload the file to.
        file_path (str): The path of the file to upload.

    Returns:
        batchmodels.ResourceFile: The resource file with the SAS URL.
    """
    blob_name = os.path.basename(file_path)
    blob_client = BLOB_SERVICE_CLIENT.get_blob_client(container_name, blob_name)

    logger.info(f'Uploading file {file_path} to container [{container_name}]...')

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    sas_token = generate_blob_sas(
        config.STORAGE_ACCOUNT_NAME,
        container_name,
        blob_name,
        account_key=config.STORAGE_ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)
    )

    sas_url = generate_sas_url(
        config.STORAGE_ACCOUNT_NAME,
        config.STORAGE_ACCOUNT_DOMAIN,
        container_name,
        blob_name,
        sas_token
    )

    return batchmodels.ResourceFile(
        http_url=sas_url,
        file_path=blob_name
    )


def generate_sas_url(
    account_name: str,
    account_domain: str,
    container_name: str,
    blob_name: str,
    sas_token: str
) -> str:
    """
    Generates a SAS URL for the specified blob.

    Args:
        account_name (str): The storage account name.
        account_domain (str): The storage account domain.
        container_name (str): The name of the container.
        blob_name (str): The name of the blob.
        sas_token (str): The SAS token.

    Returns:
        str: The generated SAS URL.
    """
    return f"https://{account_name}.{account_domain}/{container_name}/{blob_name}?{sas_token}"


def get_file_from_container(container_name: str, blob_name: str, download_path: str) -> bytes:
    """
    Downloads a file from the specified container.

    Args:
        container_name (str): The name of the container.
        blob_name (str): The name of the blob to download.
        download_path (str): The path to download the file to.

    Returns:
        bytes: The downloaded file content.
    """
    blob_client = BLOB_SERVICE_CLIENT.get_blob_client(container_name, blob_name)

    with open(download_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    logger.info(f'Blob {blob_name} downloaded to {download_path}.')
