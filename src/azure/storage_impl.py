from azure.storage.blob import BlobServiceClient
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

# Configuração básica do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Remove blob logging
logger_blob = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
logger_blob.disabled = True


def create_blob_service_client() -> BlobServiceClient:
    """
    Creates and returns a BlobServiceClient.
    """
    return BlobServiceClient(
        account_url=f"https://{config.STORAGE_ACCOUNT_NAME}.{config.STORAGE_ACCOUNT_DOMAIN}/",
        credential=config.STORAGE_ACCOUNT_KEY,
        logger=logger_blob
    )


def create_container_if_not_exists(container_name: str):
    """
    Creates a container if it does not already exist.

    :param blob_service_client: A Blob service client.
    :param str container_name: The name of the container to create.
    """
    
    blob_service_client = create_blob_service_client()
    
    try:
        blob_service_client.create_container(container_name)
        logger.info(f'Container [{container_name}] created.')
    except ResourceExistsError:
        logger.info(f'Container [{container_name}] already exists.')


def upload_file_to_container(container_name: str, file_path: str) -> batchmodels.ResourceFile:
    """
    Uploads a local file to an Azure Blob storage container.

    :param blob_storage_service_client: A blob service client.
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    blob_service_client = create_blob_service_client()
        
    blob_name = os.path.basename(file_path)
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)

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
    Generates and returns a sas url for accessing blob storage
    """
    return f"https://{account_name}.{account_domain}/{container_name}/{blob_name}?{sas_token}"