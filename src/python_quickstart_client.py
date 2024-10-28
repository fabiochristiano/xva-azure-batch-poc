"""
Create a pool of nodes to output text files from azure blob storage.
Using https://learn.microsoft.com/en-us/azure/batch/quick-run-python doc.
"""

import datetime
import os
import sys
import time
import random
import logging

from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas
)
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels
from azure.core.exceptions import ResourceExistsError

import config

# Configuração básica do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Remove blob logging
logger_blob = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
logger_blob.disabled = True


def upload_file_to_container(blob_storage_service_client: BlobServiceClient,
                             container_name: str, file_path: str) -> batchmodels.ResourceFile:
    """
    Uploads a local file to an Azure Blob storage container.

    :param blob_storage_service_client: A blob service client.
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    blob_name = os.path.basename(file_path)
    blob_client = blob_storage_service_client.get_blob_client(container_name, blob_name)

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


def create_pool(batch_service_client: BatchServiceClient, pool_id: str):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
    """
    logger.info(f'Creating pool [{pool_id}]...')

    new_pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="canonical",
                offer="0001-com-ubuntu-server-focal",
                sku="20_04-lts",
                version="latest"
            ),
            node_agent_sku_id="batch.node.ubuntu 20.04"),
        vm_size=config.POOL_VM_SIZE,
        target_dedicated_nodes=config.POOL_NODE_COUNT
    )
    
    try:
        batch_service_client.pool.add(new_pool)
    except batchmodels.BatchErrorException as err:
        if err.error.code == "PoolExists":
            logger.info(f"Pool already exists.")
            pass
        else:
            raise
    finally:
        print()


def create_job(batch_service_client: BatchServiceClient, job_id: str, pool_id: str):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    logger.info(f'Creating job [{job_id}]...')

    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id))
    
    try:
        batch_service_client.job.add(job)
    except batchmodels.BatchErrorException as err:
        if err.error.code == "JobExists":
            logger.info(f"Job already exists.")
            pass
        else:
            raise
    finally:
        print()


def list_batch_application(batch_service_client: BatchServiceClient):
    try:        
        applications = batch_service_client.application.list()
        logger.info(f'Applications')
        for app in applications:
            print(f'Application Id = {app.id} - Versions: {app.versions} - Lastest Version: {app.versions[-1]}')
    except Exception as e:
        print(f"Erro ao listar as aplicações: {e}")
        raise
    
     
def get_lastest_version_batch_application(batch_service_client: BatchServiceClient, application_id: str):
    try:        
        application = batch_service_client.application.get(application_id)
        logger.info('Application Details')
        logger.info(f'Application Id = {application.id} - Versions: {application.versions} - Lastest Version: {application.versions[-1]}')
        print()
        
        return application.versions[-1]
    except Exception as e:
        logger.info(f"Erro ao obter a aplicação: {e}")
        raise
    

def add_tasks(batch_service_client: BatchServiceClient, job_id: str, resource_input_files: list, timestap: int, application_package_references: batchmodels.ApplicationPackageReference, command_line: str):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :param str job_id: The ID of the job to which to add the tasks.
    :param list resource_input_files: A collection of input files. One task will be
     created for each input file.
    """

    logger.info(f'Creating tasks to job [{job_id}]...')

    tasks = []

    for idx, input_file in enumerate(resource_input_files):
        
        #command_line = f"/bin/bash -c \"cat {input_file.file_path} && sleep {sleep}\""
        id_task=f'Task-{timestap}-{idx}'        
        tasks.append(batchmodels.TaskAddParameter(
                id=id_task,
                command_line=command_line,
                resource_files=[input_file],
                application_package_references=application_package_references
            )
        )
        logger.info(f'Created tasks [{id_task}]')
   
    batch_service_client.task.add_collection(job_id, tasks)
    print()


def wait_for_tasks_to_complete(batch_service_client: BatchServiceClient, job_id: str,
                               timeout: datetime.timedelta):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :param job_id: The id of the job whose tasks should be to monitored.
    :param timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout

    print(f"Monitoring all tasks for 'Completed' state, timeout in {timeout}...", end='')

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True

        time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))


def print_task_output(batch_service_client: BatchServiceClient, job_id: str, timestap: str, text_encoding: str=None):
    """
    Prints the stdout.txt file for each task in the job.

    :param batch_client: The batch client to use.
    :param str job_id: The id of the job with task output files to print.
    """

    logger.info('Printing task output...')

    tasks = batch_service_client.task.list(job_id)

    for task in tasks:

        node_id = batch_service_client.task.get(job_id, task.id).node_info.node_id
        if str(timestap) in task.id:
            logger.info(f"Task: {task.id}")
            logger.info(f"Node: {node_id}")
            print()
            
def print_batch_exception(batch_exception: batchmodels.BatchErrorException):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    logger.error('-------------------------------------------')
    logger.error('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        logger.error(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                logger.error(f'{mesg.key}:\t{mesg.value}')
    logger.error('-------------------------------------------')


if __name__ == '__main__':
        
    start_time = datetime.datetime.now().replace(microsecond=0)
    logger.info(f'Sample start: {start_time}')
    print()

    # Create the blob client, for use in obtaining references to blob storage containers and uploading files to containers.
    blob_service_client = BlobServiceClient(
        account_url=f"https://{config.STORAGE_ACCOUNT_NAME}.{config.STORAGE_ACCOUNT_DOMAIN}/",
        credential=config.STORAGE_ACCOUNT_KEY,
        logger=logger_blob  
    )

    # Use the blob client to create the containers in Azure Storage if they don't yet exist.
    input_container_name = 'input'
    try:
        blob_service_client.create_container(input_container_name)
    except ResourceExistsError:
        pass
    
    # The collection of data files that are to be processed by the tasks.
    input_file_paths = [os.path.join(sys.path[0], 'files', 'taskdata0.txt'),
                        os.path.join(sys.path[0], 'files', 'taskdata1.txt'),
                        os.path.join(sys.path[0], 'files', 'taskdata2.txt')]

    # Upload the data files.
    logger.info('Uploading files to Azure Storage')
    input_files = [
        upload_file_to_container(blob_service_client, input_container_name, file_path)
        for file_path in input_file_paths]
    print()

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = SharedKeyCredentials(config.BATCH_ACCOUNT_NAME,
        config.BATCH_ACCOUNT_KEY)

    batch_client = BatchServiceClient(
        credentials,
        batch_url=config.BATCH_ACCOUNT_URL)

    try:
        timestap  = int(datetime.datetime.now().timestamp())
        
        #pool_id = f'{config.POOL_ID}-{int(datetime.datetime.now().timestamp())}'
        #job_id = f'{config.JOB_ID}-{int(datetime.datetime.now().timestamp())}'
        pool_id = f'{config.POOL_ID}'
        job_id = f'{config.JOB_ID}'
        
        #Application variables
        application_id=config.APP_ID
        application_version = get_lastest_version_batch_application(batch_client, application_id)
        application_package_references= [batchmodels.ApplicationPackageReference(
                application_id=application_id,
                version=application_version
        )]
        
        #Task command entrypoint
        env_application_package_dir = f'$AZ_BATCH_APP_PACKAGE_{application_id}_{application_version.replace('.', '_')}'
        sleep = random.uniform(2, 4)
        command_line = f"/bin/bash -c 'sh {env_application_package_dir}/{config.APP_NAME} $$ {sleep}'"
        
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.        
        create_pool(batch_client, pool_id)

        # Create the job that will run the tasks.
        create_job(batch_client, job_id, pool_id)

        # Add the tasks to the job.
        add_tasks(batch_client, job_id, input_files, timestap, application_package_references, command_line)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(batch_client, job_id, datetime.timedelta(minutes=30))
        
        print()
        logger.info("Success! All tasks reached the 'Completed' state within the specified timeout period.")
        print()

        # Print the stdout.txt and stderr.txt files for each task to the console
        print_task_output(batch_client, job_id, timestap)

        # Print out some timing info
        end_time = datetime.datetime.now().replace(microsecond=0)
        logger.info(f'Sample end: {end_time}')
        elapsed_time = end_time - start_time
        logger.info(f'Elapsed time: {elapsed_time}')
        print()

    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise