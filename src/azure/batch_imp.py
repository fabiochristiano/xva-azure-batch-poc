import logging
import azure.batch.models as batchmodels
import config
from azure.batch import BatchServiceClient
import datetime
import sys
import time
from azure.batch.batch_auth import SharedKeyCredentials
import random

logger = logging.getLogger(__name__)

def initialize_batch_client(account_name, account_key, batch_url):
    """
    Initializes the BatchServiceClient.

    :param account_name: The name of the Batch account.
    :param account_key: The key for the Batch account.
    :param batch_url: The URL for the Batch account.
    :return: An instance of BatchServiceClient.
    """
    credentials = SharedKeyCredentials(account_name, account_key)
    return BatchServiceClient(credentials, batch_url)

def create_pool(batch_service_client: BatchServiceClient, pool_id: str):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :param str pool_id: An ID for the new pool.
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
    

def add_tasks(batch_service_client: BatchServiceClient, job_id: str, resource_input_files: list, timestap: int):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :param str job_id: The ID of the job to which to add the tasks.
    :param list resource_input_files: A collection of input files. One task will be
     created for each input file.
    """

    #Application variables
    application_id=config.APP_ID
    application_version = get_lastest_version_batch_application(batch_service_client, application_id)
    application_package_references= [batchmodels.ApplicationPackageReference(
            application_id=application_id,
            version=application_version
    )]
        
    #Task command entrypoint
    env_application_package_dir = f'$AZ_BATCH_APP_PACKAGE_{application_id}_{application_version.replace('.', '_')}'
    sleep = random.uniform(2, 4)
    command_line = f"/bin/bash -c 'sh {env_application_package_dir}/{config.APP_NAME} $$ {sleep}'"


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