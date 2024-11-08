import logging
import azure.batch.models as batchmodels
import config
from azure.batch import BatchServiceClient
import datetime
import sys
import time
from azure.batch.batch_auth import SharedKeyCredentials


logger = logging.getLogger(__name__)

BATCH_CLIENT = BatchServiceClient(
        SharedKeyCredentials(
            config.BATCH_ACCOUNT_NAME, 
            config.BATCH_ACCOUNT_KEY
        ), 
        config.BATCH_ACCOUNT_URL
    )
    

def create_pool(pool_id: str):
    """
    Creates a pool with the specified pool ID.

    Args:
        pool_id (str): The ID of the pool to be created.

    Returns:
        None
    """
    logger.info(f'Creating pool [{pool_id}]...')

    virtual_machine_configuration = batchmodels.VirtualMachineConfiguration(
        image_reference=batchmodels.ImageReference(
            publisher="canonical",
            offer="0001-com-ubuntu-server-focal",
            sku="20_04-lts",
            version="latest"
        ),
        node_agent_sku_id="batch.node.ubuntu 20.04"
    )

    start_task = batchmodels.StartTask(
        command_line= """
/bin/bash -c '
sudo -S apt-get update &&
sudo -S apt-get install -y python3 python3-pip &&
pip3 install numpy azure-storage-blob==12.8.1 &&
env > env.txt &&
python3 --version > python-version.txt
'
""",
        user_identity=batchmodels.UserIdentity(
            auto_user=batchmodels.AutoUserSpecification(
                scope=batchmodels.AutoUserScope.pool,
                elevation_level=batchmodels.ElevationLevel.admin
            ),
        ),
        wait_for_success=True
    )

    application_package_references= [batchmodels.ApplicationPackageReference(
            application_id=config.APP_ID,
            version=get_lastest_version_batch_application(config.APP_ID)
    )]
    
    new_pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=virtual_machine_configuration,
        vm_size=config.POOL_VM_SIZE,
        target_dedicated_nodes=config.POOL_NODE_COUNT,
        start_task=start_task,
        application_package_references=application_package_references
    )
    
    try:
        BATCH_CLIENT.pool.add(new_pool)
    except batchmodels.BatchErrorException as err:
        if err.error.code == "PoolExists":
            logger.info(f"Pool already exists.")
            pass
        else:
            raise
    finally:
        print()
        

def create_job(job_id: str, pool_id: str):
    """
    Creates a job with the specified job ID and associates it with the specified pool ID.

    Args:
        job_id (str): The ID of the job to be created.
        pool_id (str): The ID of the pool to associate with the job.

    Returns:
        None
    """
    logger.info(f'Creating job [{job_id}]...')

    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id))
    
    try:
        BATCH_CLIENT.job.add(job)
    except batchmodels.BatchErrorException as err:
        if err.error.code == "JobExists":
            logger.info(f"Job already exists.")
            pass
        else:
            raise
    finally:
        print()


def list_batch_application():
    """
    Lists all batch applications.

    Returns:
        None
    """
    try:        
        applications = BATCH_CLIENT.application.list()
        logger.info(f'Applications')
        for app in applications:
            print(f'Application Id = {app.id} - Versions: {app.versions} - Lastest Version: {app.versions[-1]}')
    except Exception as e:
        print(f"Erro ao listar as aplicações: {e}")
        raise
    
     
def get_lastest_version_batch_application(application_id: str):
    """
    Gets the latest version of the specified batch application.

    Args:
        application_id (str): The ID of the application.

    Returns:
        str: The latest version of the application.
    """
    try:        
        application = BATCH_CLIENT.application.get(application_id)
        logger.info('Application Details')
        logger.info(f'Application Id = {application.id} - Versions: {application.versions} - Lastest Version: {application.versions[-1]}')
        print()
        
        return application.versions[-1]
    except Exception as e:
        logger.info(f"Erro ao obter a aplicação: {e}")
        raise
    

def add_tasks(job_id: str, resource_input_files: list, timestap: int):
    """
    Adds tasks to the specified job.

    Args:
        job_id (str): The ID of the job.
        resource_input_files (list): The list of input files for the tasks.
        timestap (int): The timestamp to be used in task IDs.

    Returns:
        None
    """
    application_id=config.APP_ID
    application_version = get_lastest_version_batch_application(application_id)
        
    env_application_package_dir = f'$AZ_BATCH_APP_PACKAGE_{application_id}_{application_version.replace(".", "_")}'

    logger.info(f'Creating tasks to job [{job_id}]...')

    tasks = []

    for idx, input_file in enumerate(resource_input_files):
        
        id_task=f'Task-{timestap}-{idx}'        
        tasks.append(batchmodels.TaskAddParameter(
                id=id_task,
                command_line=f"/bin/bash -c 'python3 {env_application_package_dir}/montecarlo_app.py $HOME/{input_file.file_path}'",
                resource_files=[input_file]
            )
        )
        logger.info(f'Created tasks [{id_task}]')
   
    BATCH_CLIENT.task.add_collection(job_id, tasks)
    print()


def wait_for_tasks_to_complete(job_id: str, timeout: datetime.timedelta):
    """
    Waits for all tasks in the specified job to complete within the given timeout period.

    Args:
        job_id (str): The ID of the job.
        timeout (datetime.timedelta): The timeout period.

    Returns:
        bool: True if all tasks completed within the timeout period, otherwise raises an exception.
    """
    timeout_expiration = datetime.datetime.now() + timeout

    print(f"Monitoring all tasks for 'Completed' state, timeout in {timeout}...", end='')

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = BATCH_CLIENT.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True

        time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))


def print_task_output(job_id: str, timestap: str):
    """
    Prints the output of tasks in the specified job.

    Args:
        job_id (str): The ID of the job.
        timestap (str): The timestamp to filter tasks by.

    Returns:
        None
    """
    logger.info('Printing task output...')

    tasks = BATCH_CLIENT.task.list(job_id)

    for task in tasks:
        node_id = BATCH_CLIENT.task.get(job_id, task.id).node_info.node_id
        if str(timestap) in task.id:
            logger.info(f"Task: {task.id}")
            logger.info(f"Node: {node_id}")
            print()
            

def print_batch_exception(batch_exception: batchmodels.BatchErrorException):
    """
    Prints details of the specified batch exception.

    Args:
        batch_exception (batchmodels.BatchErrorException): The batch exception to print.

    Returns:
        None
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


def delete_all_pools():
    """
    Deletes all pools.

    Returns:
        None
    """
    try:
        pools = BATCH_CLIENT.pool.list()
        for pool in pools:
            logger.info(f'Deleting pool [{pool.id}]...')
            BATCH_CLIENT.pool.delete(pool.id)
    except Exception as e:
        logger.error(f"Erro ao deletar os pools: {e}")
        raise


def delete_all_jobs():
    """
    Deletes all jobs.

    Returns:
        None
    """
    try:
        jobs = BATCH_CLIENT.job.list()
        for job in jobs:
            logger.info(f'Deleting job [{job.id}]...')
            BATCH_CLIENT.job.delete(job.id)
    except Exception as e:
        logger.error(f"Erro ao deletar os jobs: {e}")


def main():
    """
    Main function to delete all pools and jobs.

    Returns:
        None
    """
    delete_all_pools()
    delete_all_jobs()

if __name__ == "__main__":
    main()