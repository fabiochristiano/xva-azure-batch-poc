"""
Create a pool of nodes to output text files from azure blob storage.
Using https://learn.microsoft.com/en-us/azure/batch/quick-run-python doc.
"""

import datetime
import logging

import azure.batch.models as batchmodels

import config, azure_impl.storage_impl as storage_impl, azure_impl.batch_impl as batch_impl

# Configuração básica do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_batch_process(input_file_paths):
    start_time = datetime.datetime.now().replace(microsecond=0)
    logger.info(f'Sample start: {start_time}')
    print()

    # Use the blob client to create the containers in Azure Storage if they don't yet exist.
    input_container_name = 'temp'
    storage_impl.create_container_if_not_exists(input_container_name)  # Use the new function
    
    # Upload the data files.
    logger.info('Uploading files to Azure Storage')
    input_files = [
        storage_impl.upload_file_to_container(input_container_name, file_path)
        for file_path in input_file_paths]
    print()

    try:
        timestap  = int(datetime.datetime.now().timestamp())
        
        #pool_id = f'{config.POOL_ID}-{int(datetime.datetime.now().timestamp())}'
        #job_id = f'{config.JOB_ID}-{int(datetime.datetime.now().timestamp())}'
        pool_id = f'{config.POOL_ID}'
        job_id = f'{config.JOB_ID}'
        
        # Create the pool that will contain the compute nodes that will execute the tasks.        
        batch_impl.create_pool(pool_id)

        # Create the job that will run the tasks.
        batch_impl.create_job(job_id, pool_id)

        # Add the tasks to the job.
        batch_impl.add_tasks(job_id, input_files, timestap)

        # Pause execution until tasks reach Completed state.
        batch_impl.wait_for_tasks_to_complete(job_id, datetime.timedelta(minutes=30))
        
        print()
        logger.info("Success! All tasks reached the 'Completed' state within the specified timeout period.")
        print()

        # Print the stdout.txt and stderr.txt files for each task to the console
        batch_impl.print_task_output(job_id, timestap)

        # Print out some timing info
        end_time = datetime.datetime.now().replace(microsecond=0)
        logger.info(f'Sample end: {end_time}')
        elapsed_time = end_time - start_time
        logger.info(f'Elapsed time: {elapsed_time}')
        print()

    except batchmodels.BatchErrorException as err:
        batch_impl.print_batch_exception(err)
        raise

if __name__ == '__main__':
    run_batch_process()