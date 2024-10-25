# -------------------------------------------------------------------------
#
# THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
# ----------------------------------------------------------------------------------
# The example companies, organizations, products, domain names,
# e-mail addresses, logos, people, places, and events depicted
# herein are fictitious. No association with any real company,
# organization, product, domain name, email address, logo, person,
# places, or events is intended or should be inferred.
# --------------------------------------------------------------------------

# Global constant variables (Azure Storage account/Batch details)

# import "config.py" in "python_quickstart_client.py "
# Please note that storing the batch and storage account keys in Azure Key Vault
# is a better practice for Production usage.

"""
Configure Batch and Storage Account credentials
"""

BATCH_ACCOUNT_NAME = 'batchpythonquickstart'  # Your batch account name
BATCH_ACCOUNT_KEY = 'M/GTOJbIReauJEMEe0KhUaPnODm7ACMp83AWQNbQXSD5T0HmLywdQCug/Gwfujuhyw4LfJCb3uHV+ABa/hl9ug=='  # Your batch account key
BATCH_ACCOUNT_URL = 'https://batchpythonquickstart.brazilsouth.batch.azure.com'  # Your batch account URL
STORAGE_ACCOUNT_NAME = 'batchpythonquickstart2'
STORAGE_ACCOUNT_KEY = 'fnxcNTrWDzoXYYNL/Ls7Yvt1CSITogzHD+BCN6bfoMgWbasd8KmVhysVWIOTUNLLubasuZhw6vqr+ASt5R37fw=='
STORAGE_ACCOUNT_DOMAIN = 'blob.core.windows.net' # Your storage account blob service domain

POOL_ID = 'PythonQuickstartPool'  # Your Pool ID
POOL_NODE_COUNT = 2  # Pool node count
POOL_VM_SIZE = 'STANDARD_D2_v3'  # VM Type/Size
JOB_ID = 'PythonQuickstartJob'  # Job ID
STANDARD_OUT_FILE_NAME = 'stdout.txt'  # Standard Output file
APP_ID = "MyBatchApplication"
APP_NAME = "app.sh"
