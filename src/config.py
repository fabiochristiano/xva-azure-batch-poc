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

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

"""
Configure Batch and Storage Account credentials
Create a .env FIle in the src directory
"""

BATCH_ACCOUNT_NAME = os.getenv('BATCH_ACCOUNT_NAME')  # Your batch account name
BATCH_ACCOUNT_KEY = os.getenv('BATCH_ACCOUNT_KEY')  # Your batch account key
BATCH_ACCOUNT_URL = os.getenv('BATCH_ACCOUNT_URL')  # Your batch account URL
STORAGE_ACCOUNT_NAME = os.getenv('STORAGE_ACCOUNT_NAME')
STORAGE_ACCOUNT_KEY = os.getenv('STORAGE_ACCOUNT_KEY')
STORAGE_ACCOUNT_DOMAIN = 'blob.core.windows.net' # Your storage account blob service domain

POOL_ID = 'xva-pool'  # Your Pool ID
POOL_NODE_COUNT = 2  # Pool node count
POOL_VM_SIZE = 'STANDARD_D2_v3'  # VM Type/Size
JOB_ID = 'xva-job'  # Job ID
STANDARD_OUT_FILE_NAME = 'stdout.txt'  # Standard Output file
APP_ID = "montecarlo_app"  # Application ID
