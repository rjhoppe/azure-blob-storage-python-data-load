from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from dotenv import load_dotenv
import pandas as pd
import os

'''
Get data out of an Azure Blob storage and into your Python code
'''

# Load creds
load_dotenv()
account_name = os.getenv('account_name')
account_key = os.getenv('account_key')
container_name = os.getenv('container_name')

# Create client connect URL
connect_str = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# Use client connect URL
container_client = blob_service_client.get_container_client(container_name)

# Get list of all blob files in container
blob_file_list = []
for blob_i in container_client.list_blobs():
  blob_file_list.append(blob_i.name)

# Bring CSV files from Azure into Python
# Generate SAS (Shared access sig) token - do this programmatically in Python
for blob_i in blob_file_list:
  sas_i = generate_blob_sas(account_name=account_name,
                            container_name=container_name,
                            blob_name=blob_i,
                            account_key=account_key,
                            permission=BlobSasPermissions(read=True),
                            expiry=datetime.utcnow() + timedelta(hours=1))
  # Convert into URL
  sas_url = f'https://{account_name}.blob.core.windows.net/{container_name}/{blob_i}?{sas_i}'

  # Feed URL into pandas
  df = pd.read_csv(sas_url)
  df_list.append(df)

# Combine them all together
df_combined = pd.concat(df_list, ignore_index=True)
