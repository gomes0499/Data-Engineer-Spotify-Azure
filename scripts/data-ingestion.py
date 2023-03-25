import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.identity import DefaultAzureCredential
import configparser


# Get the full path to the config.ini file
config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/3-Project/config/config.ini")

account_url = "https://wu3storage.blob.core.windows.net"
default_credential = DefaultAzureCredential()

# Create the BlobServiceClient object
blob_service_client = BlobServiceClient(account_url, credential=default_credential)

# Variables
server = config.get("sqlserver", "server")
database = config.get("sqlserver", "database")
username = config.get("sqlserver", "username")
password = config.get("sqlserver", "password")
driver = 'ODBC Driver 18 for SQL Server'
container_name = config.get("blob", "container_name")

conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
conn = pyodbc.connect(conn_str)

tables_to_export = ['Artists', 'ListeningHistory', 'Songs', 'UserPreferences', 'Users']

# Loop through each table and export the data
for table in tables_to_export:
    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, conn)
    
    # Save the DataFrame to a Parquet file in memory
    output_file = table + ".parquet"
    df.to_parquet(output_file)

    # Upload the Parquet file to the Blob storage
    container_name = container_name
    folder_path = "landing/processed"  # Update this with the correct folder path
    blob_name = folder_path + output_file
    blob_service_client = BlobServiceClient(account_url, credential=default_credential)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(output_file, "rb") as data:
        blob_client.upload_blob(data)

# Close the connection
conn.close()
