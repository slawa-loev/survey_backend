from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
from dotenv import load_dotenv

# loads configurations from .env ("dotenv") file
load_dotenv()

def fetch_client():
    """Initiates BigQuery client."""
    key_path=os.getenv('KEY_PATH')
    project = os.getenv('GCP_PROJECT_ID')
    credentials = service_account.Credentials.from_service_account_file( # creates authentication credentials for client
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

    return bigquery.Client(project=project, credentials=credentials) # connect to client and returns it

def validate_table_name(table_name: str=os.getenv('GBQ_TABLE')) -> str:
    """Validates table name to prevent SQL injection attacks."""
    if not table_name[0] == "`":
        print("` missing at the start of table name. Adding ` at the start.")
        valid_table_name = "`" + table_name
        table_name = valid_table_name

    if not table_name[-1] == "`":
        print("` missing at the end of table name. Adding ` at the end.")
        valid_table_name = table_name + "`"
        table_name = valid_table_name

    return table_name
