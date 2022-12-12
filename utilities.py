from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


def fetch_client():
    """Initiates BigQuery client."""
    key_path=os.getenv('KEY_PATH')
    project = os.getenv('GCP_PROJECT_ID')
    credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

    # Creating basic structure: dataset in specified project and table

    return bigquery.Client(project=project, credentials=credentials)

def validate_table_name(table_name: str) -> str:
    """Validates table name to prevent SQL injection attacks."""
    if not table_name[0] == "`":
        #raise NameError("` missing at the start of table name. Please add and retry.")
        print("` missing at the start of table name. Adding ` at the start.")
        valid_table_name = "`" + table_name
        table_name = valid_table_name

    if not table_name[-1] == "`":
        #raise NameError("` missing at the end of table name. Please add and retry.")
        print("` missing at the end of table name. Adding ` at the end.")
        valid_table_name = table_name + "`"
        table_name = valid_table_name

    return table_name
