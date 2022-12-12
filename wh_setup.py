from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

key_path=os.getenv('KEY_PATH')
project = os.getenv('GCP_PROJECT_ID')
dataset_name = os.getenv('GBQ_DATASET')
table_name = os.getenv('GBQ_TABLE')


output_csv = False if os.getenv('OUTPUT_CSV') == "False" else True

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)



bq = bigquery.Client(project=project, credentials=credentials)
bq.create_dataset(dataset_name, exists_ok=False)
bq.create_table(table_name)

table = bq.get_table(table_name)

print("table created successfully")
