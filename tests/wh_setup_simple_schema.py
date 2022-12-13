from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
from dotenv import load_dotenv
from scripts.utilities import fetch_client, setup_and_fetch_table

load_dotenv()

# testing setting up data table on BQ with simple schema for interaction with SurveyJS

project = os.getenv('GCP_PROJECT_ID')
dataset_name = os.getenv('GBQ_DATASET')


bq = fetch_client()


schema = [
    bigquery.SchemaField("question_ID", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("answer", "INTEGER", mode="NULLABLE"),
]


table_name = f"{dataset_name}.new_table"

setup_and_fetch_table(bq, table_name, schema)
