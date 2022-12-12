from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
from dotenv import load_dotenv
from utilities import fetch_client

load_dotenv()

# Setting up data warehouse on BQ

project = os.getenv('GCP_PROJECT_ID')
dataset_name = os.getenv('GBQ_DATASET')
table_name = os.getenv('GBQ_TABLE')


bq = fetch_client()

bq.create_dataset(dataset_name, exists_ok=True)
bq.create_table(table_name, exists_ok=True)

print("table created successfully")

table = bq.get_table(table_name)

# Creating and updating schema for table

schema = [
    bigquery.SchemaField("response_ID", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("survey_ID", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("survey_version", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("participant_ID", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("question_ID", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("question_text", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("answer", "INTEGER", mode="NULLABLE"),
]

table.schema = schema # updating the schema locally

table = bq.update_table(table, ["schema"]) # updating the schema in the cloud

print("table schema created and updated successfully")
