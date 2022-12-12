import os
from dotenv import load_dotenv
from utilities import fetch_client

load_dotenv()

project = os.getenv('GCP_PROJECT_ID')
dataset_name = os.getenv('GBQ_DATASET')
table_name = os.getenv('GBQ_TABLE')


bq = fetch_client()

table = bq.get_table(table_name)

rows = [
    (1, 1, "survey1", 1, "brabant", "happy","I am happy", 3),
    (2, 1, "survey1", 1, "brabant", "sport", "I often play sports", 2),
    (3, 2, "survey2", 2, "brabant", "weather", "I like the weather here", 5),
]

errors = bq.insert_rows(table, rows)

print("test data successfully inserted")
# schema = [
#     bigquery.SchemaField("response_ID", "INTEGER", mode="NULLABLE"),
#     bigquery.SchemaField("survey_ID", "INTEGER", mode="NULLABLE"),
#     bigquery.SchemaField("survey_version", "STRING", mode="NULLABLE"),
#     bigquery.SchemaField("participant_ID", "INTEGER", mode="NULLABLE"),
#     bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
#     bigquery.SchemaField("question_ID", "STRING", mode="NULLABLE"),
#     bigquery.SchemaField("question_text", "STRING", mode="NULLABLE"),
#     bigquery.SchemaField("answer", "INTEGER", mode="NULLABLE"),
# ]
