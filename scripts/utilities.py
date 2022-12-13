from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq as pdgbq
import os
from dotenv import load_dotenv
from typing import Union

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

def create_and_fetch_empty_table(client, table_name:str, exists_ok:bool=True) -> None:
    """Creates and fetches new empty BQ-table."""

    client.create_table(table_name, exists_ok=exists_ok)

    print("table created successfully")

    # fetches table in which data is to be ingested
    return client.get_table(table_name)

def setup_and_fetch_table(client, table_name:str, schema: list):
    """Sets up a new GBQ table with schema."""

    table = create_and_fetch_empty_table(client, table_name)

    table.schema = schema # updating the schema locally

    table = client.update_table(table, ["schema"]) # updating the schema in the cloud

    print("table schema created and updated successfully")

    return table


def ingest_data(data: Union[pd.DataFrame, list], schema: list = None, new_table: bool = False, table_name: str = None) -> Union[None, list]: # alternatively one can use | instead of imprting and using Union[] with Python versions 3.1 and newer
    """Ingests data into GBQ-table."""

    if type(data) != pd.DataFrame and type(data) != list:
        raise ValueError("Data must be list or pd.DataFrame")

    if type(data) != pd.DataFrame and new_table == True and schema == None:
        raise ValueError("Data is not a pd.DataFrame from which a schema for a new table could be extracted.")

    if table_name == None and new_table == True:
        raise ValueError("Table name for new table is missing.")

    bq = fetch_client()

    if new_table and type(data) == list:

        table = setup_and_fetch_table(bq, table_name, schema)

        # inserts data and collects errors if any
        errors = bq.insert_rows(table, data)

        # if errors are encountered, this is logged.
        if errors != []:
            print(f"Errors encountered inserting data: {errors}")
        else:
            print("Data successfully inserted.")


    if new_table and type(data) == pd.DataFrame:

        table = create_and_fetch_empty_table(bq, table_name)

        bq.load_table_from_dataframe(data, table)


    if new_table == False and type(data) == pd.DataFrame:

        if table_name == None:
            print("Fetching table specified in config .env.")
            table_name = os.getenv('GBQ_TABLE')

        table = bq.get_table(table_name)

        bq.load_table_from_dataframe(data, table)

    if new_table == False and type(data) == list:

        if table_name == None:
            print("Fetching table specified in config .env.")
            table_name = os.getenv('GBQ_TABLE')

        table = bq.get_table(table_name)

        errors = bq.insert_rows(table, data)

        # if errors are encountered, this is logged .
        if errors != []:
            print(f"Errors encountered inserting data: {errors}")
            return errors
        else:
            print("Data successfully inserted.")


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
