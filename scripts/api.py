from fastapi import FastAPI, Request, Response
import os
from dotenv import load_dotenv
from utilities import fetch_client, ingest_data

# loads configurations from .env ("dotenv") file
load_dotenv()

table_name = os.getenv('GBQ_TABLE_SIMPLE')

# creates fastapi client
app = FastAPI()

# establishes post request endpoint
@app.post("/survey-results")
async def receive_survey_results(request: Request) -> Response:
    """Listens to post requests and pushes request data to GBQ database."""

    # listens to post requests
    data = await request.json()

    # extracting responses dict from data json
    responses = data[list(data.keys())[0]]

    # extracts question_IDs from dict
    question_IDs = list(responses.keys())

    # extracts answers from dict
    answers = list(responses.values())

    # creates a list of tuples (a tuple per row), this format is accepted by GBQ API
    data_entries = [(q_ID, answer) for q_ID, answer in zip(question_IDs,answers)]

    # establishes connection with GBQ client
    bq = fetch_client()

    #errors = ingest_data(data_entries, table_name=table_name) # ToDo: test module, adapt if clause to [] or None

    # fetches table in which data is to be ingested
    table = bq.get_table(table_name)

    # inserts data and collects errors if any
    errors = bq.insert_rows(table, data_entries)

    # if errors are encountered, this is logged and api responds with error status code.
    if errors != []:
        print(f"Errors encountered inserting data: {errors}")
        return Response(status_code=400)

    # if no errors are encountered, api responds with OK status code.
    print("Data successfully inserted.")
    return Response(status_code=200)
