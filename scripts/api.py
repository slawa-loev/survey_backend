from fastapi import FastAPI, Request, Response
import os
from dotenv import load_dotenv
from utilities import fetch_client

load_dotenv()

project = os.getenv('GCP_PROJECT_ID')
dataset_name = os.getenv('GBQ_DATASET')
table_name = os.getenv('GBQ_TABLE_SIMPLE')

app = FastAPI()

@app.post("/survey-results")
async def receive_survey_results(request: Request) -> Response:
    """Listens to post requests and adds data in request to GBQ database."""

    data = await request.json()

    responses = data[list(data.keys())[0]] # extracting responses dict from json

    question_IDs = list(responses.keys())
    answers = list(responses.values())

    data_entries = [(q_ID, answer) for q_ID, answer in zip(question_IDs,answers)]

    bq = fetch_client()

    table = bq.get_table(table_name)

    errors = bq.insert_rows(table, data_entries)

    if errors != []:
        print("Errors encountered inserting data.")

    print("data successfully inserted")

    return Response(status_code=200)
