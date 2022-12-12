import pandas as pd
import os
from dotenv import load_dotenv
from utilities import fetch_client
from datetime import datetime

# loads configurations from .env ("dotenv") file
load_dotenv()

# imports config from .env file  (option: whether a csv-file should be the output of the query or not). Transforms string to boolean
output_csv = False if os.getenv('OUTPUT_CSV') == "False" else True

# imports SQL query from .env config file
query = os.getenv('QUERY')

# sets up GBQ client connection
bq = fetch_client()

# Runs imported query
query_job = bq.query(query)

# Creates dataframe with query results
results = query_job.to_dataframe()

# logs results in terminal
print("\n")
print(f"The results of query\n{query}\nare:")
print("\n")

print(results)
print("\n")

# If configured, writes a csv file with query results into the data path
if output_csv:

    str_current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M_%S") # creates a time_stamp for file naming

    data_path = os.getenv('DATA_PATH') # imports data path from config file

    print(f"Writing query results to a csv-file in directory: {data_path}") # progress log

    file_name = "query_results_" + str_current_datetime + ".csv" # creates file name with timestamp

    results.to_csv(f"{data_path}/{file_name}") # creates csv-filw

    print(f"Successfully created file {file_name} in {data_path}.") # logs succcessful creation
