import pandas as pd
import os
from dotenv import load_dotenv
from utilities import fetch_client
from datetime import datetime

load_dotenv()

output_csv = False if os.getenv('OUTPUT_CSV') == "False" else True

query = os.getenv('QUERY')

bq = fetch_client()

# Run the query
query_job = bq.query(query)

# Get the results
results = query_job.to_dataframe()

print("\n")
print(f"The results of query\n{query}\nare:")
print("\n")

print(results)
print("\n")

if output_csv:

    str_current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    data_path = os.getenv('DATA_PATH')

    print(f"Writing query results to a csv-file in directory: {data_path}")

    file_name = "query_results_" + str_current_datetime + ".csv"

    results.to_csv(f"{data_path}/{file_name}")

    print(f"Successfully created file {file_name} in {data_path}.")

if __name__ == '__main__':
     pass
