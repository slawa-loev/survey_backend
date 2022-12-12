import pandas as pd
import os
from dotenv import load_dotenv
from utilities import fetch_client, validate_table_name
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

    print(f"Writing query results to current directory: {os.getcwd()}")

    file_name = "query_results_" + str_current_datetime + ".csv"

    results.to_csv(file_name)

    print(f"Successfully created file {file_name} in {os.getcwd()}.")

# if __name__ == '__main__':
#     pass
