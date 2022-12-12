## make command shortcuts

# start API

start_api:
	-@cd scripts && uvicorn api:app --reload

# run SQL query
query:
	-@python scripts/query_db.py
