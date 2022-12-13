from scripts.utilities import ingest_data

# script testing a basic data ingestion workflow with GBQ

rows = [
    (1, 1, "survey1", 1, "holland", "excited","I am happy", 3),
    (2, 1, "survey1", 1, "rotterdam", "music", "I often play sports", 2),
    (3, 2, "survey2", 2, "brabant", "weather", "I like the weather here", 5),
]

ingest_data(rows)
