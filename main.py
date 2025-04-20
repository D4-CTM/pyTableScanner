from table_entities import fetch_schema_tables
from dotenv import load_dotenv
from os import getenv

load_dotenv()

connection_string = getenv("CONNECTION_STRING")
schema = getenv("SCHEMA_TO_SCAN")

for table in fetch_schema_tables(schema, connection_string):
    print(table)
