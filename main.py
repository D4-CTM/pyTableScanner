from table_entities import fetch_schema_tables

connection_string = """
    dbname=postgres
    host=localhost
    port=4884
    user=postgres
    password=Postgres
"""

for table in fetch_schema_tables("public", connection_string):
    print(table)
