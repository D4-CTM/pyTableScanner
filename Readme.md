# Postgres schema table scan

For this app to work it'll requiere the next dependencies to be installed:
```bash
pip install psycopg2-binary python-dotenv pypdf langchain langchain-openai sentence-transformers
```

As for the vector database, it is postgres with the **pgvector** extension. In this 
repository you'll find a docker compose that uses pgvector's image, simply execute
on the terminal:
```bash
docker compose -f docker-compose.yml up -d
```

Finally, before running this program you'll need to create a `.env` file with the next
environment variables:
```bash
VECTOR_CONNECTION_STRING="dbname=postgres host=localhost port=4884 user=postgres password=Postgres"
CONNECTION_STRING="..." # As for now, this project only supports postgres databases
SCHEMA_TO_SCAN="public"
```

