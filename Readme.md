# Postgres schema table scan

For this app to work it'll requiere the next dependencies to be installed:
```bash
pip install psycopg2-binary python-dotenv
```

It is a simple schema scanner for **postgres** databases. It is require that the root
of the app contains a `.env` file with the next fields:
```bash
CONNECTION_STRING="..."
SCHEMA_TO_SCAN="..."
```

