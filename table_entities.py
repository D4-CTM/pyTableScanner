from langchain_text_splitters import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer
from dataclasses import dataclass, field
from langchain_core.tools import tool
from dotenv import load_dotenv
from pypdf import PdfReader
from os import getenv
import psycopg2

load_dotenv()


@dataclass
class Columns:
    field_name: str
    type: str
    primary_key: bool = False

    def __str__(self) -> str:
        return f"Column: {self.field_name} - {self.type}"


@dataclass
class ForeignKeys:
    referencing_column: str
    reference_column: str
    reference_table: str

    def __str__(self):
        return f"foreign key({self.referencing_column}) references {self.reference_table}({self.reference_column})"


@dataclass
class Table:
    table_name: str
    columns: dict[str, Columns] = field(default_factory=dict)
    foreign_keys: dict[str, ForeignKeys] = field(default_factory=dict)

    def get_primary_keys(self) -> list[Columns]:
        columns: list[Columns] = []
        for name, column in self.columns.items():
            if column.primary_key:
                columns.append(column)

        return columns

    def __str__(self):
        result = f"Table name: {self.table_name}\n"
        for _, column in self.columns.items():
            result += f"\t{str(column)}\n"

        result += "\nForeign key(s):\n"

        for _, fk in self.foreign_keys.items():
            result += f"\t{fk}\n"

        result += "\nPrimary key(s):\n"

        for _, column in self.columns.items():
            if column.primary_key:
                result += f"\tPrimary key: {column.field_name}\n"

        return result


def fetch_table_foreign_keys(schema_name: str, table_name: str, con) -> dict[str, ForeignKeys]:
    try:
        fks: dict[str, ForeignKeys] = {}
        with con.cursor() as cur:
            fks_query = """
            SELECT
                kcu.COLUMN_NAME AS referencing_col,
                ccu.TABLE_NAME AS reference_table,
                ccu.COLUMN_NAME AS reference_col
            FROM
                pg_namespace nms
            JOIN
                pg_constraint cns
            ON
                nms.oid = cns.connamespace
            JOIN
                pg_class rel
            ON
                rel.oid = cns.conrelid
            JOIN
                information_schema.key_column_usage kcu
            ON
                cns.conname = kcu.constraint_name
            LEFT JOIN
                information_schema.constraint_column_usage ccu
            ON
                cns.conname = ccu.CONSTRAINT_NAME
            WHERE
                nms.nspname = %s AND
                kcu.table_name = %s AND
                cns.contype = 'f'
            """

            cur.execute(fks_query, (schema_name, table_name))

            rows = cur.fetchall()
            for row in rows:
                fk = ForeignKeys(
                    referencing_column=row[0],
                    reference_table=row[1],
                    reference_column=row[2],
                )
                fks[row[0]] = fk

            return fks
    except Exception as err:
        if con:
            con.rollback()
        raise RuntimeError(f"Crash while getting tables from schema:\n{err}")


def fetch_table_columns(schema_name: str, table_name: str, con) -> dict[str, Columns]:
    try:
        columns: dict[str, Columns] = {}
        with con.cursor() as cur:
            column_query = """
                SELECT
                    column_name,
                    CASE
                        WHEN data_type = 'numeric' AND
                            numeric_precision IS NOT NULL AND
                            numeric_scale IS NOT NULL
                        THEN data_type || '(' || numeric_precision || ','|| numeric_scale || ')'
                        ELSE data_type END AS "data_type"
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """

            cur.execute(column_query, (schema_name, table_name))
            rows = cur.fetchall()
            for row in rows:
                column = Columns(
                    field_name=row[0],
                    type=row[1]
                )
                columns[row[0]] = column

            pk_query = """
                SELECT DISTINCT
                    kcu.COLUMN_NAME AS primary_key
                FROM
                    pg_namespace nms
                JOIN
                    pg_constraint cns
                ON
                    nms.oid = cns.connamespace
                JOIN
                    pg_class rel
                ON
                    rel.oid = cns.conrelid
                JOIN
                    information_schema.key_column_usage kcu
                ON
                    cns.conname = kcu.constraint_name
                LEFT JOIN
                    information_schema.constraint_column_usage ccu
                ON
                    cns.conname = ccu.CONSTRAINT_NAME
                WHERE
                    nms.nspname = %s AND
                    kcu.table_name = %s AND
                    cns.contype = 'p'
            """

            cur.execute(pk_query, (schema_name, table_name))
            rows = cur.fetchall()

            pks: list[str] = []
            for row in rows:
                pks.append(row[0])

            for name in pks:
                if (columns.get(name, False)):
                    columns[name].primary_key = True

            return columns
    except Exception as err:
        if con:
            con.rollback()
        raise RuntimeError(f"Crash while getting tables from schema:\n{err}")


@tool
def fetch_schema_tables() -> str:
    """
    Function that gets the tables on a the 'public' schema.

    Use this tool always that the user ask's about the database, even if it is general
    information.

    The only case in which you'll not use this tool, is when the question is super abroad.

    In case it throws an error, DON'T retry using this method tool again, inform the user
    about this error and tell him you can't proceed.
    """
    try:
        connection_string = getenv("CONNECTION_STRING")
        schema_name = getenv("SCHEMA_TO_SCAN")

        tables: list[Table] = []
        with psycopg2.connect(connection_string) as con:
            with con.cursor() as cur:

                tables_query = """
                    SELECT
                        table_name
                    FROM
                        information_schema.tables
                    WHERE
                        table_schema = %s
                """

                cur.execute(tables_query, (schema_name,))

                rows = cur.fetchall()
                for row in rows:
                    table = Table(
                        table_name=row[0],
                        columns=fetch_table_columns(schema_name, row[0], con),
                        foreign_keys=fetch_table_foreign_keys(
                            schema_name,
                            row[0],
                            con
                        )
                    )

                    tables.append(table)

                result: str = ""
                for table in tables:
                    result += str(table)

                return result
    except Exception as err:
        if con:
            con.rollback()
        return f"Crash while getting the tables from {schema_name}:\n{err}"
        # raise RuntimeError(f"Crash while getting tables from schema:\n{err}")


@tool
def execute_query(query: str) -> str:
    """
    Tool that executes a query and returns a json based
    on that result.

    Use only in two cases:
    1. When the user inputs directly a query that he needs you to execute.
    2. When the user asks for the retrival of information within the database, in this
       cases YOU are in charge of creating the query!

    In both cases, you should, after the output is given (in case it didn't crash), return
    a formatted answer based on that json rather than the json itself.
    """
    if query[-1] == ";":
        query = query[:-1]

    print(query)
    try:
        connection_string = getenv("CONNECTION_STRING")
        with psycopg2.connect(connection_string) as con:
            with con.cursor() as cur:
                q = f"""
                SELECT json_agg(row_to_json(t))
                FROM (
                        {query}
                ) t
                """
                cur.execute(q, ())
                row = cur.fetchone()

                return str(row)
    except Exception as err:
        if con:
            con.rollback()
        return f"Crash while executing {query}\nError: {err}\n"
        # raise RuntimeError(f"Crash while executing {query}\nError: {err}\n")


def vectorize(docPath: str, docName: str):
    try:
        connection_string = getenv("VECTOR_CONNECTION_STRING")
        with psycopg2.connect(connection_string) as con:
            with con.cursor() as cur:

                doc_exists_query = """
                SELECT 1
                FROM item_origin
                WHERE file_name = %s
                """

                cur.execute(doc_exists_query, (docName,))

                row = cur.fetchone()
                if row:
                    print("File already exists")
                    return

                item_insertion = """
                INSERT INTO item_origin(file_name)
                VALUES(%s) RETURNING id
                """

                cur.execute(item_insertion, (docName,))

                row = cur.fetchone()
                if row:
                    id = row[0]
                else:
                    raise Exception('Could not get the id')

                doc = PdfReader(docPath)
                text = ""
                for page in doc.pages:
                    text += page.extract_text()

                splitter = RecursiveCharacterTextSplitter(
                    chunk_size=350,
                    chunk_overlap=50,
                )

                chunks = splitter.split_text(text)

                embedding_insertion = """
                INSERT INTO vectorized_item(content, embedding, origin_id)
                VALUES(%s, %s, %s)
                """

                model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
                idx: int = 1
                for chunk in chunks:
                    tensor = model.encode(chunk)
                    cur.execute(embedding_insertion,
                                (chunk, tensor.tolist(), id))
                    print(f"Inserted {idx}/{len(chunks)} chunks")

                con.commit()
    except (Exception, psycopg2.DatabaseError) as err:
        if con:
            con.rollback()
        raise RuntimeError(f"Failed to transcribe video: {err}")


@tool
def search_on_postgres_documentation(text: str) -> str:
    """
    Makes a semantic search using the postgres documentation.

    Use this tool ONLY when you have no idea on how to create a query!
    """
    connection_string = getenv("VECTOR_CONNECTION_STRING")
    try:
        with psycopg2.connect(connection_string) as con:
            with con.cursor() as cur:
                model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
                embedding = model.encode(text)

                cur.execute("""
                    SELECT vi.id, vi.content
                    FROM vectorized_item vi
                    JOIN item_origin io
                    ON io.id = vi.origin_id
                    WHERE io.file_name = 'Postgres 17 documentation'
                    ORDER BY embedding <-> %s::vector
                    LIMIT 100
                """, (embedding.tolist(),))

                result = cur.fetchall()
                format_str = "-------\n"
                for row in result:
                    format_str += row[1] + "\n"
                    format_str += "-------\n"

                return format_str
    except (Exception, psycopg2.DatabaseError) as err:
        if con:
            con.rollback()
        raise RuntimeError(f"Failed to transcribe video: {err}")
