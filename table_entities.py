from dataclasses import dataclass, field
from langchain_core.tools import tool
from collections import defaultdict
from dotenv import load_dotenv
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
    referencing_table: str
    reference_column: str
    reference_table: str

    def __str__(self):
        if not self.is_reference:
            return f"foreign key({self.referencing_column}) references {self.reference_table}({self.reference_column})"


@dataclass
class Table:
    table_name: str
    registries: int
    columns: dict[str, Columns] = field(default_factory=dict)
    foreign_keys: dict[str, list[ForeignKeys]] = field(default_factory=dict)
    references_to_table: dict[str, list[ForeignKeys]] = field(default_factory=dict)

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

        for _, fks in self.foreign_keys.items():
            if (len(fks) == 0):
                result += "\tThis table has no foreign keys\n"
            else:
                for fk in fks:
                    result += f"\t{fk}\n"

        result += "\nPrimary key(s):\n"

        for _, column in self.columns.items():
            if column.primary_key:
                result += f"\tPrimary key: {column.field_name}\n"

        result += f"\tThis tables has {self.registries} rows\n\n"

        return result


def fetch_table_row_amount(table_name: str, con) -> int:
    try:
        with con.cursor() as cur:
            registries_query = f"""
            SELECT
                count(*)
            FROM
                {table_name}
            """
            cur.execute(registries_query, ())

            row = cur.fetchone()
            if row:
                return row[0]

    except Exception as err:
        if con:
            con.rollback()
        raise RuntimeError(f"Crash while getting the amount of rows in {table_name}:\n{err}")


def fetch_references_to_table(schema_name: str, table_name: str, con) -> dict[str, list[ForeignKeys]]:
    try:
        fks: dict[str, list[ForeignKeys]] = defaultdict(list)
        with con.cursor() as cur:
            fks_query = """
            SELECT
                kcu.TABLE_NAME AS referencing_table,
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
                ccu.table_name = %s AND
                cns.contype = 'f'
            """

            cur.execute(fks_query, (schema_name, table_name))

            rows = cur.fetchall()
            for row in rows:
                fk = ForeignKeys(
                    referencing_table=row[0],
                    referencing_column=row[1],
                    reference_table=row[2],
                    reference_column=row[3],
                )
                fks[table_name].append(fk)

            return fks
    except Exception as err:
        if con:
            con.rollback()
        raise RuntimeError(f"Crash while getting tables from schema:\n{err}")


def fetch_table_foreign_keys(schema_name: str, table_name: str, con) -> dict[str, list[ForeignKeys]]:
    try:
        fks: dict[str, list[ForeignKeys]] = defaultdict(list)
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
                    referencing_table=table_name,
                    reference_table=row[1],
                    reference_column=row[2],
                )
                fks[table_name].append(fk)

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
def fetch_schema_tables() -> list[Table]:
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
                        ),
                        references_to_table=fetch_references_to_table(
                            schema_name,
                            row[0],
                            con
                        ),
                        registries=fetch_table_row_amount(row[0], con)
                    )

                    tables.append(table)

                return tables
    except Exception as err:
        if con:
            con.rollback()
        return f"Crash while getting the tables from {schema_name}:\n{err}"
        # raise RuntimeError(f"Crash while getting tables from schema:\n{err}")


@tool
def execute_query(query: str) -> str:
    """
    Executes a SQL query and returns a safe, formatted JSON summary
    of the result. Limits the number of rows to avoid context overload.

    Use only:
    1. If the user gives you a SQL query directly.
    2. If you generated the query to retrieve information from the database.

    After executing (if successful), format the answer using the data,
    NOT the raw JSON.

    If the result is too large, return a warning to the user instead.
    """
    query = query.strip().rstrip(";")
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
        return f"Crash while executing the queyr\nError: {err}\n"
        # raise RuntimeError(f"Crash while executing {query}\nError: {err}\n")
