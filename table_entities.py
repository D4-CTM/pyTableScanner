from dataclasses import dataclass, field
import psycopg2


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


def fetch_schema_tables(schema_name: str, conStr: str) -> list[Table]:
    try:
        tables: list[Table] = []
        with psycopg2.connect(conStr) as con:
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

                return tables
    except Exception as err:
        if con:
            con.rollback()
        raise RuntimeError(f"Crash while getting tables from schema:\n{err}")
