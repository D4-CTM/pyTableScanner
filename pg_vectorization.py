from langchain_text_splitters import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer
from langchain_core.tools import tool
from dotenv import load_dotenv
from pypdf import PdfReader
from os import getenv
import psycopg2

load_dotenv()


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
                    ++idx

                con.commit()
                print(F"{docName} vectorized succesfully")
    except (Exception, psycopg2.DatabaseError) as err:
        if con:
            con.rollback()
        raise RuntimeError(f"Failed to transcribe video: {err}")


@tool
def search_on_postgres_documentation(text: str) -> str:
    """
    Makes a semantic search using the postgres documentation.

    Use this tool when you have no idea on how to create a query!
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
        return f"Failed to searching on documentation:\n{err}"
        # raise RuntimeError(f"Failed to searching on documentation: {err}")
