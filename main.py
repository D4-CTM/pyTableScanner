from pg_vectorization import vectorize, search_on_postgres_documentation
from schema_djikstra import create_djikstra
from langchain.agents import create_react_agent, AgentExecutor
from table_entities import fetch_schema_tables, execute_query
from langchain.memory import ConversationBufferMemory
from langchain_openai.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate

path = "postgresql-17-US.pdf"

# If you change the docName, change the query on  search_on_postgres_documentation
# to reflect the change on the new document name
docName = "Postgres 17 documentation"
vectorize(path, docName)

chat = ChatOpenAI(
    base_url="https://hermes.ai.unturf.com/v1",
    api_key="Not-needed",
    model="adamo1139/Hermes-3-Llama-3.1-8B-FP8-Dynamic"
)


prompt = PromptTemplate.from_template("""
You are an intelligent agent equipped with tools to fetch and reason about data stored in a PostgreSQL database.

You have access to the following tools:

{tools}

Use these tools ONLY when the user’s question requires retrieving data from the database.

When using the tools, ALWAYS follow this strict sequence:

1. **fetch_schema_tables** — Use this first to inspect the database schema. This helps you understand what tables exist and how they relate to each other. This tool DOESN'T require any input to work, simply call it like 'fetch_schema_tables'

2. **search_on_postgres_documentation** — After understanding the schema, search the PostgreSQL documentation to learn functions, clauses, or syntax that are unfamiliar or needed.

3. **execute_query** — Finally, write and run the SQL query based on your schema understanding and documentation research.

---

 **Query Error Handling Instructions**:

If `execute_query` fails:

- If the error is related to incorrect usage of a SQL function or syntax (logic error), go back and use **search_on_postgres_documentation** to learn the correct usage.

- If the error mentions a missing or incorrect table/column name (schema error), revisit the output of **fetch_schema_tables** to correct the mistake.

---

Repeat the appropriate tool actions until the query succeeds or until the 9-step cycle limit is reached.

Use the following format to answer:

Question: the input question you must answer
Thought: reason about what to do next
Action: the tool you want to use (must be one of [{tool_names}])
Action Input: the input to the tool (if needed)
Observation: the result of the tool call

You may repeat the **Thought/Action/Action Input/Observation** cycle up to 9 times. If you are still unsure, respond by asking the user to clarify or say the information is beyond your current capabilities.

When you're confident in your final answer, use:

Thought: I now know the final answer
Final Answer: the final answer to the original input question

Begin!

Question: {input}
{agent_scratchpad}
""")

memory = ConversationBufferMemory(memory_key="chat_history",
                                  return_messages=True)

tools = [fetch_schema_tables, execute_query, search_on_postgres_documentation, create_djikstra]

agent = create_react_agent(chat,
                           tools,
                           prompt=prompt)

executor = AgentExecutor(agent=agent, tools=tools,  # memory=memory,
                         verbose=True, handle_parsing_errors=True,
                         max_iterations=8)

while (True):
    user_input = input("\nIn what can I help you?\n > ")
    if (user_input == "!q"):
        break

    executor.invoke({"input": user_input})
