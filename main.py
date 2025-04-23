from table_entities import fetch_schema_tables, execute_query, vectorize, search_on_postgres_documentation
from langchain.agents import create_react_agent, AgentExecutor
from langchain.memory import ConversationBufferMemory
from langchain_openai.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate

path = "postgresql-17-US.pdf"

# If you change the docName, change the query on
# search_on_postgres_documentation to reflect the change on the new
# document name
docName = "Postgres 17 documentation"
vectorize(path, docName)

chat = ChatOpenAI(
    base_url="https://hermes.ai.unturf.com/v1",
    api_key="Not-needed",
    model="adamo1139/Hermes-3-Llama-3.1-8B-FP8-Dynamic"
)

prompt = PromptTemplate.from_template("""
Answer the following questions as best you can. You have
access to the following tools:

{tools}

You can also use the chat history to retain information:

{chat_history}

If the question doesn't requieres the use of a tool
then simply try answering based on your knowledge

Before entering the format loop there are somethings to take in mind:
- If the question asked requieres the help from the database
  you'll first fetch the tables and AFTER THAT you'll start the
  loop. Whenever you are using the fetch_schema_tables tool, remember
  this tool DOESN'T requires any Action Input, so drop the parenthesis.

- Whenever the user is asking a question that requires data FROM a database
  you are ment to use fetch_schema_tables and THEN, with that information,
  generate a query that will be used for the execute_query tool.

- Whenever a query fails, there are two course of actions:
    1. Confirm there was no typo. First check using the fetch_schema_tables that
       every field was correctly specified and there wasn't any spelling mistakes.

    2. Query structure logic. If there are no typos, make a brief summary out of
       the error messageand pass it to the search_on_postgres_documentation to get
       a better reference on how to fix the query.

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action in case it requieres one, if not, skip
Observation: the result of the action

You may only repeat the Thought/Action/Action Input/Observation
cycle up to 8 times.

If after 8 attempts you are still unsure, tell the user to be
more specific in what he ment by that question.

Thought: I now know the final answer
Final Answer: the final answer to the original input question

Begin!

Question: {input}
Thought:{agent_scratchpad}
""")

memory = ConversationBufferMemory(memory_key="chat_history",
                                  return_messages=True)

tools = [fetch_schema_tables, execute_query, search_on_postgres_documentation]

agent = create_react_agent(chat,
                           tools,
                           prompt=prompt)

executor = AgentExecutor(agent=agent, tools=tools, memory=memory,
                         verbose=True, handle_parsing_errors=True,
                         max_iterations=8)

while (True):
    user_input = input("\nIn what can I help you?\n > ")
    if (user_input == "!q"):
        break

    executor.invoke({"input": user_input})
