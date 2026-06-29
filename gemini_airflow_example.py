## pasted with care from Gemini (Flash using a Pro account)
#
# Can you please find me an example of an agentic pipeline with Apache Airflow?
#
# Apache Airflow is well-suited for agentic pipelines. Traditionally, AI agents run in loose, unpredictable loops that can easily break, run up massive API costs, or become entirely un-observable. Bringing them into Airflow adds structure, explicit retry logic, caching, dynamic task mapping, and human-in-the-loop checkpoints.
# Astronomer
# + 1
#
# With the release of the Common AI Provider (apache-airflow-providers-common-ai), Airflow natively supports LLM and AI agent integration.
#
# Below is an architectural example of how an agentic pipeline looks in Airflow, followed by a complete Python code example.
#
# The Agentic Workflow Pattern
# In an agentic pipeline, a single complex prompt is decomposed into discrete steps, executed via dynamic task mapping (parallel tool calls), synthesized by a main agent, and held for a human review.
# Apache Airflow - Apache Software Foundation
#
# Code Example: Autonomous SQL Data Analyst Agent
# This example uses the native @task.agent decorator and the built-in SQLToolset from Airflow's Common AI Provider. It takes a natural language question, turns the agent loose to introspect your database schema, executes the proper queries autonomously, and passes the output to a downstream validation step.
#
# Python
# from datetime import datetime
# from airflow.models.dag import DAG
# from airflow.providers.common.ai.toolsets.sql import SQLToolset
# from airflow.providers.common.compat.sdk import task
#
# # Define the Agentic Workflow
# with DAG(
#     dag_id="agentic_sql_analyst",
#     schedule_interval=None,
#     start_date=datetime(2026, 1, 1),
#     catchup=False,
#     tags=["ai_agent", "llm"],
# ) as dag:
#
#     # 1. Define the Autonomous Agent Task
#     @task.agent(
#         llm_conn_id="openai_default",  # Airflow Connection housing your LLM API keys
#         system_prompt=(
#             "You are an expert data analyst agent. Your goal is to answer user questions "
#             "by inspecting the database, writing exact SQL queries, running them, and analyzing the data. "
#             "Only use the tools explicitly provided to you."
#         ),
#         toolsets=[
#             SQLToolset(
#                 db_conn_id="postgres_warehouse",  # The managed database connection in Airflow
#                 allowed_tables=["customers", "orders", "subscriptions"],
#                 max_rows=50,
#             )
#         ],
#     )
#     def run_analyst_agent(user_query: str):
#         """
#         The agent receives this string, determines which tools to use
#         (e.g., list_tables -> get_schema -> query), executes them in an internal
#         reasoning loop, and returns the final text response.
#         """
#         return f"Analyze the data to answer this: {user_query}"
#
#     # 2. A downstream task to format or validate the agent's work
#     @task
#     def format_report(agent_output: str):
#         print("--- Final Agent Synthesis ---")
#         print(agent_output)
#         return {"report": agent_output, "status": "READY_FOR_REVIEW"}
#
#     # Pipeline Execution Flow
#     # In production, the trigger query might come from a UI or a Kafka message trigger
#     query = "What are our top 5 customers by total order spend, and are they on active subscriptions?"
#
#     raw_analysis = run_analyst_agent(user_query=query)
#     final_report = format_report(raw_analysis)
# Why building Agentic pipelines this way matters:
# HookToolset Extensibility: Instead of writing custom API integration code for every tool your agent needs, you can pass any native Airflow Hook (like S3Hook, SlackHook, or SnowflakeHook) into a HookToolset. Airflow automatically extracts the docstrings and method signatures, converting them directly into JSON Schemas that the LLM uses for tool-calling.
#
# Token Optimization: Because Airflow handles the state machine and step-by-step logging, you don't need to keep an entire, massive multi-turn conversation history inside an external framework's memory.
#
# Deterministic Guardrails: Notice that allowed_tables and max_rows are set at the infrastructure layer inside the DAG. The agent cannot hallucinate a command to drop a table or run a query that pulls a billion rows because the execution boundary is enforced by the Airflow worker, not the LLM's system prompt.
#
#---

from datetime import datetime
from airflow.models.dag import DAG
from airflow.providers.common.ai.decorators.agent import agent_task # I added this
#from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.compat.sdk import task

# Define the Agentic Workflow
with DAG(
    dag_id="llm_test",
    #schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ai_agent", "llm"],
) as dag:

    # 1. Define the Autonomous Agent Task
    @task.agent(
        llm_conn_id="openai_default",  # Airflow Connection housing your LLM API keys
        system_prompt=(
            "You are an expert data analyst agent. Your goal is to answer user questions "
            #"by inspecting the database, writing exact SQL queries, running them, and analyzing the data. "
            #"Only use the tools explicitly provided to you."
        )#,
        #toolsets=[
        #    SQLToolset(
        #        db_conn_id="postgres_warehouse",  # The managed database connection in Airflow
        #        allowed_tables=["customers", "orders", "subscriptions"],
        #        max_rows=50,
        #    )
        #],
    )
    def run_analyst_agent(user_query: str):
        """
        The agent receives this string, determines which tools to use
        (e.g., list_tables -> get_schema -> query), executes them in an internal
        reasoning loop, and returns the final text response.
        """
        return f"Analyze the data to answer this: {user_query}"

    # 2. A downstream task to format or validate the agent's work
    @task
    def format_report(agent_output: str):
        print("--- Final Agent Synthesis ---")
        print(agent_output)
        return {"report": agent_output, "status": "READY_FOR_REVIEW"}

    # Pipeline Execution Flow
    # In production, the trigger query might come from a UI or a Kafka message trigger
    #query = "What are our top 5 customers by total order spend, and are they on active subscriptions?"
    query = "What is the capital city of Canada?"

    raw_analysis = run_analyst_agent(user_query=query)
    final_report = format_report(raw_analysis)
