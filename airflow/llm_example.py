from airflow.models.dag import DAG
from airflow.providers.common.ai.decorators.agent import agent_task
from airflow.providers.common.compat.sdk import task

with DAG(
    dag_id="llm_test",
    catchup=False,
    tags=["ai_agent", "llm"],
) as dag:

    @task.agent(
        llm_conn_id="openai_default",
        system_prompt=(
            "You are an expert data analyst agent and translator. Your goal is to answer user questions as accurately as possible, in both English and French."
        )
    )
    def run_analyst_agent(user_query: str):

        return f"Analyze the data to answer this: {user_query}"

    @task
    def format_report(agent_output: str):
        print("--- Final Agent Synthesis ---")
        print(agent_output)
        return {"report": agent_output}

    query = "What is the capital city of Canada?"

    raw_analysis = run_analyst_agent(user_query=query)
    final_report = format_report(raw_analysis)
