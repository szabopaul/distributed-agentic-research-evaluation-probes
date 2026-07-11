from airflow.providers.common.ai.decorators.agent import agent_task
from airflow.providers.common.compat.sdk import DAG, task
from pathlib import Path
import json

#imports of various classes
from research.context import (
    ResearchContext, # infra: ResearchInfrastructure, state: ResearchState
    ResearchInfrastructure, # """Immutable infrastructure injected once at startup."""
    ResearchState, # """Mutable state accumulated during a research run."""
)

#imports of functions
from citations.tracker import CitationTracker # Tracks citations, assigns sequential [N] IDs, deduplicates by chunk_id.
from ingest.pipeline import ingest_corpus # Run the full ingestion pipeline.
from store.document_store import DocumentStore # Manages chunks in memory with JSONL persistence in ldr_index/

from research.tools import DeepResearchTools

deep_research_tools = DeepResearchTools()

with DAG(
    dag_id="deep_research",
    catchup=False,
    tags=["ai_agent", "llm"],
) as dag:

    @task.agent(
        llm_conn_id="deep_research-manager",
        # system_prompt="""
        # You are a research manager. Your ONLY job is to decompose a research question into \
        # focused sub-questions and submit them using the submit_plan tool.
        #
        # You MUST call the submit_plan tool with a list of 3-5 sub-questions that together \
        # cover the full scope of the user's research question. Do NOT try to answer the \
        # question yourself. Do NOT write a report. Just decompose and submit.
        #
        # Example: if the user asks "How does X relate to Y?", you call submit_plan with:
        # ["What is X and what are its core principles?", \
        # "What is Y and how does it work?", \
        # "What are the connections between X and Y?", \
        # "How do X and Y differ in practice?"]
        # """,
        system_prompt="""You are an expert planning assistant. Your task is to analyze the user's request and break it down into a list of specific, actionable questions that need to be answered.

        To complete this task, you MUST use the `submit_plan` tool.

        ### Tool Specification
        - **Tool Name**: submit_plan
        - **Argument**: `questions` (Type: List of Strings)
        - **Description**: Submits the final array of generated questions to the Airflow DAG pipeline.

        ### Response Format
        You must call the tool by responding ONLY with a valid JSON object matching this schema. Do not include any conversational filler, markdown formatting outside of the JSON block, or extra text.

        ```json
        {
          "tool_name": "submit_plan",
          "arguments": {
            "questions": [
              "First question string here?",
              "Second question string here?",
              "Third question string here?"
            ]
          }
        """,
        # toolsets=[deep_research_tools]
    )
    def research_manager_agent(prompt: str):
        print(f"[INFO]: {prompt}")
        # return prompt
        return (
            f"Decompose the following question into focused sub-questions and submit them to the submit_plan tool as the parameter sub_questions."
            f"\n\n{prompt}"
        )
    def submit_plan(questions: list[str]) -> str:
        """Submit a research plan consisting of focused sub-questions.

        The manager must call this tool with a list of 3-5 sub-questions that
        together cover the full scope of the research question.
        """
        # ctx.context.state.sub_questions = sub_questions
        formatted = "\n".join(f"  {i}. {q}" for i, q in enumerate(questions, 1))
        return f"Plan submitted with {len(questions)} sub-questions:\n{formatted}"

    @task
    def execute_tool_call(json_response: str):
        # Parse the LLM's string output into a Python dict
        response_data = json.loads(json_response)

        tool_name = response_data.get("tool_name")
        arguments = response_data.get("arguments", {})

        if tool_name == "submit_plan":
            # 'questions' is now a true Python list of strings
            questions_list = arguments.get("questions", [])
            return submit_plan(questions=questions_list)

    response = research_manager_agent("What are the historical reasons for Canada's bilingualism?")
    report = execute_tool_call(json_response=response)
