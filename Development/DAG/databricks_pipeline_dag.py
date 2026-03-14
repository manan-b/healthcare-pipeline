from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="run_databricks_pipeline",
    default_args=default_args,
    start_date=datetime(2024,1,1),
    schedule="30 0 * * *",   # 6:00 AM IST
    catchup=False,
    tags=["databricks","healthcare"]
) as dag:


    # Slack message when pipeline starts
    start_alert = SlackWebhookOperator(
        task_id="pipeline_started",
        slack_webhook_conn_id="slack_webhook",
        message="""
🚀 Databricks pipeline started

DAG: run_databricks_pipeline
Status: RUNNING
Time: {{ ts }}
"""
    )


    # Run Databricks Pipeline
    run_pipeline = DatabricksSubmitRunOperator(
        task_id="trigger_pipeline",
        databricks_conn_id="databricks_default",
        json={
            "pipeline_task": {
                "pipeline_id": "10f71ea0-9391-461c-8ee6-2de43f3b6074"
            }
        }
    )


    # Slack message on success
    success_alert = SlackWebhookOperator(
        task_id="pipeline_success",
        slack_webhook_conn_id="slack_webhook",
        message="""
✅ Databricks pipeline completed successfully

DAG: run_databricks_pipeline
Task: trigger_pipeline
Time: {{ ts }}
""",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )


    # Slack message on failure
    failure_alert = SlackWebhookOperator(
        task_id="pipeline_failed",
        slack_webhook_conn_id="slack_webhook",
        message="""
❌ Databricks pipeline FAILED

DAG: run_databricks_pipeline
Task: trigger_pipeline
Check Airflow logs immediately.
""",
        trigger_rule=TriggerRule.ONE_FAILED
    )


    # Workflow order
    start_alert >> run_pipeline
    run_pipeline >> [success_alert, failure_alert]