from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import snowflake.connector
import pandas as pd
import requests
import os

# Config

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable

SLACK_BOT_TOKEN = Variable.get("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = Variable.get("SLACK_CHANNEL_ID")
CSV_FILE_PATH = "/opt/airflow/dags/failed_csv/failing_balance_alerts.csv"

def get_snowflake_data():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT organization_id, invoice_date, balance_usd, prev_balance_usd
                FROM ASSERT_NEW_BALANCE_ALERT
                ORDER BY invoice_date DESC
                LIMIT 50
            """)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
    return pd.DataFrame(rows, columns=columns)

def send_csv_preview_to_slack():
    df = get_snowflake_data()
    if df.empty:
        message = "✅ No balance anomalies found today."
        post_slack_message(message)
        return

    df.to_csv(CSV_FILE_PATH, index=False)
    with open(CSV_FILE_PATH, "r") as f:
        lines = f.readlines()

    preview = "".join(lines[:10])
    message = f"🚨 {len(df)} balance anomalies detected.\n```{preview}```"
    post_slack_message(message)

def post_slack_message(text):
    response = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json"
        },
        json={
            "channel": SLACK_CHANNEL_ID,
            "text": text
        }
    )

    response_data = response.json()
    if not response.ok or not response_data.get("ok"):
        raise Exception(f"Slack message failed: {response.text}")

    print("✅ Slack alert sent.")

def create_dag():
    default_args = {
        'owner': 'data-platform',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }

    with DAG(
        dag_id="balance_alert_to_slack",
        schedule_interval="0 9 * * *",  # 9 AM daily
        default_args=default_args,
        catchup=False,
        description="Send preview of balance anomalies to Slack"
    ) as dag:

        alert_task = PythonOperator(
            task_id="send_balance_alert_preview",
            python_callable=send_csv_preview_to_slack
        )

        alert_task

    return dag

dag = create_dag()
