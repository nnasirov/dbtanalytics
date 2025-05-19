import os
import requests
import pandas as pd
import snowflake.connector

# Config
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = "DEEL_ANALYTICS"
SNOWFLAKE_SCHEMA = "DBT_TEST__AUDIT"
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")  # Must be channel ID (e.g. C01XXXX)
CSV_FILE_PATH = "/Users/nasirovnicat/dbtanalytics/scripts/failed_csv/failing_balance_alerts.csv"
FAILURE_TABLE = "ASSERT_NEW_BALANCE_ALERT"

def get_snowflake_data():
    query = f"""
        SELECT organization_id, invoice_date, balance_usd, prev_balance_usd
        FROM {FAILURE_TABLE}
        ORDER BY invoice_date DESC
        LIMIT 50
    """
    with snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
    return pd.DataFrame(rows, columns=columns)

def send_slack_message(text):
    headers = slack_headers()
    response = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers=headers,
        json={"channel": SLACK_CHANNEL_ID, "text": text}
    )
    _raise_if_failed(response, "chat.postMessage")
    print("Slack message sent.")

def send_slack_file(file_path, initial_comment):
    import json

    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)

    # Step 1: Get an external upload URL
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json; charset=utf-8"
    }

    payload = {
        "filename": file_name,
        "length": file_size,
    }

    res = requests.post(
        "https://slack.com/api/files.getUploadURLExternal",
        headers=headers,
        data=json.dumps(payload) 
    )
def send_csv_as_text_snippet(file_path, header="🚨 Balance anomalies detected:"):
    with open(file_path, "r") as f:
        lines = f.readlines()

    preview = "".join(lines[:10]) 
    message = f"{header}\n```{preview}```"

    response = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json"
        },
        json={
            "channel": SLACK_CHANNEL_ID,
            "text": message
        }
    )

    response_data = response.json()
    print("Slack message response:", response_data)

    if not response.ok or not response_data.get("ok"):
        raise Exception(f"Slack snippet upload failed: {response.text}")

    print("CSV preview sent to Slack.")


def slack_headers():
    return {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }

def _raise_if_failed(response, label):
    if not response.ok or not response.json().get("ok"):
        raise Exception(f"Slack API '{label}' failed: {response.text}")

def main():
    df = get_snowflake_data()
    if df.empty:
        msg = "No balance anomalies found today."
        print(msg)
        send_slack_message(msg)
    else:
        df.to_csv(CSV_FILE_PATH, index=False)
        msg = f"{len(df)} balance anomalies detected. Showing preview below:"
        send_csv_as_text_snippet(CSV_FILE_PATH, msg)

if __name__ == "__main__":
    main()
