# 🔔 Financial Balance Change Alerting System

This repository contains a data alerting pipeline that detects when a customer's financial account balance changes by more than **50% day-over-day**. The project includes:

1. A **dimension table** enriched with organization-level metadata.
2. A **fact table** with daily balance computations per organization.
3. A suite of **data quality tests**.
4. Three methods to trigger alerts when sudden balance changes are detected.

---

## 📁 Project Structure
├── models/
│ ├── intermediate/payment/
│ │ ├── inmd_invoices.sql
│ │ └── schema.yml
│ └── marts/payment/
│ ├── dim_organization.sql # Enriched org dimension
│ ├── fct_organization_balance.sql # Main balance fact model
│ └── schema.yml # Tests for marts
├── macros/common/
│ └── generate_schema_name.sql
├── scripts/
│ ├── send_slack_balance_alert.py # Python alert script
│ ├── balance_alert_dag.py # Airflow DAG for Slack alerting
│ └── failed_csv/ # Output folder for CSV previews
├── tests/
│ └── assert_new_balance_alert.sql # Custom test SQL
├── seeds/
│ └── organizations.csv # Raw seed data
├── dbt_project.yml
├── packages.yml
└── README.md

---

## ✅ Deliverables (Based on Assignment)

### 1. **Dimension Table**
📄 `models/marts/payment/dim_organization.sql`

- Enriched organization table
- Includes currency, payment method, invoice activity, has_balance_due, total_invoice_usd, total_payment_usd and etc.

### 2. **Fact Table**
📄 `models/marts/payment/fct_organization_balance.sql`

- One row per day and `organization_id`
- Contains:
  - Daily & cumulative invoice and payment amounts
  - USD-converted balance
  - Day-over-day balance change %
  - Flag for >50% change: `balance_change_gt_50pct`

### 3. **Tests**
📄 `models/marts/payment/schema.yml` + `tests/assert_new_balance_alert.sql`

- `not_null`, `unique`, and `expect_column_values_to_be_in_set`
- Custom test to assert >50% balance jumps in recent days
- Elementary-compatible for Slack alerts

---

## 4. 🚨 Alerting Implementation

Three methods provided to detect and notify when daily balance jumps >50%:

### 🔹 Option A: **dbt + Elementary Slack Alerts**
- Defined in `schema.yml`
- We can add Volume Anomalies from dbt elementary adding time bucet to filter to recent days dynamically. Check: [[dbt elementary test](https://docs.elementary-data.com/oss/quickstart/quickstart-tests)]
- Slack channel and owner defined in `meta`

### 🔹 Option B: **Standalone Python Script**
📄 `scripts/send_slack_balance_alert.py`

- Connects to Snowflake
- Fetches latest balance anomalies
- Sends Slack message with top 10 as CSV preview (or prints to console)

python scripts/send_slack_balance_alert.py

### 🔹 Option C: **SAirflow DAGt**

- Used Docker compose
- Connects to Snowflake
- Fetches latest balance anomalies
- Sends Slack message with top 10 as CSV preview (or prints to console)


---

## ⚙️ Configuration Guide

### 🧊 Snowflake Setup

To connect this project to Snowflake, you'll need:

| Parameter        | Description                          | Example                          |
|------------------|--------------------------------------|----------------------------------|
| `SNOWFLAKE_USER` | Your Snowflake username              | `john.doe@getsafe.com`           |
| `SNOWFLAKE_PASSWORD` | Your Snowflake password          | `••••••••`                       |
| `SNOWFLAKE_ACCOUNT` | Full account identifier            | `abc-xy12345.eu-central-1.aws`  |
| `SNOWFLAKE_DATABASE` | Target database name              | `DEEL_ANALYTICS`                |
| `SNOWFLAKE_SCHEMA` | Target schema name                 | `MARTS_PAYMENT`                 |

If you want see data in snowflake, please reach out to me, I can create user for you.

---

### 💬 Slack Setup

To enable Slack alerts:

| Variable           | Description                            | Example                        |
|--------------------|----------------------------------------|--------------------------------|
| `SLACK_BOT_TOKEN`  | Bot token from your Slack app          | `xoxb-1234-56789...`           |
| `SLACK_CHANNEL_ID` | Channel ID, not name                   | `C01ABCDEF`                    |

#### 🔍 How to Find Channel ID
1. Open Slack in your browser.
2. Click the channel.
3. The URL will be like:  
   `https://app.slack.com/client/TXXXXX/C01ABCDEF`
   → `C01ABCDEF` is the **channel ID**.

You are wellcome to my slack to see the balance alerts: https://join.slack.com/share/enQtODkwMzUyODI4NDQzOS00ZmU1ZGUzYzk0NWFmMGQyYzg2N2E5MzMxZmY2NjY3MTAwMjNkMWZmMWQ0YjFjMzRhZjRhMmMyYmM1NTJhNjI5

---

## 🖼️ Screenshots

### ✅ Elementary Test Firing a Slack Alert
![Elementary Slack Alert](/images/elementary.png)

### ✅ Python Alert (CSV Preview)
![Slack CSV Preview](/images/pythonslackalert.png)

### ✅ Airflow DAG Triggered Successfully
![Airflow DAGt](/images/airflowdag.png)


---


