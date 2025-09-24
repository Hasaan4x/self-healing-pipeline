# 🛠️ Self-Healing Data Pipeline (Open-Meteo → Postgres)

This project implements a **resilient, self-healing data pipeline**.  
It ingests real hourly weather data from [Open-Meteo](https://open-meteo.com/), lands it into PostgreSQL, validates quality, decides remediation actions (dedupe, quarantine, allow-with-log), and stores a clean dataset.  

It also includes:
- **Audit logging** for every stage  
- **Slack & Email alerts** for anomalies  
- **Streamlit dashboard** for visibility  
- **Cron job scheduling** for automation  

---

## 🚀 Features

- **Ingestion**: Pull hourly weather → normalize into “order-like” schema (`customer_id = city`, `amount = temperature`)  
- **Validation**: Detect nulls, duplicates, and schema drift  
- **Decision Engine**: Choose remediation actions (quarantine, dedupe, allow with logs, or block)  
- **Remediation**: Apply chosen actions, write to `orders_clean` or quarantine table  
- **Audit Trail**: Every run logs to `audit_events`  
- **Observability**: Streamlit dashboard, Slack/email notifications  
- **Automation**: Cron scheduling with safe logging  

---

## 🧰 Tech Stack

- **Python**: pandas, SQLAlchemy, requests, python-dotenv  
- **Database**: PostgreSQL (Docker)  
- **Admin UI**: Adminer (via Docker)  
- **Dashboard**: Streamlit  
- **Alerts**: Slack Incoming Webhook + SMTP email  
- **Scheduler**: cron  

---

## ⚡ Quickstart

- **1) Clone & enter project**
  ```bash
  git clone https://github.com/<your-username>/self-healing-pipeline.git
  cd self-healing-pipeline


# Start services

docker compose up -d


Postgres → localhost:5432

Adminer → http://localhost:8080

(host: db, user: hasaan, pass: dev, db: healing)

# Python environment

python3.9 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


# Configure .env

cp .env.example .env
 Edit .env with:
 DB_URL=postgresql+psycopg2://hasaan:dev@localhost:5432/healing
 SLACK_WEBHOOK_URL=...
 SMTP_HOST=smtp.gmail.com
 SMTP_PORT=587
 SMTP_USER=your.email@gmail.com
 SMTP_PASS=app_password
 ALERT_EMAIL_FROM=your.email@gmail.com
 ALERT_EMAIL_TO=alerts+pipeline@gmail.com


# Initialize schema

psql "postgresql://hasaan:dev@localhost:5432/healing" -f sql/schema.sql


# Run once (with alerts)

python -m app.run_scheduled


# Dashboard

python -m streamlit run dashboard/app.py


Open → http://localhost:8501


