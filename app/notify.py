from typing import Optional, Dict, Any
import os, json, ssl, smtplib, requests
from email.message import EmailMessage

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM")
EMAIL_TO   = os.getenv("ALERT_EMAIL_TO")

def slack(title: str, payload: Optional[Dict[str, Any]] = None) -> None:
    """Send a Slack message via incoming webhook."""
    if not SLACK_WEBHOOK:
        return
    text = f"*Self-Healing Pipeline*: {title}"
    if payload:
        try:
            text += f"\n```{json.dumps(payload, indent=2)}```"
        except Exception:
            # fallback if payload contains non-serializable values
            text += f"\n```{str(payload)}```"
    try:
        requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=8)
    except Exception:
        # don't crash pipeline on notify failures
        pass

def email(subject: str, body: str) -> None:
    """Send a plain-text email via SMTP (TLS)."""
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and EMAIL_FROM and EMAIL_TO):
        return
    msg = EmailMessage()
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg["Subject"] = subject
    msg.set_content(body)
    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls(context=ctx)
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

def alert(title: str, payload: Optional[Dict[str, Any]] = None, severity: str = "info") -> None:
    """Send both Slack + Email notifications."""
    prefix = f"[{severity.upper()}] "
    body = json.dumps(payload, indent=2) if payload is not None else title
    slack(prefix + title, payload)
    email(prefix + title, body)
