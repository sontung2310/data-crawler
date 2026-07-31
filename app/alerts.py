"""Operator alerts: logs + optional email on session expiry."""
from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage

from config import (
    ALERT_EMAIL_FROM,
    ALERT_EMAIL_TO,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_USE_TLS,
    SMTP_USER,
)
from exceptions import SessionExpiredError

logger = logging.getLogger(__name__)

__all__ = ["SessionExpiredError", "notify_session_expired"]


def notify_session_expired(source: str, message: str) -> None:
    logger.error("session_expired source=%s msg=%s", source, message)
    _send_email(
        subject=f"[Data-Crawler] session expired: {source}",
        body=(
            f"Source: {source}\n\n{message}\n\n"
            "Refresh browser cookies in .env and restart if needed."
        ),
    )


def _send_email(*, subject: str, body: str) -> None:
    if not (ALERT_EMAIL_TO and SMTP_HOST and ALERT_EMAIL_FROM):
        return
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = ALERT_EMAIL_FROM
        msg["To"] = ALERT_EMAIL_TO
        msg.set_content(body)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as smtp:
            if SMTP_USE_TLS:
                smtp.starttls()
            if SMTP_USER and SMTP_PASSWORD:
                smtp.login(SMTP_USER, SMTP_PASSWORD)
            smtp.send_message(msg)
    except Exception as exc:
        logger.warning("alert email failed: %s", exc)
