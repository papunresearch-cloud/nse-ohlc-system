"""
ALERT NOTIFICATION ENGINE (LIVE MARKET DISPATCHER)
- Runs strictly during live market hours (09:15 - 15:30 IST).
- Direct SSL Email delivery via port 465 (bypasses Render/cloud network blocks).
- Real-time Telegram Bot messaging via official HTTPS API (replaces CallMeBot/WhatsApp).
- Evaluates Hi/Lo breakout conditions from /param and /watchlist/detailedDb.
- Enforces 24-hour single-alert per stock rule (auto-resets at 00:00 IST).
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import requests
import pytz
from firebase_admin import db

from config import (
    TIMEZONE,
    SMTP_SERVER,
    SMTP_SSL_PORT,
    SENDER_EMAIL,
    SENDER_APP_PASSWORD,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    logger
)
from firebase_manager import init_firebase, sanitize_key

IST = pytz.timezone(TIMEZONE)


def send_email_alert(recipient_emails: list[str], stock_code: str, alert_type: str, cmp_val: float, boundary_val: float):
    """
    Sends alert email via direct SSL (port 465).
    Bypasses standard cloud firewall blocks on port 25 and 587.
    """
    if not recipient_emails:
        return

    if not SENDER_EMAIL or not SENDER_APP_PASSWORD:
        logger.warning("[ALERT-EMAIL] SENDER_EMAIL or SENDER_APP_PASSWORD missing. Skipping email.")
        return

    subject = f"🚨 NSE Alert: {stock_code} {alert_type} Triggered (₹{cmp_val:.2f})"
    body_text = (
        f"STOCK ALERT: {stock_code}\n"
        f"Trigger: {alert_type}\n"
        f"Current Market Price (CMP): ₹{cmp_val:.2f}\n"
        f"Target Threshold: ₹{boundary_val:.2f}\n"
        f"Timestamp: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}\n"
    )

    for to_addr in recipient_emails:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = SENDER_EMAIL
            msg["To"] = to_addr
            msg.attach(MIMEText(body_text, "plain"))

            # Port 465 explicit SSL socket
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_SSL_PORT, timeout=15) as server:
                server.login(SENDER_EMAIL, SENDER_APP_PASSWORD)
                server.sendmail(SENDER_EMAIL, to_addr, msg.as_string())

            logger.info(f"[ALERT-EMAIL] Sent {alert_type} email for {stock_code} to {to_addr}")
        except Exception as e:
            logger.error(f"[ALERT-EMAIL] Failed sending to {to_addr} for {stock_code}: {e}")


def send_telegram_alert(target_chat_ids: list[str], stock_code: str, alert_type: str, cmp_val: float, boundary_val: float):
    """
    Sends instant Telegram push notifications via the official Telegram Bot API.
    Does not suffer from bans, rate limits, or approval queues.
    """
    token = TELEGRAM_BOT_TOKEN.strip() if TELEGRAM_BOT_TOKEN else ""
    if not token:
        logger.warning("[ALERT-TELEGRAM] TELEGRAM_BOT_TOKEN is not configured. Skipping.")
        return

    # Combine recipient IDs from parameter/UI and environment
    recipients = set(target_chat_ids)
    if TELEGRAM_CHAT_ID:
        recipients.add(str(TELEGRAM_CHAT_ID).strip())

    if not recipients:
        logger.warning("[ALERT-TELEGRAM] No recipient Chat IDs configured.")
        return

    icon = "🚀" if alert_type == "HIGH_BREAKOUT" else "🔻"
    message = (
        f"{icon} *STOCK ALERT: {stock_code}*\n\n"
        f"• *Signal:* `{alert_type}`\n"
        f"• *CMP:* ₹`{cmp_val:.2f}`\n"
        f"• *Target Limit:* ₹`{boundary_val:.2f}`\n"
        f"• *Time:* `{datetime.now(IST).strftime('%H:%M:%S IST')}`\n"
    )

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    for chat_id in recipients:
        clean_chat_id = str(chat_id).strip()
        if not clean_chat_id:
            continue
        try:
            payload = {
                "chat_id": clean_chat_id,
                "text": message,
                "parse_mode": "Markdown"
            }
            res = requests.post(url, json=payload, timeout=10)
            if res.status_code == 200:
                logger.info(f"[ALERT-TELEGRAM] Telegram alert delivered for {stock_code} to Chat ID {clean_chat_id}")
            else:
                logger.warning(f"[ALERT-TELEGRAM] Telegram API responded {res.status_code}: {res.text}")
        except Exception as e:
            logger.error(f"[ALERT-TELEGRAM] Failed sending Telegram message to {clean_chat_id}: {e}")


def evaluate_market_alerts():
    """
    Core Evaluation Engine:
    1. Checks global master_disable switch.
    2. Resolves enabled email and Telegram recipients.
    3. Evaluates price boundary triggers (CMP vs limits or Hi/Lo flags).
    4. Enforces 24-hour single-alert per stock rule.
    5. Dispatches notifications and records state.
    """
    init_firebase()
    try:
        alerts_root = db.reference("alerts").get() or {}
        if not isinstance(alerts_root, dict):
            return

        # 1. Global Kill Switch Check
        if bool(alerts_root.get("master_disable", False)):
            logger.debug("[ALERT-EVAL] Alerts globally disabled (master_disable=True). Skipping.")
            return

        # 2. Extract enabled emails
        emails_dict = alerts_root.get("emails", {})
        active_emails = []
        if isinstance(emails_dict, dict):
            for item in emails_dict.values():
                if isinstance(item, dict) and item.get("enabled", True):
                    email_addr = item.get("email", "").strip()
                    if email_addr and "@" in email_addr:
                        active_emails.append(email_addr)

        # 3. Extract enabled messaging IDs (from whatsapp/telegram node in UI)
        contact_dict = alerts_root.get("whatsapp", {}) or {}
        active_chat_ids = []
        if isinstance(contact_dict, dict):
            for item in contact_dict.values():
                if isinstance(item, dict) and item.get("enabled", True):
                    cid = str(item.get("phone", "")).strip()
                    if cid:
                        active_chat_ids.append(cid)

        # If no custom IDs exist in Firebase, fallback to default TELEGRAM_CHAT_ID
        if not active_chat_ids and TELEGRAM_CHAT_ID:
            active_chat_ids.append(str(TELEGRAM_CHAT_ID).strip())

        if not active_emails and not active_chat_ids:
            return

        stock_controls = alerts_root.get("stock_controls", {})
        param_snapshot = db.reference("param").get() or {}
        detailed_snapshot = db.reference("watchlist/detailedDb").get() or {}

        today_str = datetime.now(IST).strftime("%Y-%m-%d")

        for stock_code, param_data in param_snapshot.items():
            if not isinstance(param_data, dict):
                continue

            safe_code = sanitize_key(stock_code)

            # Per-stock toggle check (defaults to True)
            stock_cfg = stock_controls.get(safe_code) or stock_controls.get(stock_code) or {}
            if stock_cfg.get("enabled") is False:
                continue

            # Current Market Price check
            cmp_val = param_data.get("CMP")
            if cmp_val is None or not isinstance(cmp_val, (int, float)) or cmp_val <= 0:
                continue

            stock_details = detailed_snapshot.get(safe_code) or detailed_snapshot.get(stock_code) or {}
            p_high = stock_details.get("preset_high")
            p_low = stock_details.get("preset_low")

            hi_flag = param_data.get("Hi") is True
            lo_flag = param_data.get("Lo") is True

            alert_type = None
            boundary_val = 0.0

            # Trigger condition: High Breakout
            if (p_high is not None and cmp_val > float(p_high) and float(p_high) < 9999999) or hi_flag:
                alert_type = "HIGH_BREAKOUT"
                boundary_val = float(p_high) if p_high is not None else float(cmp_val)

            # Trigger condition: Low Breakdown
            elif (p_low is not None and cmp_val < float(p_low) and float(p_low) > 0) or lo_flag:
                alert_type = "LOW_BREAKDOWN"
                boundary_val = float(p_low) if p_low is not None else float(cmp_val)

            if not alert_type:
                continue

            # Enforce 24-hour lockout memory
            stock_alert_state = alerts_root.get(safe_code, {})
            last_date = stock_alert_state.get("last_alert_date")

            if last_date == today_str:
                # Already alerted today for this stock; suppress
                continue

            logger.info(f"[ALERT-TRIGGER] Firing {alert_type} for {safe_code}: CMP={cmp_val} vs Target={boundary_val}")
            
            # Dispatch notifications
            send_email_alert(active_emails, safe_code, alert_type, cmp_val, boundary_val)
            send_telegram_alert(active_chat_ids, safe_code, alert_type, cmp_val, boundary_val)

            # Record dispatched state to prevent duplicate alerts today
            db.reference(f"alerts/{safe_code}").update({
                "cmp": cmp_val,
                "latched_state": "HI" if alert_type == "HIGH_BREAKOUT" else "LO",
                "boundary_value": boundary_val,
                "last_alert_date": today_str,
                "updated_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
            })

    except Exception as e:
        logger.error(f"[ALERT-ENGINE-ERROR] Evaluation pass failed: {e}", exc_info=True)