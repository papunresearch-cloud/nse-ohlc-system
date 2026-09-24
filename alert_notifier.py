"""
ALERT NOTIFICATION ENGINE (LIVE MARKET DISPATCHER)
- Runs during live market hours (09:15 - 15:30 IST).
- Inspects Firebase /alerts (master_disable, stock_controls, emails, whatsapp).
- Reads live CMP and preset boundaries from /param and /watchlist/detailedDb.
- Evaluates Hi/Lo breakout conditions:
    * CMP > preset_high (or Hi == True)
    * CMP < preset_low  (or Lo == True)
- Enforces strict 24-hour single-alert per stock rule (auto-resets at 00:00 IST).
- Dispatches SMTP Emails and WhatsApp notifications concurrently.
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
    SMTP_PORT,
    SENDER_EMAIL,
    SENDER_APP_PASSWORD,
    CALLMEBOT_API_KEY,
    logger
)
from firebase_manager import init_firebase, sanitize_key

IST = pytz.timezone(TIMEZONE)


def send_email_alert(recipient_emails: list[str], stock_code: str, alert_type: str, cmp_val: float, boundary_val: float):
    """Sends HTML & Plaintext alert email via SMTP to all enabled email addresses."""
    if not recipient_emails or not SENDER_EMAIL or not SENDER_APP_PASSWORD:
        return

    subject = f"🚨 NSE Alert: {stock_code} {alert_type} Triggered (₹{cmp_val:.2f})"
    body_plain = (
        f"STOCK ALERT: {stock_code}\n"
        f"Type: {alert_type}\n"
        f"Current Market Price (CMP): ₹{cmp_val:.2f}\n"
        f"Target Threshold: ₹{boundary_val:.2f}\n"
        f"Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}\n"
    )

    for to_addr in recipient_emails:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = SENDER_EMAIL
            msg["To"] = to_addr
            msg.attach(MIMEText(body_plain, "plain"))

            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=15) as server:
                server.starttls()
                server.login(SENDER_EMAIL, SENDER_APP_PASSWORD)
                server.sendmail(SENDER_EMAIL, to_addr, msg.as_string())

            logger.info(f"[ALERT-EMAIL] Sent {alert_type} email for {stock_code} to {to_addr}")
        except Exception as e:
            logger.error(f"[ALERT-EMAIL] Failed sending email to {to_addr} for {stock_code}: {e}")


def send_whatsapp_alert(phone_numbers: list[str], stock_code: str, alert_type: str, cmp_val: float, boundary_val: float):
    """Sends instant alert message to 10-digit Indian WhatsApp numbers via CallMeBot gateway."""
    if not phone_numbers:
        return

    message = (
        f"🚨 *STOCK ALERT: {stock_code}*\n"
        f"Trigger: *{alert_type}*\n"
        f"CMP: *₹{cmp_val:.2f}*\n"
        f"Target: *₹{boundary_val:.2f}*\n"
        f"Time: {datetime.now(IST).strftime('%H:%M:%S IST')}"
    )

    for phone in phone_numbers:
        # Standardize 10-digit Indian number with +91 country prefix
        clean_phone = phone.strip()
        if len(clean_phone) == 10 and clean_phone.isdigit():
            clean_phone = f"+91{clean_phone}"
        elif clean_phone.startswith("91") and len(clean_phone) == 12:
            clean_phone = f"+{clean_phone}"

        url = f"https://api.callmebot.com/whatsapp.php?phone={clean_phone}&text={requests.utils.quote(message)}&apikey={CALLMEBOT_API_KEY}"
        try:
            res = requests.get(url, timeout=12)
            if res.status_code == 200:
                logger.info(f"[ALERT-WHATSAPP] WhatsApp sent for {stock_code} to {clean_phone}")
            else:
                logger.warning(f"[ALERT-WHATSAPP] Gateway HTTP {res.status_code} for {clean_phone}: {res.text}")
        except Exception as e:
            logger.error(f"[ALERT-WHATSAPP] Failed WhatsApp dispatch to {clean_phone}: {e}")


def evaluate_market_alerts():
    """
    Main evaluation routine:
    1. Checks global master_disable.
    2. Gathers active recipients.
    3. Evaluates price boundary triggers against 24-hour lockout memory.
    4. Dispatches alerts and updates /alerts/<CODE>.
    """
    init_firebase()
    try:
        # 1. Fetch alert configurations
        alerts_root = db.reference("alerts").get() or {}
        if not isinstance(alerts_root, dict):
            return

        # Global Kill Switch Check
        if bool(alerts_root.get("master_disable", False)):
            logger.debug("[ALERT-EVAL] Alerts globally disabled (master_disable=True). Skipping.")
            return

        # 2. Extract enabled email recipients
        emails_dict = alerts_root.get("emails", {})
        active_emails = []
        if isinstance(emails_dict, dict):
            for item in emails_dict.values():
                if isinstance(item, dict) and item.get("enabled", True):
                    email_addr = item.get("email", "").strip()
                    if email_addr and "@" in email_addr:
                        active_emails.append(email_addr)

        # 3. Extract enabled WhatsApp recipients
        wa_dict = alerts_root.get("whatsapp", {})
        active_phones = []
        if isinstance(wa_dict, dict):
            for item in wa_dict.values():
                if isinstance(item, dict) and item.get("enabled", True):
                    phone_no = str(item.get("phone", "")).strip()
                    if phone_no:
                        active_phones.append(phone_no)

        if not active_emails and not active_phones:
            logger.debug("[ALERT-EVAL] No active email or WhatsApp recipients configured.")
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

            # Fetch CMP and target values
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

            # Condition: CMP > Preset High OR Hi == True
            if (p_high is not None and cmp_val > float(p_high) and float(p_high) < 9999999) or hi_flag:
                alert_type = "HIGH_BREAKOUT"
                boundary_val = float(p_high) if p_high is not None else float(cmp_val)

            # Condition: CMP < Preset Low OR Lo == True
            elif (p_low is not None and cmp_val < float(p_low) and float(p_low) > 0) or lo_flag:
                alert_type = "LOW_BREAKDOWN"
                boundary_val = float(p_low) if p_low is not None else float(cmp_val)

            if not alert_type:
                continue

            # Check 24-hour lockout memory
            stock_alert_state = alerts_root.get(safe_code, {})
            last_date = stock_alert_state.get("last_alert_date")

            if last_date == today_str:
                # Already alerted today for this stock; suppress duplicate
                continue

            # Dispatch notifications
            logger.info(f"[ALERT-TRIGGER] Firing {alert_type} for {safe_code}: CMP={cmp_val} vs Target={boundary_val}")
            send_email_alert(active_emails, safe_code, alert_type, cmp_val, boundary_val)
            send_whatsapp_alert(active_phones, safe_code, alert_type, cmp_val, boundary_val)

            # Update memory in Firebase
            db.reference(f"alerts/{safe_code}").update({
                "cmp": cmp_val,
                "latched_state": "HI" if alert_type == "HIGH_BREAKOUT" else "LO",
                "boundary_value": boundary_val,
                "last_alert_date": today_str,
                "updated_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
            })

    except Exception as e:
        logger.error(f"[ALERT-ENGINE-ERROR] Evaluation pass failed: {e}", exc_info=True)