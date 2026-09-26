"""
ALERT NOTIFICATION ENGINE (TELEGRAM DIRECT API)
- Runs during live market hours (09:15 - 15:30 IST).
- Evaluates Hi/Lo triggers from /param and /watchlist/detailedDb.
- Dedicated cooldown sub-node: /alerts/<CODE>/dispatch/last_dispatched_date.
- Automatic midnight re-arm (calendar date comparison).
"""

from datetime import datetime
import requests
import pytz
from firebase_admin import db

from config import (
    TIMEZONE,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    logger
)
from firebase_manager import init_firebase, sanitize_key

IST = pytz.timezone(TIMEZONE)


def send_telegram_alert(target_chat_ids: list, stock_code: str, alert_type: str, cmp_val: float, boundary_val: float):
    """Sends Markdown formatted alert message via official Telegram Bot API."""
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("[ALERT-TELEGRAM] TELEGRAM_BOT_TOKEN is not set. Skipping.")
        return

    # Strictly filter for valid numeric Telegram Chat IDs (drop raw phone numbers)
    recipients = set()
    for cid in target_chat_ids:
        clean_cid = str(cid).strip()
        if clean_cid.isdigit() and len(clean_cid) in [8, 9, 10]:
            recipients.add(clean_cid)

    if TELEGRAM_CHAT_ID and str(TELEGRAM_CHAT_ID).strip().isdigit():
        recipients.add(str(TELEGRAM_CHAT_ID).strip())

    if not recipients:
        logger.warning("[ALERT-TELEGRAM] No valid numeric Telegram Chat IDs found.")
        return

    icon = "🚀" if alert_type == "HIGH_BREAKOUT" else "🔻"
    message = (
        f"{icon} *STOCK ALERT: {stock_code}*\n\n"
        f"• *Signal:* `{alert_type}`\n"
        f"• *CMP:* ₹`{cmp_val:.2f}`\n"
        f"• *Target Limit:* ₹`{boundary_val:.2f}`\n"
        f"• *Time:* `{datetime.now(IST).strftime('%H:%M:%S IST')}`\n"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    for chat_id in recipients:
        try:
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown"
            }
            res = requests.post(url, json=payload, timeout=10)
            if res.status_code == 200:
                logger.info(f"[ALERT-TELEGRAM] Delivered alert for {stock_code} to Chat ID {chat_id}")
            else:
                logger.warning(f"[ALERT-TELEGRAM] Failed for Chat ID {chat_id} (HTTP {res.status_code}): {res.text}")
        except Exception as e:
            logger.error(f"[ALERT-TELEGRAM] Network error sending to Chat ID {chat_id}: {e}")


def evaluate_market_alerts():
    """
    Scans Firebase for price breaches and dispatches Telegram notifications.
    """
    init_firebase()
    try:
        alerts_root = db.reference("alerts").get() or {}
        if not isinstance(alerts_root, dict):
            return

        # 1. Global Master Switch
        if bool(alerts_root.get("master_disable", False)):
            logger.debug("[ALERT-EVAL] Alerts globally disabled (master_disable=True). Skipping.")
            return

        # 2. Extract enabled Chat IDs from Firebase alerts modal
        active_chat_ids = []
        contacts_dict = alerts_root.get("whatsapp", {}) or {}
        if isinstance(contacts_dict, dict):
            for item in contacts_dict.values():
                if isinstance(item, dict) and item.get("enabled", True):
                    cid = str(item.get("phone", "")).strip()
                    if cid.isdigit() and len(cid) in [8, 9, 10]:
                        active_chat_ids.append(cid)

        # Fallback to TELEGRAM_CHAT_ID from config
        if not active_chat_ids and TELEGRAM_CHAT_ID:
            active_chat_ids.append(str(TELEGRAM_CHAT_ID).strip())

        if not active_chat_ids:
            return

        stock_controls = alerts_root.get("stock_controls", {})
        param_snapshot = db.reference("param").get() or {}
        detailed_snapshot = db.reference("watchlist/detailedDb").get() or {}

        today_str = datetime.now(IST).strftime("%Y-%m-%d")

        for stock_code, param_data in param_snapshot.items():
            if not isinstance(param_data, dict):
                continue

            safe_code = sanitize_key(stock_code)

            # Check individual stock toggle
            stock_cfg = stock_controls.get(safe_code) or stock_controls.get(stock_code) or {}
            if stock_cfg.get("enabled") is False:
                continue

            # Verify CMP
            cmp_val = param_data.get("CMP")
            if cmp_val is None or not isinstance(cmp_val, (int, float)) or cmp_val <= 0:
                continue

            # Retrieve target thresholds and flags
            stock_details = detailed_snapshot.get(safe_code) or detailed_snapshot.get(stock_code) or {}
            p_high = stock_details.get("preset_high")
            p_low = stock_details.get("preset_low")

            hi_flag = param_data.get("Hi") is True
            lo_flag = param_data.get("Lo") is True

            alert_type = None
            boundary_val = 0.0

            # Condition 1: High Breakout
            if (p_high is not None and cmp_val > float(p_high) and float(p_high) < 9999999) or hi_flag:
                alert_type = "HIGH_BREAKOUT"
                boundary_val = float(p_high) if p_high is not None else float(cmp_val)

            # Condition 2: Low Breakdown
            elif (p_low is not None and cmp_val < float(p_low) and float(p_low) > 0) or lo_flag:
                alert_type = "LOW_BREAKDOWN"
                boundary_val = float(p_low) if p_low is not None else float(cmp_val)

            if not alert_type:
                continue

            # Check dedicated cooldown sub-node
            stock_dispatch_node = (alerts_root.get(safe_code) or {}).get("dispatch", {})
            last_date = stock_dispatch_node.get("last_dispatched_date")

            if last_date == today_str:
                continue  # Suppressed: Already alerted today

            logger.info(f"[ALERT-TRIGGER] Firing {alert_type} for {safe_code}: CMP={cmp_val} vs Target={boundary_val}")

            # Send Telegram alert
            send_telegram_alert(active_chat_ids, safe_code, alert_type, cmp_val, boundary_val)

            # Lock alert for today in dedicated /dispatch sub-node
            db.reference(f"alerts/{safe_code}/dispatch").update({
                "last_dispatched_date": today_str,
                "last_signal": alert_type,
                "dispatched_price": cmp_val,
                "boundary_value": boundary_val,
                "updated_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
            })

    except Exception as e:
        logger.error(f"[ALERT-ENGINE-ERROR] Evaluation pass failed: {e}", exc_info=True)