"""
===============================================================================
MASTER ORCHESTRATOR (Primary Key Architecture: CODE)
===============================================================================
* Coordinates live OHLC backfills, indicators, and HTTP control signals.
* Keyed exclusively by stock CODE across /stocks/<CODE> and /param/<CODE>.
* Listens to /system_commands/stock_event dispatched from Watchlist.jsx.
* Embeds HTTP server on port 10000 with CORS, HEAD, GET, and POST support.
* Self-healing state-machine scheduling: pre-market catch-up, live ticks,
  post-market settlement window, and per-stock quarantine protection.
* Updates real-time Firebase /system_status telemetry on every cycle and heartbeat.
===============================================================================
"""

import os
import sys
import time
import signal
import json
import logging
import threading
from urllib.parse import urlparse
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, time as dtime
import pytz

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 1. Config imports
from config import (
    TIMEZONE,
    PATH_STOCKS,
    PATH_SCRIPTS,
    LIVE_UPDATE_INTERVAL_SEC,
    PULSE_VALIDITY_SEC,
    logger
)

# Benchmark indices definition strictly using synthetic uppercase CODEs
FIXED_INDICES = {
    "NIFTY50": "^NSEI",
    "NIFTY100": "^CNX100",
    "NIFTYMID150": "NIFTYMIDCAP150.NS",
    "NIFTYSM250": "NIFTYSMLCAP250.NS"
}

# 2. Firebase Manager imports
from firebase_manager import (
    init_firebase,
    sanitize_key,
    reconcile_stocklist_with_watchlist,
    get_stocklist_mapping,
    clear_live_candle
)
import firebase_admin
from firebase_admin import db

# 3. Market Calendar
try:
    from market_calendar import MarketCalendar
except ImportError:
    class MarketCalendar:
        def is_trading_day(self, dt=None):
            return True
        def get_market_status(self, dt=None):
            return "CLOSED", "Default fallback"

# 4. Child Workers (sync and live)
try:
    from sync_child import sync_historical_script
except ImportError:
    def sync_historical_script(safe_code, ticker, gap_trading_days=300, calendar=None):
        logger.warning(f"[SYNC] sync_historical_script not available for {safe_code}")
        return False, "Not implemented"

try:
    from live_child import update_live_script
except ImportError:
    def update_live_script(safe_code, ticker):
        pass

# 5. Parameter Calculation Engine
try:
    from parameter import update_all_parameters, calculate_single_script_parameters
except ImportError:
    try:
        from parameter import update_all_parameters
        calculate_single_script_parameters = None
    except ImportError:
        update_all_parameters = None
        calculate_single_script_parameters = None

# 6. Screener Pipeline ETL Runner
try:
    from RUN_PIPELINE import main as run_screener_pipeline
except ImportError:
    try:
        from BASIC import run_pipeline as run_screener_pipeline
    except ImportError:
        try:
            from basic import run_pipeline as run_screener_pipeline
        except ImportError:
            run_screener_pipeline = None

IST = pytz.timezone(TIMEZONE)
_keep_running = True
_screener_lock = threading.Lock()
_sync_lock = threading.Lock()

def handle_exit_signal(sig, frame):
    global _keep_running
    logger.info("[SHUTDOWN] Terminating orchestrator cleanly...")
    _keep_running = False

signal.signal(signal.SIGINT, handle_exit_signal)
signal.signal(signal.SIGTERM, handle_exit_signal)

# =====================================================================
# SYSTEM FLIP-FLOP & PULSE BUS
# =====================================================================
class SystemStateBus:
    def __init__(self):
        self.lock = threading.Lock()
        self.power_latched_on = True
        self.pulse_sync_time = 0.0

    def trigger_pulse(self, command: str):
        now = time.time()
        with self.lock:
            if command == "START":
                self.power_latched_on = True
                logger.info("[SIGNAL] START pulse captured -> Latch ON.")
            elif command == "STOP":
                self.power_latched_on = False
                logger.warning("[SIGNAL] STOP pulse captured -> Latch OFF.")
            elif command == "SYNC":
                self.pulse_sync_time = now
                logger.info("[SIGNAL] MANUAL SYNC pulse captured.")

    def check_and_clear_manual_sync(self) -> bool:
        now = time.time()
        with self.lock:
            if (now - self.pulse_sync_time) <= PULSE_VALIDITY_SEC:
                self.pulse_sync_time = 0.0
                return True
        return False

    def is_power_on(self) -> bool:
        with self.lock:
            return self.power_latched_on

STATE_BUS = SystemStateBus()

def dispatch_screener_sync_job():
    """Runs screener ETL in a background daemon thread with full trace logging."""
    if not run_screener_pipeline:
        logger.error("[SCREENER] Cannot run pipeline: ETL runner not loaded.")
        return False, "ETL runner module not available"

    if not _screener_lock.acquire(blocking=False):
        logger.warning("[SCREENER] Pipeline sync already in progress.")
        return False, "Screener pipeline sync already in progress"

    def worker():
        try:
            logger.info("[SCREENER] Starting Google Drive -> Firebase ETL Pipeline...")
            run_screener_pipeline()
            logger.info("[SCREENER] Pipeline successfully written to Firebase /SCREENER.")
        except Exception as ex:
            logger.error(f"[SCREENER] CRASHED WITH ERROR: {ex}", exc_info=True)
        finally:
            _screener_lock.release()

    threading.Thread(target=worker, daemon=True, name="ScreenerWorker").start()
    return True, "Screener ETL pipeline triggered in background"

# =====================================================================
# HTTP REQUEST HANDLER (Supports GET, POST, HEAD, OPTIONS)
# =====================================================================
class HealthAndControlHandler(BaseHTTPRequestHandler):
    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")

    def _send_json_response(self, code: int, data: dict):
        payload = json.dumps(data).encode("utf-8")
        self.send_response(code)
        self._send_cors_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def do_HEAD(self):
        """Responds 200 OK to keepalive uptime pings."""
        self.send_response(200)
        self._send_cors_headers()
        self.send_header("Content-Type", "application/json")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.lower().rstrip("/")
        if not path:
            path = "/"

        if path in ["/", "/health", "/ping"]:
            status = {
                "status": "RUNNING" if STATE_BUS.is_power_on() else "IDLE",
                "timestamp": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
            }
            self._send_json_response(200, status)

        elif path in ["/sync", "/api/sync"]:
            STATE_BUS.trigger_pulse("SYNC")
            self._send_json_response(200, {"status": "ok", "message": "Historical sync pulse triggered"})

        elif path in ["/sync-screener", "/run-pipeline", "/api/sync-screener"]:
            started, msg = dispatch_screener_sync_job()
            code = 200 if started else 409
            self._send_json_response(code, {"message": msg})

        elif path in ["/start", "/api/start"]:
            STATE_BUS.trigger_pulse("START")
            self._send_json_response(200, {"message": "Master engine started"})

        elif path in ["/stop", "/api/stop"]:
            STATE_BUS.trigger_pulse("STOP")
            self._send_json_response(200, {"message": "Master engine paused"})

        elif path in ["/calc-param", "/api/calc-param"]:
            if update_all_parameters:
                stock_map = get_stocklist_mapping(force_reconcile=True)
                threading.Thread(target=update_all_parameters, args=(list(stock_map.keys()),), daemon=True).start()
                self._send_json_response(200, {"status": "started", "message": "Manual parameter calculation started."})
            else:
                self._send_json_response(500, {"error": "parameter module not loaded"})

        else:
            self._send_json_response(404, {"error": f"Endpoint '{path}' not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.lower().rstrip("/")
        if not path:
            path = "/"

        if path in ["/start", "/api/start"]:
            STATE_BUS.trigger_pulse("START")
            self._send_json_response(200, {"message": "Master engine started"})

        elif path in ["/stop", "/api/stop"]:
            STATE_BUS.trigger_pulse("STOP")
            self._send_json_response(200, {"message": "Master engine paused"})

        elif path in ["/sync", "/api/sync"]:
            STATE_BUS.trigger_pulse("SYNC")
            self._send_json_response(200, {"status": "ok", "message": "Historical sync pulse triggered"})

        elif path in ["/sync-screener", "/run-pipeline", "/api/sync-screener"]:
            started, msg = dispatch_screener_sync_job()
            code = 200 if started else 409
            self._send_json_response(code, {"message": msg})

        elif path in ["/calc-param", "/api/calc-param"]:
            if update_all_parameters:
                stock_map = get_stocklist_mapping(force_reconcile=True)
                threading.Thread(target=update_all_parameters, args=(list(stock_map.keys()),), daemon=True).start()
                self._send_json_response(200, {"status": "started", "message": "Manual parameter calculation started."})
            else:
                self._send_json_response(500, {"error": "parameter module not loaded"})

        else:
            self._send_json_response(404, {"error": f"Endpoint '{path}' not found"})

    def log_message(self, format, *args):
        logger.info(f"[HTTP] {self.command} {self.path} - {format % args}")

def start_http_listener(port=10000):
    port = int(os.environ.get("PORT", port))
    server = HTTPServer(("0.0.0.0", port), HealthAndControlHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True, name="HttpServer")
    t.start()
    logger.info(f"[HTTP] Control and Health server listening on port {port}")

# =====================================================================
# MASTER ORCHESTRATOR
# =====================================================================
class MasterOrchestrator:
    def __init__(self):
        init_firebase()
        self.calendar = MarketCalendar()
        self.script_status = {}
        self.last_stock_event_ts = 0.0
        self.last_heartbeat_time = 0.0
        self.last_planned_date = None
        self.eod_reconciled_today = False
        self.replan_daily_routine()

    def record_heartbeat(self):
        """Updates /system_status with current heartbeat so frontend sees backend as active."""
        try:
            now_epoch = time.time()
            now_ist_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
            power_status = "RUNNING" if STATE_BUS.is_power_on() else "STOPPED"

            db.reference("system_status").update({
                "backend_power": power_status,
                "last_heartbeat": now_ist_str,
                "heartbeat_epoch": now_epoch
            })
            self.last_heartbeat_time = now_epoch
            logger.info(f"[HEARTBEAT] Telemetry updated -> {now_ist_str}")
        except Exception as e:
            logger.error(f"[HEARTBEAT] Failed to update telemetry: {e}")

    def replan_daily_routine(self):
        """Builds target dictionary keyed exclusively by primary key CODE."""
        now_date = datetime.now(IST).date()
        self.last_planned_date = now_date
        self.eod_reconciled_today = False

        stock_map = get_stocklist_mapping(force_reconcile=True)

        # 1. Purge legacy spaced index targets from memory and Firebase
        legacy_keys = [
            "NIFTY MIDCAP 150",
            "NIFTY SMALLCAP 250",
            "NIFTY_MIDCAP_150",
            "NIFTY_SMALLCAP_250"
        ]
        for lk in legacy_keys:
            if lk in stock_map:
                del stock_map[lk]
                try:
                    db.reference(f"stocklist/{lk}").delete()
                    db.reference(f"stocks/{lk}").delete()
                    db.reference(f"param/{lk}").delete()
                    logger.info(f"[PLANNER] Automatically purged legacy index target: {lk}")
                except Exception:
                    pass

        # 2. Merge synthetic fixed market indices
        for k, v in FIXED_INDICES.items():
            stock_map[k] = v

        # Preserve existing sync state if target already verified today
        new_status = {}
        for code, ticker in stock_map.items():
            if code in self.script_status and self.script_status[code].get("synced"):
                new_status[code] = self.script_status[code]
                new_status[code]["ticker"] = ticker
            else:
                new_status[code] = {
                    "synced": False,
                    "last_attempt_at": None,
                    "error": None,
                    "ticker": ticker,
                    "failure_count": 0,
                    "quarantined": False
                }

        self.script_status = new_status
        logger.info(f"[PLANNER] Reconciled stock targets by CODE. Total targets: {len(self.script_status)}")

    def check_stock_event(self) -> tuple[bool, dict]:
        """Listens for realtime commands from Watchlist.jsx at /system_commands/stock_event."""
        try:
            event_ref = db.reference("system_commands/stock_event")
            payload = event_ref.get()
            if isinstance(payload, dict):
                ts = float(payload.get("timestamp", 0))
                if ts > self.last_stock_event_ts:
                    self.last_stock_event_ts = ts
                    return True, payload
        except Exception as e:
            logger.error(f"[EVENT LISTENER] Error checking stock_event node: {e}")
        return False, {}

    def handle_stock_event(self, event: dict):
        action = event.get("action")
        stock_code = event.get("stock")
        ticker = event.get("ticker")

        if not stock_code:
            return

        safe_code = sanitize_key(stock_code)
        logger.info(f"[STOCK EVENT] Received action '{action}' for CODE '{safe_code}'")

        if action == "ADD":
            self.replan_daily_routine()
            resolved_ticker = ticker or f"{safe_code}.NS"
            threading.Thread(
                target=self.sync_single_stock_addition,
                args=(safe_code, resolved_ticker),
                daemon=True,
                name=f"SyncAdd-{safe_code}"
            ).start()

        elif action == "DELETE":
            try:
                db.reference(f"stocks/{safe_code}").delete()
                db.reference(f"param/{safe_code}").delete()
                logger.info(f"[DELETE EVENT] Purged /stocks/{safe_code} and /param/{safe_code}")
            except Exception as e:
                logger.error(f"[DELETE EVENT] Error purging /stocks or /param: {e}")

            try:
                display_ref = db.reference("display_list/stocks")
                current_display = display_ref.get()

                if isinstance(current_display, list):
                    filtered = [s for s in current_display if sanitize_key(str(s)) != safe_code]
                    display_ref.set(filtered if filtered else None)
                    logger.info(f"[DELETE EVENT] Removed {safe_code} from array /display_list/stocks")
                elif isinstance(current_display, dict):
                    db.reference(f"display_list/stocks/{safe_code}").delete()
                    logger.info(f"[DELETE EVENT] Removed {safe_code} from map /display_list/stocks")
            except Exception as e:
                logger.error(f"[DELETE EVENT] Error updating display_list: {e}")

            if safe_code in self.script_status:
                del self.script_status[safe_code]

    def sync_single_stock_addition(self, safe_code: str, ticker: str):
        """Seeds 300 bars, initializes live candle 0, and calculates indicators."""
        logger.info(f"[ON-DEMAND SYNC] Seeding 300 bars for CODE: {safe_code} ({ticker})...")
        try:
            success, msg = sync_historical_script(safe_code, ticker, gap_trading_days=300, calendar=self.calendar)
            if success:
                if safe_code in self.script_status:
                    self.script_status[safe_code]["synced"] = True
                    self.script_status[safe_code]["quarantined"] = False
                logger.info(f"[ON-DEMAND SYNC] Historical 300 bars seeded for {safe_code}: {msg}")
                try:
                    update_live_script(safe_code, ticker)
                except Exception as live_err:
                    logger.warning(f"[ON-DEMAND SYNC] Live candle init skipped for {safe_code}: {live_err}")

                if calculate_single_script_parameters:
                    try:
                        calculate_single_script_parameters(safe_code)
                        logger.info(f"[ON-DEMAND SYNC] /param/{safe_code} populated successfully.")
                    except Exception as param_err:
                        logger.error(f"[ON-DEMAND SYNC] Parameter calc error for {safe_code}: {param_err}")
                elif update_all_parameters:
                    update_all_parameters([safe_code])
                self._commit_sync_telemetry()
            else:
                logger.error(f"[ON-DEMAND SYNC] Historical sync failed for {safe_code}: {msg}")
        except Exception as e:
            logger.error(f"[ON-DEMAND SYNC] Failed syncing {safe_code}: {e}", exc_info=True)

    def execute_historical_sync(self, is_manual: bool = False):
        """Runs OHLC historical sync for targets with quarantine isolation and updates telemetry."""
        if not _sync_lock.acquire(blocking=False):
            logger.warning("[SYNC] A synchronization pass is already running. Skipping concurrent call.")
            return

        try:
            logger.info(f"[SYNC] Starting historical OHLC sync for {len(self.script_status)} targets (Manual={is_manual})...")
            now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

            for safe_code, meta in list(self.script_status.items()):
                if not _keep_running:
                    break

                # Skip already verified scripts unless it is an explicit manual forced run
                if meta.get("synced") and not is_manual:
                    continue

                # Check quarantine status (skip if 3 consecutive failures recorded today)
                if meta.get("quarantined") and not is_manual:
                    logger.info(f"[SYNC] Skipping quarantined ticker {safe_code}.")
                    continue

                ticker = meta.get("ticker") or f"{safe_code}.NS"
                meta["last_attempt_at"] = now_str

                try:
                    success, msg = sync_historical_script(safe_code, ticker, gap_trading_days=300, calendar=self.calendar)
                    if success:
                        meta["synced"] = True
                        meta["error"] = None
                        meta["failure_count"] = 0
                        meta["quarantined"] = False
                        logger.info(f"[SYNC] ✓ {safe_code} ({ticker}): {msg}")
                        try:
                            update_live_script(safe_code, ticker)
                        except Exception as live_err:
                            logger.warning(f"[SYNC] Live init skipped for {safe_code}: {live_err}")
                    else:
                        meta["synced"] = False
                        meta["error"] = msg
                        meta["failure_count"] = meta.get("failure_count", 0) + 1
                        if meta["failure_count"] >= 3:
                            meta["quarantined"] = True
                            logger.warning(f"[SYNC] ✗ {safe_code} failed 3 times. Tagged as QUARANTINED for today.")
                        else:
                            logger.warning(f"[SYNC] ✗ {safe_code} ({ticker}): {msg} (Fail count: {meta['failure_count']})")
                except Exception as e:
                    meta["synced"] = False
                    meta["error"] = str(e)
                    meta["failure_count"] = meta.get("failure_count", 0) + 1
                    logger.error(f"[SYNC] Failed sync for {safe_code}: {e}")

            # Recalculate parameters for all verified scripts
            if update_all_parameters:
                try:
                    active_codes = [c for c, s in self.script_status.items() if s.get("synced")]
                    if active_codes:
                        update_all_parameters(active_codes)
                        logger.info("[SYNC] Parameters recalculated successfully.")
                except Exception as p_err:
                    logger.error(f"[SYNC] Parameter calculation error: {p_err}")

            # Commit health telemetry to Firebase
            self._commit_sync_telemetry()

        finally:
            _sync_lock.release()

    def _commit_sync_telemetry(self):
        """Updates /system_status/sync_data in Firebase so Health Service reflects true state."""
        try:
            now_ist_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
            synced_count = len([c for c, s in self.script_status.items() if s.get("synced")])
            total_count = len(self.script_status)
            status_label = "VERIFIED" if (total_count > 0 and synced_count == total_count) else "IDLE"

            db.reference("system_status/sync_data").update({
                "last_sync_time": now_ist_str,
                "total_registered": total_count,
                "synced_count": synced_count,
                "status": status_label
            })
            logger.info(f"[HEALTH SIGNAL] Updated /system_status/sync_data -> {now_ist_str} (Synced: {synced_count}/{total_count})")
        except Exception as e:
            logger.error(f"[HEALTH SIGNAL] Failed to write sync_data: {e}")

    def execute_live_intraday_cycle(self):
        """Updates live candle (index 0) for active, verified stocks."""
        for safe_code, info in list(self.script_status.items()):
            if not info.get("synced"):
                continue
            ticker = info.get("ticker", f"{safe_code}.NS")
            try:
                update_live_script(safe_code, ticker)
            except Exception as e:
                logger.debug(f"[LIVE CYCLE] Live update skipped for {safe_code}: {e}")

    def execute_eod_reconciliation(self):
        """Post-Market settlement: ensures closed candle is Index 1, clears Index 0, and updates params."""
        logger.info("[EOD-SETTLEMENT] Initiating post-market data reconciliation...")
        try:
            for safe_code, meta in list(self.script_status.items()):
                ticker = meta.get("ticker") or f"{safe_code}.NS"
                try:
                    sync_historical_script(safe_code, ticker, gap_trading_days=5, calendar=self.calendar)
                    clear_live_candle(safe_code)
                except Exception as script_err:
                    logger.warning(f"[EOD-SETTLEMENT] Cleanup skipped for {safe_code}: {script_err}")

            active_codes = [c for c, s in self.script_status.items() if s.get("synced")]
            if active_codes and update_all_parameters:
                update_all_parameters(active_codes)
                logger.info("[EOD-SETTLEMENT] Technical parameters recalculated with finalized closing prices.")

            self.eod_reconciled_today = True
            self._commit_sync_telemetry()
            logger.info("[EOD-SETTLEMENT] Post-market reconciliation finalized.")
        except Exception as e:
            logger.error(f"[EOD-SETTLEMENT] Error during post-market settlement: {e}", exc_info=True)

    def run(self):
        logger.info("[ORCHESTRATOR] Master engine started. Primary key: CODE.")
        last_intraday_tick = 0.0
        last_param_calc_tick = 0.0

        # 1. Initial heartbeat write and boot catch-up sync
        self.record_heartbeat()
        threading.Thread(target=self.execute_historical_sync, kwargs={"is_manual": False}, daemon=True, name="BootSyncWorker").start()

        while _keep_running:
            try:
                now_epoch = time.time()
                now_ist = datetime.now(IST)
                today_date = now_ist.date()
                now_time = now_ist.time()

                # --- 1. Heartbeat Telemetry (Every 300 seconds) ---
                if (now_epoch - self.last_heartbeat_time) >= 300:
                    self.record_heartbeat()

                # --- 2. Realtime Stock Events from Watchlist.jsx (Priority 0) ---
                has_event, event_payload = self.check_stock_event()
                if has_event:
                    self.handle_stock_event(event_payload)

                # --- 3. Manual Sync Override via HTTP (/sync) ---
                if STATE_BUS.check_and_clear_manual_sync():
                    logger.info("[OVERRIDE PULSE] Immediate sync commanded. Running reconciliation & sync...")
                    self.replan_daily_routine()
                    threading.Thread(target=self.execute_historical_sync, kwargs={"is_manual": True}, daemon=True, name="ManualSyncWorker").start()

                # --- 4. Midnight Date Flip Detection ---
                if self.last_planned_date != today_date:
                    logger.info(f"[DATE FLIP] Rollover detected -> Refreshing routine for {today_date} IST.")
                    self.replan_daily_routine()

                # Sleep if master engine power is toggled OFF
                if not STATE_BUS.is_power_on():
                    time.sleep(5.0)
                    continue

                # --- 5. Market Session Evaluation & Routing ---
                status, _ = self.calendar.get_market_status(now_ist) if hasattr(self.calendar, "get_market_status") else ("CLOSED", "")

                # A. LIVE MARKET WINDOW (09:15 – 15:30 IST)
                if status == "LIVE":
                    # Intraday tick cycle (every LIVE_UPDATE_INTERVAL_SEC, e.g. 300s)
                    if (now_epoch - last_intraday_tick) >= LIVE_UPDATE_INTERVAL_SEC:
                        self.execute_live_intraday_cycle()
                        last_intraday_tick = now_epoch

                    # Parameter engine recalculation (every 15 minutes / 900s)
                    if (now_epoch - last_param_calc_tick) >= 900:
                        active_synced = [c for c, s in self.script_status.items() if s.get("synced")]
                        if active_synced and update_all_parameters:
                            threading.Thread(target=update_all_parameters, args=(active_synced,), daemon=True).start()
                        last_param_calc_tick = now_epoch

                # B. POST-MARKET SETTLEMENT WINDOW (15:45 – 16:30 IST)
                elif dtime(15, 45) <= now_time < dtime(16, 30):
                    if not self.eod_reconciled_today and self.calendar.is_trading_day(today_date):
                        threading.Thread(target=self.execute_eod_reconciliation, daemon=True, name="EODWorker").start()

                # C. PRE-MARKET / OFF-HOURS CATCH-UP (Before 09:15 IST)
                elif now_time < dtime(9, 15):
                    has_unsynced_and_unquarantined = any(
                        (not s.get("synced") and not s.get("quarantined")) for s in self.script_status.values()
                    )
                    # Retry unsynced items every 300 seconds if needed
                    if has_unsynced_and_unquarantined and (now_epoch - last_intraday_tick >= 300):
                        logger.info("[SCHEDULE] Pre-market catch-up active. Retrying unsynced targets...")
                        threading.Thread(target=self.execute_historical_sync, kwargs={"is_manual": False}, daemon=True).start()
                        last_intraday_tick = now_epoch

            except Exception as loop_err:
                logger.error(f"[MAIN LOOP EXCEPTION] {loop_err}", exc_info=True)

            time.sleep(2.0)

        logger.info("[ORCHESTRATOR] Master engine stopped cleanly.")

if __name__ == "__main__":
    start_http_listener(port=10000)
    orchestrator = MasterOrchestrator()
    orchestrator.run()