#!/usr/bin/env python3
"""
Cleaned-up main.py for Machine Health Monitoring
Key fixes applied:
- Removed duplicate flusher thread starts
- Use explicit float(ts) handling for timestamps (no parse_number misuse)
- Normalized Google Sheet header comparison
- Added locks around all shared structure mutations (including sheet_cache)
- Clamped logistic argument to avoid OverflowError
- Single reliable GS flush strategy (periodic flusher only)
- Consistent spec keys and safer reload logic
- Improved defensive logging and error messages
- Optional DEBUG mode via ENV (DEBUG=true)
"""

from flask import Flask, jsonify, request, render_template_string
import os
import time
import math
import statistics
import json
import threading
import requests
import re
import traceback

# Optional Google Sheets - use google.oauth2 Credentials (preferred)
try:
    import gspread
    from google.oauth2.service_account import Credentials as GoogleCredentials
    GS_AVAILABLE = True
except Exception:
    GS_AVAILABLE = False

app = Flask(__name__)

# =========================
# CONFIG (environment-driven)
# =========================
SENSOR_URL = os.getenv("SENSOR_URL", "https://dataset1st.onrender.com/api/data")
MACHINE_SPEC_PATH = os.getenv("MACHINE_SPEC_PATH", "machine_spec.json")
MACHINE_SPEC_URL = os.getenv("MACHINE_SPEC_URL", None)

WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "300"))  # sliding window length
GS_FLUSH_INTERVAL = int(os.getenv("GS_FLUSH_INTERVAL", "60"))  # seconds between periodic flushes
GS_BATCH_THRESHOLD = int(os.getenv("GS_BATCH_THRESHOLD", "10"))  # flush when this many rows buffered

GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", None)  # either a JSON string or file path
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", None)
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "Sheet1")

# thresholds and tuning
VIB_THRESHOLD = float(os.getenv("VIB_THRESHOLD", "0.4"))
DELTA_T_CRIT = float(os.getenv("DELTA_T_CRIT", "30.0"))
RUL_K_EXP = float(os.getenv("RUL_K_EXP", "4.0"))

# weights for final score
W_VIB, W_TEMP, W_TORQUE, W_POWER = 0.4, 0.3, 0.2, 0.1

# Poll cadence fallback (poller sleeps between loops, sensor controls cadence)
POLL_SLEEP = float(os.getenv("POLL_SLEEP", "2.0"))

DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

# =========================
# Utilities: parse numbers and units
# =========================

def parse_number(value, default=0.0):
    """
    Extract float and approximate unit from strings like '2.4 Nm', '15 kW', '60:1', '5000 hours'.
    Returns (number, unit_string_or_None). Always returns a float for number or default on error.
    """
    try:
        if value is None:
            return float(default), None
        if isinstance(value, (int, float)):
            return float(value), None

        text = str(value).strip()
        text = text.replace(",", "").strip()

        # gear ratio like "60:1"
        if ":" in text and re.match(r"^\s*\d+(?:\.\d+)?\s*:\s*\d+(?:\.\d+)?\s*$", text):
            left = text.split(":")[0].strip()
            return float(left), "ratio"

        m = re.match(r"^([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s*(.*)$", text)
        if not m:
            return float(default), None

        number = float(m.group(1))
        unit = m.group(2).strip().lower()

        if unit == "":
            return number, None

        if "kw" in unit:
            return number * 1000.0, "W"
        if unit in ("w", "watt", "watts"):
            return number, "W"
        if "nm" in unit:
            return number, "Nm"
        if unit in ("c", "°c", "degc", "celsius"):
            return number, "°C"
        if "%" in unit:
            return number / 100.0, "ratio"
        if "hour" in unit or "hr" in unit:
            return number, "hours"
        if "rpm" in unit:
            return number, "rpm"

        return number, unit or None
    except Exception:
        return float(default), None


# =========================
# Spec loading (URL first, then local file)
# =========================

def load_spec():
    spec = None
    if MACHINE_SPEC_URL:
        try:
            r = requests.get(MACHINE_SPEC_URL, timeout=5)
            r.raise_for_status()
            spec = r.json()
            print("Loaded machine spec from URL:", MACHINE_SPEC_URL)
        except Exception as e:
            print("Spec URL load failed:", e)
    if spec is None:
        try:
            with open(MACHINE_SPEC_PATH, "r") as f:
                spec = json.load(f)
                print("Loaded machine spec from local file:", MACHINE_SPEC_PATH)
        except Exception as e:
            print("Failed to load local spec file:", e)
            spec = {}
    return spec


SPEC = load_spec()

# helper to read spec values safely with fallback
def spec_get(path_list, default=None):
    cur = SPEC
    try:
        for k in path_list:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                return default
        return cur
    except Exception:
        return default

# Pull default spec values (use parse_number to allow strings like "2.4 Nm")
rated_torque, _ = parse_number(spec_get(["mechanical_specs", "rated_torque"], 2.4))
gear_ratio, _ = parse_number(spec_get(["mechanical_specs", "gear_ratio"], 60.0))
gear_eff, _ = parse_number(spec_get(["mechanical_specs", "gear_efficiency"], 0.8))
rated_power, _ = parse_number(spec_get(["electrical_specs", "rated_power_output"], 15.0))
expected_life_hours, _ = parse_number(spec_get(["performance", "expected_life"], 5000))

# =========================
# In-memory structures (thread-safe via lock)
# =========================
history = []        # computed metric dicts (most recent last)
raw_history = []    # raw sensor samples (most recent last)
failures = []       # detected failure events (append-only)
sheet_cache = []    # mirrored rows inserted into Google Sheet (newest-first)
lock = threading.Lock()

# =========================
# Google Sheets (buffered writer)
# =========================
GS_SHEET = None          # gspread Worksheet object or None
GS_BUFFER = []           # newest-first buffer of rows (list of lists)
GS_LAST_FLUSH = time.time()
GS_AVAILABLE_FLAG = GS_AVAILABLE  # store availability flag


def normalize_headers(headers):
    return [str(h).strip().lower() for h in (headers or [])]


def init_gs():
    """Initialize Google Sheets access if possible."""
    global GS_SHEET
    if not GS_AVAILABLE_FLAG or not GOOGLE_SHEET_NAME:
        print("Google Sheets not configured or library unavailable.")
        return None

    creds = None
    if GOOGLE_CREDS_JSON:
        # Try JSON blob
        try:
            creds_dict = json.loads(GOOGLE_CREDS_JSON)
            creds = GoogleCredentials.from_service_account_info(
                creds_dict,
                scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            )
            print("Loaded Google service account from JSON string.")
        except Exception:
            try:
                creds = GoogleCredentials.from_service_account_file(
                    GOOGLE_CREDS_JSON,
                    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
                )
                print("Loaded Google service account from file path.")
            except Exception as e:
                print("Google creds parsing/loading error:", e)
                creds = None

    if creds is None:
        print("No valid Google credentials found in GOOGLE_CREDS_JSON; skipping Google Sheets init.")
        return None

    try:
        client = gspread.authorize(creds)
        ws = client.open(GOOGLE_SHEET_NAME).worksheet(GOOGLE_SHEET_TAB)

        headers = [
            "Timestamp",
            "RPM",
            "Torque (Nm)",
            "Power (W)",
            "Vibration (m/s²)",
            "Temperature (°C)",
            "Failure Probability",
            "Warnings"
        ]

        existing = ws.get_all_values()
        if not existing:
            try:
                ws.append_row(headers)
            except Exception:
                pass
        else:
            # normalize and compare
            if normalize_headers(existing[0]) != normalize_headers(headers):
                try:
                    # try to replace first row
                    try:
                        ws.delete_rows(1)
                        ws.insert_row(headers, 1)
                    except Exception:
                        ws.update('A1', [headers])
                except Exception:
                    print("Could not normalize/replace sheet headers; leaving as-is.")

        print("Google Sheet opened:", GOOGLE_SHEET_NAME, "/", GOOGLE_SHEET_TAB)
        return ws
    except Exception as e:
        print("Could not open Google Sheet:", e)
        return None

# initialize GS once at startup
try:
    GS_SHEET = init_gs()
except Exception as e:
    print("Google Sheets init error:", e)
    GS_SHEET = None


def save_to_gs_buffer(row):
    """Add a row (list) to GS_BUFFER newest-first."""
    global GS_BUFFER
    with lock:
        GS_BUFFER.insert(0, row)


def flush_gs_buffer(ws):
    """Flush GS_BUFFER to the Google Sheet (ws) by appending rows."""
    global GS_BUFFER, GS_LAST_FLUSH
    if not ws:
        return
    with lock:
        buffer_copy = list(reversed(GS_BUFFER))  # oldest-first for append order
        if not buffer_copy:
            return
        GS_BUFFER = []  # optimistic clear
    try:
        if hasattr(ws, "append_rows"):
            ws.append_rows(buffer_copy, value_input_option="RAW")
        else:
            for r in buffer_copy:
                try:
                    ws.append_row(r)
                except Exception:
                    pass
        GS_LAST_FLUSH = time.time()
        print(f"Flushed {len(buffer_copy)} rows to Google Sheets at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(GS_LAST_FLUSH))}")
    except Exception as e:
        print("GS batch write error:", e)
        # restore on failure
        with lock:
            for r in reversed(buffer_copy):
                GS_BUFFER.insert(0, r)


def gs_periodic_flusher():
    while True:
        try:
            if GS_SHEET:
                with lock:
                    count = len(GS_BUFFER)
                if count >= GS_BATCH_THRESHOLD or (time.time() - GS_LAST_FLUSH) >= GS_FLUSH_INTERVAL:
                    flush_gs_buffer(GS_SHEET)
        except Exception as e:
            print("GS flusher exception:", e)
        time.sleep(max(1.0, GS_FLUSH_INTERVAL / 4.0))

# start flusher thread if sheet is configured
if GS_SHEET:
    threading.Thread(target=gs_periodic_flusher, daemon=True).start()

# =========================
# Unit conversion helper
# =========================
def auto_convert(value, unit_type="torque"):
    """
    Return (value, unit_label) in SI for common unit types.
    """
    units = {"torque": "Nm", "power": "W", "temperature": "°C", "vibration": "m/s²"}
    return value, units.get(unit_type, "unitless")


# =========================
# Core computation: metrics & warnings
# =========================

def compute_metrics_for_sample(sample, torque_scale=None, vib_baseline=1e-6):
    try:
        result = {}
        rpm, _ = parse_number(sample.get("rpm", 0.0))
        ambient, _ = parse_number(sample.get("surrounding_temp", sample.get("ambient", 25.0)))
        temp, _ = parse_number(sample.get("temp", ambient))
        torque_sensor, _ = parse_number(sample.get("torque", 0.0))
        vib, _ = parse_number(sample.get("vibration_rms", 0.0))

        # scaling
        if torque_scale is None:
            torque_scaled = torque_sensor
            scale_used = 1.0
            scale_flag = "none"
        else:
            torque_scaled = torque_sensor * torque_scale
            scale_used = torque_scale
            scale_flag = "auto" if abs(torque_scale - 1.0) > 1e-9 else "none"

        # kinematic/power calculations
        omega = 2.0 * math.pi * rpm / 60.0 if rpm else 0.0
        P_motor = torque_scaled * omega  # W

        # gearbox output (approx)
        rr = gear_ratio if gear_ratio else 1.0
        re = gear_eff if gear_eff else 1.0
        rpm_out = rpm / rr
        torque_out = torque_scaled * rr * re
        P_out = torque_out * (2.0 * math.pi * rpm_out / 60.0)

        delta_T = temp - ambient

        vib_norm = min(vib / (VIB_THRESHOLD if VIB_THRESHOLD else 1e-6), 3.0)
        temp_norm = min(max(delta_T / (DELTA_T_CRIT if DELTA_T_CRIT else 1.0), 0.0), 3.0)
        torque_norm = min(torque_scaled / (rated_torque + 1e-9), 3.0)
        power_norm = min(P_motor / (rated_power + 1e-9), 10.0)

        vib_ref = max(vib_baseline, 1e-6)
        RUL_est = expected_life_hours * (vib_ref / max(vib, vib_ref)) ** RUL_K_EXP

        raw = W_VIB * (vib_norm / 3.0) + W_TEMP * (temp_norm / 3.0) + \
              W_TORQUE * min(torque_norm / 2.0, 1.0) + W_POWER * min(power_norm / 3.0, 1.0)

        # clamp z to avoid overflow in exp
        k = 8.0
        offset = 0.5
        z = k * (raw - offset)
        z = max(min(z, 50.0), -50.0)
        failure_prob = 1.0 / (1.0 + math.exp(-z))

        warnings_list = []
        try:
            max_temp_candidate = spec_get(["performance", "operating_temperature_range"], "+60")
            if isinstance(max_temp_candidate, str) and "to" in max_temp_candidate:
                max_temp_str = max_temp_candidate.split("to")[-1]
            else:
                max_temp_str = str(max_temp_candidate)
            max_temp, _ = parse_number(max_temp_str)
            if max_temp is None:
                max_temp = 60
        except Exception as e:
            if DEBUG:
                print("Error parsing max temp from spec:", e)
            max_temp = 60

        if temp > max_temp:
            warnings_list.append("operating_temp_exceeded")
        if delta_T > DELTA_T_CRIT:
            warnings_list.append("high_delta_temperature")
        if vib > VIB_THRESHOLD:
            warnings_list.append("high_vibration_absolute")
        if torque_scaled > (rated_torque * 1.1):
            warnings_list.append("torque_overload")

        result.update({
            "ts": sample.get("ts", time.time()),
            "rpm": rpm,
            "rpm_output": rpm_out,
            "torque_sensor": auto_convert(torque_sensor, "torque"),
            "torque_scaled": auto_convert(torque_scaled, "torque"),
            "torque_output": auto_convert(torque_out, "torque"),
            "P_motor": auto_convert(P_motor, "power"),
            "P_out": auto_convert(P_out, "power"),
            "temp": auto_convert(temp, "temperature"),
            "ambient": auto_convert(ambient, "temperature"),
            "delta_T": auto_convert(delta_T, "temperature"),
            "vibration_rms": auto_convert(vib, "vibration"),
            "vib_norm": vib_norm,
            "temp_norm": temp_norm,
            "torque_norm": torque_norm,
            "power_norm": power_norm,
            "RUL_hours": RUL_est,
            "failure_probability": failure_prob,
            "warnings": warnings_list,
            "scale_used": scale_used,
            "scale_flag": scale_flag
        })

        if warnings_list:
            with lock:
                failures.append({
                    "ts": result["ts"],
                    "failure_probability": failure_prob,
                    "reasons": warnings_list,
                    "details": result
                })

        return result
    except Exception as e:
        print("compute_metrics_for_sample error:", e)
        traceback.print_exc()
        return {
            "ts": sample.get("ts", time.time()),
            "rpm": 0,
            "failure_probability": 0.0,
            "warnings": ["compute_error"]
        }


# =========================
# Baseline & auto-scale computation
# =========================

def compute_baselines_and_scale(raw_window):
    if not raw_window:
        return None, 1e-6
    try:
        torques = [parse_number(x.get("torque", 0.0))[0] for x in raw_window]
        vibs = [parse_number(x.get("vibration_rms", 0.0))[0] for x in raw_window]
        median_torque_sensor = statistics.median([t for t in torques if t is not None]) if torques else 0.0
        mean_vib = statistics.mean([v for v in vibs if v is not None]) if vibs else 1e-6
        scale = None
        if median_torque_sensor and median_torque_sensor > 0:
            scale_candidate = rated_torque / median_torque_sensor
            if 0.01 <= abs(scale_candidate) <= 10:
                scale = scale_candidate
        vib_baseline = max(mean_vib, 1e-6)
        return scale, vib_baseline
    except Exception as e:
        print("compute_baselines_and_scale error:", e)
        return None, 1e-6


# =========================
# Background poller: fetch sensor JSON -> compute -> store -> buffer to GS
# =========================

def poll_and_compute():
    global GS_SHEET, GS_LAST_FLUSH
    while True:
        try:
            r = requests.get(SENSOR_URL, timeout=10)
            try:
                r.raise_for_status()
            except Exception:
                if DEBUG:
                    print("Polling HTTP error/status:", getattr(r, "status_code", None))
                time.sleep(POLL_SLEEP)
                continue

            try:
                samples = r.json()
            except ValueError:
                if DEBUG:
                    print("Polling error: non-JSON response length:", len(r.text) if r.text else 0)
                time.sleep(POLL_SLEEP)
                continue

            if isinstance(samples, dict):
                samples = [samples]
            if not isinstance(samples, list):
                if DEBUG:
                    print("Polling error: unexpected data type from sensor endpoint:", type(samples))
                time.sleep(POLL_SLEEP)
                continue

            now = time.time()
            with lock:
                for s in samples:
                    # ensure ts exists and is numeric
                    if "ts" not in s:
                        s["ts"] = now
                    else:
                        try:
                            s_ts = float(s.get("ts", now))
                            s["ts"] = s_ts
                        except Exception:
                            s["ts"] = now
                    raw_history.append(s)

                cutoff = now - WINDOW_SECONDS
                raw_history[:] = [x for x in raw_history if float(x.get("ts", now)) >= cutoff]

                torque_scale, vib_baseline = compute_baselines_and_scale(raw_history)

                for s in samples:
                    metrics = compute_metrics_for_sample(s, torque_scale=torque_scale, vib_baseline=vib_baseline)
                    history.append(metrics)

                    try:
                        # extract numeric part when auto_convert returned tuple
                        def _num(v):
                            return v[0] if isinstance(v, (list, tuple)) else v

                        row = [
                            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(metrics.get("ts", now)))),
                            metrics.get("rpm"),
                            _num(metrics.get("torque_scaled")),
                            _num(metrics.get("P_motor")),
                            _num(metrics.get("vibration_rms")),
                            _num(metrics.get("temp")),
                            round(float(metrics.get("failure_probability", 0.0)), 6),
                            ",".join(metrics.get("warnings", []))
                        ]
                        save_to_gs_buffer(row)
                        with lock:
                            sheet_cache.insert(0, row)
                            if len(sheet_cache) > 2000:
                                sheet_cache[:] = sheet_cache[:2000]
                    except Exception as e:
                        print("Error preparing GS row:", e)

                if len(history) > 1000:
                    history[:] = history[-1000:]

            time.sleep(POLL_SLEEP)
        except Exception as e:
            print("Polling exception:", e)
            traceback.print_exc()
            time.sleep(POLL_SLEEP)

# start poller thread
threading.Thread(target=poll_and_compute, daemon=True).start()

# =========================
# Homepage (status)
# =========================
@app.route("/")
def home():
    with lock:
        last_result_time = float(history[-1]["ts"]) if history else None
        history_count = len(history)
        raw_count = len(raw_history)
        failures_count = len(failures)
        buffer_len = len(GS_BUFFER)
    last_fetch_readable = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_result_time)) if last_result_time else "No data yet"
    sheet_status = f"{buffer_len} rows in buffer, last flush at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(GS_LAST_FLUSH))}" if GS_SHEET else "Google Sheet not configured"
    return render_template_string(f"""
    <h2>🚀 Machine Health Monitoring API</h2>
    <p>Available endpoints:</p>
    <ul>
        <li><a href='/api/metrics'>/api/metrics</a> – Latest computed metrics</li>
        <li><a href='/api/predict'>/api/predict</a> – Predict failure probabilities (recent window)</li>
        <li><a href='/api/history'>/api/history</a> – Last N computed samples</li>
        <li><a href='/api/failures'>/api/failures</a> – Recorded failures</li>
        <li><a href='/api/sheet'>/api/sheet</a> – Google Sheet data (JSON)</li>
        <li><a href='/api/reload_spec'>/api/reload_spec</a> – Reload machine spec</li>
    </ul>

    <h3>Status</h3>
    <ul>
        <li>✅ Last sensor fetch time: {last_fetch_readable}</li>
        <li>✅ Results in memory: {history_count}</li>
        <li>✅ Raw samples cached: {raw_count}</li>
        <li>✅ Failures recorded: {failures_count}</li>
        <li>✅ Google Sheet: {sheet_status}</li>
        <li>⚙️ Spec model: {SPEC.get('product', {}).get('model', 'unknown')}</li>
    </ul>
    """)

# =========================
# REST endpoints
# =========================
@app.route("/api/metrics", methods=["GET"])
def get_metrics():
    with lock:
        if not history:
            return jsonify({"error": "no metrics yet"}), 404
        return jsonify(history[-1])


@app.route("/api/predict", methods=["GET"])
def get_predict():
    with lock:
        if not history:
            return jsonify({"error": "no metrics yet"}), 404
        last = history[-1]
        return jsonify({
            "machine_id": SPEC.get("product", {}).get("model", "unknown"),
            "ts": last["ts"],
            "failure_probability": last["failure_probability"],
            "RUL_hours": last["RUL_hours"],
            "warnings": last["warnings"],
            "details": last
        })


@app.route("/api/history", methods=["GET"])
def get_history():
    n = int(request.args.get("n", 50))
    with lock:
        start = max(len(history) - n, 0)
        return jsonify(history[start:])


@app.route("/api/failures", methods=["GET"])
def get_failures():
    n = int(request.args.get("n", 20))
    with lock:
        return jsonify(failures[-n:])


@app.route("/api/sheet", methods=["GET"])
def get_sheet_data():
    if not GS_SHEET:
        return jsonify({"error": "Google Sheet not configured"}), 500
    try:
        rows = GS_SHEET.get_all_values()
        if not rows or len(rows) < 2:
            return jsonify({"headers": rows[0] if rows else [], "rows": []})
        headers = rows[0]
        data_rows = rows[1:]
        data = [dict(zip(headers, r)) for r in data_rows]
        return jsonify({"headers": headers, "rows": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/reload_spec", methods=["POST"])
def reload_spec():
    global SPEC, rated_torque, gear_ratio, gear_eff, rated_power, expected_life_hours
    try:
        SPEC = load_spec()
        rated_torque, _ = parse_number(spec_get(["mechanical_specs", "rated_torque"], 2.4))
        gear_ratio, _ = parse_number(spec_get(["mechanical_specs", "gear_ratio"], 60.0))
        gear_eff, _ = parse_number(spec_get(["mechanical_specs", "gear_efficiency"], 0.8))
        rated_power, _ = parse_number(spec_get(["electrical_specs", "rated_power_output"], 15.0))
        expected_life_hours, _ = parse_number(spec_get(["performance", "expected_life"], 5000))
        return jsonify({"status": "ok", "spec": SPEC})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/flush_gs", methods=["POST"])
def api_flush_gs():
    if not GS_SHEET:
        return jsonify({"error": "Google Sheet not configured"}), 500
    try:
        flush_gs_buffer(GS_SHEET)
        with lock:
            remaining = len(GS_BUFFER)
        return jsonify({"status": "ok", "flushed": True, "remaining_buffer": remaining})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sheet_cache", methods=["GET"])
def api_sheet_cache():
    with lock:
        return jsonify({"rows_cached": len(sheet_cache), "sample": sheet_cache[:50]})


@app.route("/api/ingest", methods=["POST"])
def api_ingest():
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "no JSON body"}), 400

    now = time.time()
    data.setdefault("ts", now)
    try:
        data["ts"] = float(data["ts"])
    except Exception:
        data["ts"] = now

    with lock:
        raw_history.append(data)
        cutoff = now - WINDOW_SECONDS
        raw_history[:] = [x for x in raw_history if float(x.get("ts", now)) >= cutoff]

        torque_scale, vib_baseline = compute_baselines_and_scale(raw_history)
        metrics = compute_metrics_for_sample(data, torque_scale=torque_scale, vib_baseline=vib_baseline)
        history.append(metrics)
        if len(history) > 1000:
            history[:] = history[-1000:]

        if GS_SHEET:
            try:
                def _num(v):
                    return v[0] if isinstance(v, (list, tuple)) else v

                row = [
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(metrics.get("ts", now)))),
                    metrics.get("rpm"),
                    _num(metrics.get("torque_scaled")),
                    _num(metrics.get("P_motor")),
                    _num(metrics.get("vibration_rms")),
                    _num(metrics.get("temp")),
                    round(float(metrics.get("failure_probability", 0.0)), 6),
                    ",".join(metrics.get("warnings", []))
                ]
                save_to_gs_buffer(row)
                with lock:
                    sheet_cache.insert(0, row)
                    if len(sheet_cache) > 2000:
                        sheet_cache[:] = sheet_cache[:2000]
            except Exception as e:
                print("Ingest GS buffer error:", e)

    return jsonify({"status": "ok", "metrics": metrics})


# =========================
# Run server
# =========================
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug_flag = DEBUG
    app.run(host="0.0.0.0", port=port, debug=debug_flag)
