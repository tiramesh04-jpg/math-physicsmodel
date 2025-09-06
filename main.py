# main.py
from flask import Flask, jsonify, request
import os
import time
import math
import statistics
import json
import threading
import requests
import re

# Optional: Google Sheets
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GS_AVAILABLE = True
except Exception:
    GS_AVAILABLE = False

app = Flask(__name__)

# ---------- CONFIG ----------
SENSOR_URL = os.getenv("SENSOR_URL", "https://dataset1st.onrender.com")

# Spec file (URL preferred, fallback to local path)
MACHINE_SPEC_PATH = os.getenv("MACHINE_SPEC_PATH", "machine_spec.json")
MACHINE_SPEC_URL = os.getenv("MACHINE_SPEC_URL", None)

WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "300"))
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", None)  # JSON string or path
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", None)
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "Sheet1")

# thresholds and tuning
VIB_THRESHOLD = float(os.getenv("VIB_THRESHOLD", "0.4"))
DELTA_T_CRIT = float(os.getenv("DELTA_T_CRIT", "30.0"))
RUL_K_EXP = float(os.getenv("RUL_K_EXP", "4.0"))

# weights for final score
W_VIB, W_TEMP, W_TORQUE, W_POWER = 0.4, 0.3, 0.2, 0.1

# ---------- Helper: parse number with units ----------
def parse_number(value, default=0.0):
    """
    Extract float and detect units from strings like '2.4 Nm', '15 kW', '60:1', '5000 hours'.
    Returns (number_in_SI, unit_string)
    """
    try:
        if value is None:
            return default, None
        if isinstance(value, (int, float)):
            return float(value), None

        text = str(value).strip()
        # quick cleanup
        text = text.replace(",", "").strip()

        # detect gear ratio format separately
        if ":" in text and re.match(r"\s*\d+(\.\d+)?\s*:\s*\d+(\.\d+)?", text):
            # return left side as ratio number (e.g., '60:1' -> 60.0)
            left = text.split(":")[0].strip()
            try:
                return float(left), "ratio"
            except Exception:
                return default, None

        # Match number and optional unit
        m = re.match(r"^([-+]?\d*\.?\d+)\s*(.*)$", text)
        if not m:
            return default, None

        number = float(m.group(1))
        unit = m.group(2).strip().lower()

        if not unit:
            return number, None

        # Normalize known units into SI
        if "kw" in unit:
            return number * 1000.0, "W"
        if unit in ("w", "watt", "watts"):
            return number, "W"
        if "nm" in unit:
            return number, "Nm"
        if unit in ("c", "°c", "degc", "celsius"):
            return number, "°C"
        if "%" in unit:
            # return as ratio 0..1
            return number / 100.0, "ratio"
        if "hour" in unit or "hours" in unit:
            return number, "hours"
        # fallback: return unit as-is
        return number, unit
    except Exception:
        return default, None


# ---------- Load spec ----------
def load_spec():
    spec = None
    # try URL first
    if MACHINE_SPEC_URL:
        try:
            r = requests.get(MACHINE_SPEC_URL, timeout=5)
            r.raise_for_status()
            spec = r.json()
            print("Loaded spec from URL")
        except Exception as e:
            print("Spec URL load failed:", e)
    # fallback to local
    if spec is None:
        try:
            with open(MACHINE_SPEC_PATH, "r") as f:
                spec = json.load(f)
                print("Loaded spec from local file")
        except Exception as e:
            print("Failed to load local spec file:", e)
            spec = {}
    return spec

SPEC = load_spec()

rated_torque, _ = parse_number(SPEC.get("mechanical_specs", {}).get("rated_torque", 2.4))
gear_ratio, _ = parse_number(SPEC.get("mechanical_specs", {}).get("gear_ratio", 60.0))
gear_eff, _ = parse_number(SPEC.get("mechanical_specs", {}).get("gear_efficiency", 0.8))
rated_power, _ = parse_number(SPEC.get("electrical_specs", {}).get("rated_power_output", 15.0))
expected_life_hours, _ = parse_number(SPEC.get("performance", {}).get("expected_life", 5000))

# ---------- In-memory sliding window ----------
history = []      # computed metric dicts (most recent last)
raw_history = []  # raw sensor samples (most recent last)
failures = []     # detected failures list
sheet_cache = []  # cached rows mirrored from sheet (newest-first)
lock = threading.Lock()

# ---------- Google Sheets init helper ----------
def init_gs():
    if not GS_AVAILABLE or not GOOGLE_SHEET_NAME:
        return None
    creds = None
    # GOOGLE_CREDS_JSON can be a JSON string or file path
    if GOOGLE_CREDS_JSON:
        # try parse as JSON
        try:
            creds_dict = json.loads(GOOGLE_CREDS_JSON)
            creds = Credentials.from_service_account_info(
                creds_dict,
                scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            )
        except Exception:
            # assume it's a filepath
            try:
                creds = Credentials.from_service_account_file(
                    GOOGLE_CREDS_JSON,
                    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
                )
            except Exception as e:
                print("Google creds parsing/loading error:", e)
                creds = None
    if creds is None:
        return None
    try:
        client = gspread.authorize(creds)
        ws = client.open(GOOGLE_SHEET_NAME).worksheet(GOOGLE_SHEET_TAB)
        return ws
    except Exception as e:
        print("Could not open Google Sheet:", e)
        return None

GS_SHEET = None
try:
    GS_SHEET = init_gs()
except Exception as e:
    print("Could not init Google Sheets:", e)

# ---------- Google Sheets save helper ----------
def save_to_gs(ws, row):
    """
    Ensure header exists and insert new row at position 2 (so newest-first).
    ws: gspread Worksheet
    row: list of cell values
    """
    if not ws:
        return
    try:
        existing = ws.get_all_values()  # small sheets OK; large sheets may be slower

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

        # create or fix headers if needed
        if not existing:
            ws.insert_row(headers, 1)
        elif existing[0] != headers:
            # replace first row with our headers
            try:
                ws.delete_rows(1)
            except Exception:
                # not all backends implement delete_rows; fallback to overwrite
                pass
            ws.insert_row(headers, 1)

        # insert new data in row 2 (below header)
        ws.insert_row(row, 2)
    except Exception as e:
        # log the error but do not crash the poller
        print("GS write error:", e)


# ---------- Unit conversion helper ----------
def auto_convert(value, unit_type="torque"):
    """
    Return (value, unit_label) in SI for common unit types.
    This function assumes parse_number already produced SI numeric for spec strings.
    """
    if unit_type == "torque":
        return value, "Nm"
    if unit_type == "power":
        return value, "W"
    if unit_type == "temperature":
        return value, "°C"
    if unit_type == "vibration":
        return value, "m/s²"
    return value, "unitless"


# ---------- Core compute functions ----------
def compute_metrics_for_sample(sample, torque_scale=None, vib_baseline=1e-6):
    """
    Compute derived metrics and failure indicators for a single sample.
    Returns dict.
    """
    result = {}
    rpm, _ = parse_number(sample.get("rpm", 0.0))
    ambient, _ = parse_number(sample.get("surrounding_temp", sample.get("ambient", 25.0)))
    temp, _ = parse_number(sample.get("temp", ambient))
    torque_sensor, _ = parse_number(sample.get("torque", 0.0))
    vib, _ = parse_number(sample.get("vibration_rms", 0.0))

    # scaling (auto scale may be provided externally)
    if torque_scale is None:
        torque_scaled = torque_sensor
        scale_used = 1.0
        scale_flag = "none"
    else:
        torque_scaled = torque_sensor * torque_scale
        scale_used = torque_scale
        scale_flag = "auto" if torque_scale != 1.0 else "none"

    # kinematic/power calculations
    omega = 2.0 * math.pi * rpm / 60.0
    P_motor = torque_scaled * omega  # W

    # gearbox output
    rpm_out = rpm / (gear_ratio if gear_ratio else 1.0)
    torque_out = torque_scaled * (gear_ratio if gear_ratio else 1.0) * (gear_eff if gear_eff else 1.0)
    P_out = torque_out * (2.0 * math.pi * rpm_out / 60.0)

    # thermal
    delta_T = temp - ambient

    # normalized metrics
    vib_norm = min(vib / (VIB_THRESHOLD if VIB_THRESHOLD else 1e-6), 3.0)
    temp_norm = min(max(delta_T / (DELTA_T_CRIT if DELTA_T_CRIT else 1.0), 0.0), 3.0)
    torque_norm = min(torque_scaled / (rated_torque + 1e-9), 3.0)
    power_norm = min(P_motor / (rated_power + 1e-9), 10.0)

    # RUL proxy based on vibration
    vib_ref = max(vib_baseline, 1e-6)
    RUL_est = expected_life_hours * (vib_ref / max(vib, vib_ref)) ** RUL_K_EXP

    # combined raw score and logistic failure probability
    raw = W_VIB * (vib_norm / 3.0) + W_TEMP * (temp_norm / 3.0) + \
          W_TORQUE * min(torque_norm / 2.0, 1.0) + W_POWER * min(power_norm / 3.0, 1.0)
    k = 8.0
    offset = 0.5
    failure_prob = 1.0 / (1.0 + math.exp(-k * (raw - offset)))

    # warnings detection
    warnings = []
    try:
        max_temp, _ = parse_number(
            SPEC.get("performance", {}).get("operating_temperature_range", "+60").split("to")[-1]
        )
    except Exception:
        max_temp = 60
    if temp > max_temp:
        warnings.append("operating_temp_exceeded")
    if delta_T > DELTA_T_CRIT:
        warnings.append("high_delta_temperature")
    if vib > VIB_THRESHOLD:
        warnings.append("high_vibration_absolute")
    if torque_scaled > (rated_torque * 1.1):
        warnings.append("torque_overload")

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
        "warnings": warnings,
        "scale_used": scale_used,
        "scale_flag": scale_flag
    })

    if warnings:
        failures.append({
            "ts": result["ts"],
            "failure_probability": failure_prob,
            "reasons": warnings,
            "details": result
        })

    return result


# ---------- Helper: compute sliding-window baseline & scale ----------
def compute_baselines_and_scale(raw_window):
    """
    raw_window: list of raw sample dicts (most recent last)
    Returns: (torque_scale or None, vib_baseline)
    """
    if not raw_window:
        return None, 1e-6
    torques = [parse_number(x.get("torque", 0.0))[0] for x in raw_window]
    vibs = [parse_number(x.get("vibration_rms", 0.0))[0] for x in raw_window]
    median_torque_sensor = statistics.median(torques) if torques else 0.0
    mean_vib = statistics.mean(vibs) if vibs else 1e-6
    scale = None
    if median_torque_sensor > 0:
        scale_candidate = rated_torque / median_torque_sensor
        if 0.01 <= abs(scale_candidate) <= 10:
            scale = scale_candidate
    vib_baseline = max(mean_vib, 1e-6)
    return scale, vib_baseline


# ---------- Background poller ----------
def poll_and_compute():
    """
    Polls SENSOR_URL, computes metrics, stores to history and optionally saves to Google Sheet.
    Runs in a background thread.
    """
    while True:
        try:
            r = requests.get(SENSOR_URL, timeout=10)
            # if response is empty or not JSON, handle gracefully
            try:
                r.raise_for_status()
            except Exception:
                print("Polling HTTP error/status:", getattr(r, "status_code", None))
                time.sleep(2.0)
                continue

            # parse JSON safely
            try:
                samples = r.json()
            except ValueError:
                # non-JSON response
                print("Polling error: non-JSON response:", r.text[:200])
                time.sleep(2.0)
                continue

            if isinstance(samples, dict):
                samples = [samples]
            if not isinstance(samples, list):
                print("Polling error: unexpected data type from sensor endpoint")
                time.sleep(2.0)
                continue

            now = time.time()
            with lock:
                # append raw samples and prune by WINDOW_SECONDS
                for s in samples:
                    raw_history.append(s)
                cutoff = now - WINDOW_SECONDS
                raw_history[:] = [x for x in raw_history if parse_number(x.get("ts", now))[0] >= cutoff]

                # compute dynamic baselines and torque scale
                torque_scale, vib_baseline = compute_baselines_and_scale(raw_history)

                # compute metrics for incoming samples
                for s in samples:
                    metrics = compute_metrics_for_sample(s, torque_scale=torque_scale, vib_baseline=vib_baseline)
                    history.append(metrics)

                # keep bounded
                if len(history) > 1000:
                    history[:] = history[-1000:]

                # persist latest to Google Sheet (insert at top) and update sheet_cache newest-first
                if GS_SHEET is not None and history:
                    try:
                        last = history[-1]
                        # prepare row values (primitive scalar values for sheet)
                        row = [
                            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last["ts"])),
                            last.get("rpm"),
                            last.get("torque_scaled")[0] if isinstance(last.get("torque_scaled"), (list, tuple)) else last.get("torque_scaled"),
                            last.get("P_motor")[0] if isinstance(last.get("P_motor"), (list, tuple)) else last.get("P_motor"),
                            last.get("vibration_rms")[0] if isinstance(last.get("vibration_rms"), (list, tuple)) else last.get("vibration_rms"),
                            last.get("temp")[0] if isinstance(last.get("temp"), (list, tuple)) else last.get("temp"),
                            round(last.get("failure_probability", 0.0), 6),
                            ",".join(last.get("warnings", []))
                        ]
                        # insert at top (below header)
                        save_to_gs(GS_SHEET, row)
                        # maintain local cache newest-first
                        sheet_cache.insert(0, row)
                        # trim cache
                        if len(sheet_cache) > 2000:
                            sheet_cache[:] = sheet_cache[:2000]
                    except Exception as e:
                        print("GS write error:", e)

        except Exception as e:
            print("Polling error:", e)
        # short sleep to avoid tight loop; sensor endpoint controls cadence
        time.sleep(2.0)

# start poller thread
threading.Thread(target=poll_and_compute, daemon=True).start()


# ---------- Homepage ----------
@app.route("/")
def home():
    sheet_link = ""
    if GOOGLE_SHEET_NAME:
        # a human cannot open by name; include env var hint
        sheet_link = f"<p>Google Sheet name (env): <b>{GOOGLE_SHEET_NAME}</b></p>"
    html = f"""
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
    {sheet_link}
    <p>Sensor URL: <code>{SENSOR_URL}</code></p>
    """
    return html


# ---------- REST endpoints ----------
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
        return jsonify(history[-n:])


@app.route("/api/failures", methods=["GET"])
def get_failures():
    n = int(request.args.get("n", 20))
    with lock:
        return jsonify(failures[-n:])


@app.route("/api/sheet", methods=["GET"])
def get_sheet_data():
    """
    Return structured JSON with headers + rows. Newest-first.
    """
    if not GS_SHEET:
        return jsonify({"error": "Google Sheet not configured"}), 500
    try:
        rows = GS_SHEET.get_all_values()
        if not rows or len(rows) < 2:
            # if headers empty or no data
            return jsonify({"headers": rows[0] if rows else [], "rows": []})
        headers = rows[0]
        data_rows = rows[1:]  # sheet rows below header, newest-first because we insert at row 2
        # convert to list of dicts
        data = [dict(zip(headers, r)) for r in data_rows]
        return jsonify({"headers": headers, "rows": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/reload_spec", methods=["POST"])
def reload_spec():
    global SPEC, rated_torque, gear_ratio, gear_eff, rated_power, expected_life_hours
    try:
        SPEC = load_spec()
        rated_torque, _ = parse_number(SPEC.get("mechanical_specs", {}).get("rated_torque", 2.4))
        gear_ratio, _ = parse_number(SPEC.get("mechanical_specs", {}).get("gear_ratio", 60.0))
        gear_eff, _ = parse_number(SPEC.get("mechanical_specs", {}).get("gear_efficiency", 0.8))
        rated_power, _ = parse_number(SPEC.get("electrical_specs", {}).get("rated_power_output", 15.0))
        expected_life_hours, _ = parse_number(SPEC.get("performance", {}).get("expected_life", 5000))
        return jsonify({"status": "ok", "spec": SPEC})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
