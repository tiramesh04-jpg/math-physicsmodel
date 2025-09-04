# main.py
from flask import Flask, jsonify, request
import os, time, math, statistics, json, threading, requests, re

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
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", None)
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
    Returns (number_in_SI, unit_string).
    """
    try:
        if value is None:
            return default, None
        if isinstance(value, (int, float)):
            return float(value), None

        text = str(value).strip()
        unit = None
        number = None

        # Match number and optional unit
        match = re.match(r"([-+]?\d*\.?\d+)(.*)", text)
        if match:
            number = float(match.group(1))
            unit = match.group(2).strip()

        if not unit:
            return number, None

        # Normalize units
        unit = unit.lower()
        if "kw" in unit:
            return number * 1000.0, "W"
        if unit in ["w", "watt", "watts"]:
            return number, "W"
        if "nm" in unit:
            return number, "Nm"
        if "c" in unit or "°c" in unit:
            return number, "°C"
        if "%" in unit:
            return number / 100.0, "ratio"
        if "hour" in unit:
            return number, "hours"
        if ":" in text:  # gear ratio like 60:1
            return float(text.split(":")[0]), "ratio"

        return number, unit
    except Exception:
        return default, None


# ---------- Load spec ----------
def load_spec():
    spec = None
    if MACHINE_SPEC_URL:
        try:
            r = requests.get(MACHINE_SPEC_URL, timeout=5)
            r.raise_for_status()
            spec = r.json()
            print("Loaded spec from URL")
        except Exception as e:
            print("Spec URL load failed, falling back to local:", e)
    if spec is None:
        with open(MACHINE_SPEC_PATH, "r") as f:
            spec = json.load(f)
            print("Loaded spec from local file")
    return spec


SPEC = load_spec()

rated_torque, _ = parse_number(SPEC.get("mechanical_specs", {}).get("rated_torque", 2.4))
gear_ratio, _ = parse_number(SPEC.get("mechanical_specs", {}).get("gear_ratio", 60.0))
gear_eff, _ = parse_number(SPEC.get("mechanical_specs", {}).get("gear_efficiency", 0.8))
rated_power, _ = parse_number(SPEC.get("electrical_specs", {}).get("rated_power_output", 15.0))
expected_life_hours, _ = parse_number(SPEC.get("performance", {}).get("expected_life", 5000))


# ---------- In-memory sliding window ----------
history = []
raw_history = []
failures = []  # store failures with reasons
sheet_cache = []  # store data pushed to Google Sheet
lock = threading.Lock()


# ---------- Google Sheets init helper ----------
def init_gs():
    if not GS_AVAILABLE or not GOOGLE_SHEET_NAME:
        return None
    creds = None
    if GOOGLE_CREDS_JSON:
        try:
            creds_dict = json.loads(GOOGLE_CREDS_JSON)
            creds = Credentials.from_service_account_info(
                creds_dict,
                scopes=["https://www.googleapis.com/auth/spreadsheets",
                        "https://www.googleapis.com/auth/drive"]
            )
        except Exception:
            creds = Credentials.from_service_account_file(
                GOOGLE_CREDS_JSON,
                scopes=["https://www.googleapis.com/auth/spreadsheets",
                        "https://www.googleapis.com/auth/drive"]
            )
    if creds is None:
        return None
    client = gspread.authorize(creds)
    return client.open(GOOGLE_SHEET_NAME).worksheet(GOOGLE_SHEET_TAB)


GS_SHEET = None
try:
    GS_SHEET = init_gs()
except Exception as e:
    print("Could not init Google Sheets:", e)


# ---------- Unit conversion helper ----------
def auto_convert(value, unit_type="torque"):
    """
    Standardize value into SI units based on unit_type.
    Returns (value_in_SI, unit).
    """
    if unit_type == "torque":  # Nm
        return value, "Nm"
    if unit_type == "power":  # Watt
        return value, "W"
    if unit_type == "temperature":  # Celsius
        return value, "°C"
    if unit_type == "vibration":  # m/s²
        return value, "m/s²"
    return value, "unitless"


# ---------- Core compute functions ----------
def compute_metrics_for_sample(sample, torque_scale=None, vib_baseline=1e-6):
    result = {}
    rpm, _ = parse_number(sample.get("rpm", 0.0))
    ambient, _ = parse_number(sample.get("surrounding_temp", sample.get("ambient", 25.0)))
    temp, _ = parse_number(sample.get("temp", ambient))
    torque_sensor, _ = parse_number(sample.get("torque", 0.0))
    vib, _ = parse_number(sample.get("vibration_rms", 0.0))

    if torque_scale is None:
        torque_scaled = torque_sensor
        scale_used = 1.0
        scale_flag = "none"
    else:
        torque_scaled = torque_sensor * torque_scale
        scale_used = torque_scale
        scale_flag = "auto" if torque_scale != 1.0 else "none"

    omega = 2.0 * math.pi * rpm / 60.0
    P_motor = torque_scaled * omega

    rpm_out = rpm / gear_ratio
    torque_out = torque_scaled * gear_ratio * gear_eff
    P_out = torque_out * (2.0 * math.pi * rpm_out / 60.0)

    delta_T = temp - ambient

    vib_norm = min(vib / VIB_THRESHOLD, 3.0)
    temp_norm = min(max(delta_T / DELTA_T_CRIT, 0.0), 3.0)
    torque_norm = min(torque_scaled / (rated_torque + 1e-9), 3.0)
    power_norm = min(P_motor / (rated_power + 1e-9), 10.0)

    vib_ref = max(vib_baseline, 1e-6)
    RUL_est = expected_life_hours * (vib_ref / max(vib, vib_ref)) ** RUL_K_EXP

    raw = W_VIB * (vib_norm/3.0) + W_TEMP * (temp_norm/3.0) + \
          W_TORQUE * min(torque_norm/2.0, 1.0) + W_POWER * min(power_norm/3.0, 1.0)
    k = 8.0; offset = 0.5
    failure_prob = 1.0 / (1.0 + math.exp(-k*(raw - offset)))

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
    if vib > VIB_THRESHOLD or vib > (vib_baseline + 3e-9):
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
    if not raw_window:
        return None, 1e-6
    torques = [parse_number(x.get("torque",0.0))[0] for x in raw_window]
    vibs = [parse_number(x.get("vibration_rms",0.0))[0] for x in raw_window]
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
    while True:
        try:
            r = requests.get(SENSOR_URL, timeout=6)
            r.raise_for_status()
            samples = r.json()
            if isinstance(samples, dict):
                samples = [samples]
            now = time.time()
            with lock:
                for s in samples:
                    raw_history.append(s)
                cutoff = now - WINDOW_SECONDS
                raw_history[:] = [x for x in raw_history if parse_number(x.get("ts", now))[0] >= cutoff]
                torque_scale, vib_baseline = compute_baselines_and_scale(raw_history)
                for s in samples:
                    metrics = compute_metrics_for_sample(s, torque_scale=torque_scale, vib_baseline=vib_baseline)
                    history.append(metrics)
                if len(history) > 1000:
                    history[:] = history[-1000:]
                if GS_SHEET is not None and history:
                    try:
                        last = history[-1]
                        row = [
                            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last["ts"])),
                            last["rpm"],
                            last["torque_scaled"][0],
                            last["P_motor"][0],
                            last["vibration_rms"][0],
                            last["temp"][0],
                            last["failure_probability"],
                            ",".join(last["warnings"])
                        ]
                        GS_SHEET.append_row(row)
                        sheet_cache.append(row)
                    except Exception as e:
                        print("GS write error:", e)
        except Exception as e:
            print("Polling error:", e)
        time.sleep(2.0)


threading.Thread(target=poll_and_compute, daemon=True).start()


# ---------- REST endpoints ----------
@app.route("/api/metrics", methods=["GET"])
def get_metrics():
    with lock:
        if not history:
            return jsonify({"error":"no metrics yet"}), 404
        return jsonify(history[-1])


@app.route("/api/predict", methods=["GET"])
def get_predict():
    with lock:
        if not history:
            return jsonify({"error":"no metrics yet"}), 404
        last = history[-1]
        return jsonify({
            "machine_id": SPEC.get("product", {}).get("model","unknown"),
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
    n = int(request.args.get("n", 50))
    with lock:
        return jsonify(sheet_cache[-n:])


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
        return jsonify({"status":"ok","spec":SPEC})
    except Exception as e:
        return jsonify({"error":str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
