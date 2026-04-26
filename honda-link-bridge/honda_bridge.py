"""
HondaLink Bridge - polls HondaLink CIG API and publishes to MQTT
with Home Assistant auto-discovery. Targets the BEV3 telematics platform
(Honda Prologue, Acura ZDX).

Flow:
    1. HIDAS register + token (one-time per device, ~6mo lifespan).
    2. Exchange HIDAS bearer for a CIG JWT + signature.
    3. Connect to Honda's AWS IoT MQTT WebSocket using the JWT/signature
       as the custom authorizer credentials.
    4. Subscribe to the DASHBOARD_ASYNC named-shadow get/accepted topic.
    5. POST /REST/NGT/CIG/dbd/async to trigger a fresh poll, then wait
       for the shadow message whose requestId matches.
    6. Parse and publish to the user's MQTT broker with HA discovery.
"""

import json
import logging
import os
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Configuration (set via env vars by run.sh)
# ---------------------------------------------------------------------------

HONDA_EMAIL    = os.environ["HONDA_EMAIL"]
HONDA_PASSWORD = os.environ["HONDA_PASSWORD"]
HONDA_PIN      = os.environ.get("HONDA_PIN", "").strip()
VIN            = os.environ["VIN"].strip().upper()
POLL_INTERVAL  = int(os.environ.get("POLL_INTERVAL_SECONDS", "600"))
ENABLE_DAY_NIGHT     = os.environ.get("ENABLE_DAY_NIGHT_SCHEDULE", "false").lower() == "true"
POLL_INTERVAL_DAY    = int(os.environ.get("POLL_INTERVAL_DAY",   "300"))
POLL_INTERVAL_NIGHT  = int(os.environ.get("POLL_INTERVAL_NIGHT", "1800"))
LATITUDE             = float(os.environ.get("LATITUDE",  "0") or "0")
LONGITUDE            = float(os.environ.get("LONGITUDE", "0") or "0")
DAY_START_HOUR       = int(os.environ.get("DAY_START_HOUR", "6"))
DAY_END_HOUR         = int(os.environ.get("DAY_END_HOUR",  "20"))
MQTT_HOST      = os.environ.get("MQTT_HOST", "core-mosquitto")
MQTT_PORT      = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER      = os.environ.get("MQTT_USER") or None
MQTT_PASSWORD  = os.environ.get("MQTT_PASSWORD") or None
DEVICE_NAME    = os.environ.get("DEVICE_NAME", "Honda Prologue")
SUMMER_EFFICIENCY = float(os.environ.get("SUMMER_EFFICIENCY_MI_PER_KWH", "3.5"))
WINTER_EFFICIENCY = float(os.environ.get("WINTER_EFFICIENCY_MI_PER_KWH", "3.0"))
SUMMER_MONTHS = {
    int(m.strip()) for m in os.environ.get("SUMMER_MONTHS", "4,5,6,7,8,9,10").split(",")
    if m.strip().isdigit()
}
LOG_LEVEL      = os.environ.get("LOG_LEVEL", "INFO").upper()
STATE_DIR      = Path(os.environ.get("STATE_DIR", "/data"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("honda-bridge")

# ---------------------------------------------------------------------------
# Honda API constants (reverse-engineered from observed mobile-client traffic)
# ---------------------------------------------------------------------------

HIDAS_BASE            = "https://identity.services.honda.com"
WSC_BASE              = "https://wsc.hondaweb.com"
ANDROID_CLIENT_ID     = "AcuraEVAndroidAppPrOd0083"
ANDROID_CLIENT_SECRET = "q4w5hzeqkFVMPQaeKuil"

AWS_IOT_HOST       = "am7ptks1rwalc-ats.iot.us-east-2.amazonaws.com"
AWS_IOT_AUTHORIZER = "CPSD-IOT-CustAuthorizer-prod"

# Per-endpoint header sets, captured from observed mobile-client traffic.
HDR_MYVEHICLE = {
    "hondaHeaderType.businessId":   "HONDALINK CONNECT",
    "hondaHeaderType.systemId":     "com.honda.dealer.cv_android",
    "hondaHeaderType.siteId":       "00e0e97f0fb543208a918fc946dea334",
    "hondaHeaderType.clientType":   "Mobile",
    "hondaHeaderType.country_code": "US",
    "hondaHeaderType.language_code":"en",
    "hondaHeaderType.version":      "2.0",
}
HDR_CIG_TOKEN = {
    "hondaHeaderType.businessId":   "HONDALINK CONNECT",
    "hondaHeaderType.systemId":     "com.honda.hondalink.cv_android",
    "hondaHeaderType.siteId":       "b407a3025b374f668475e97d2e750816",
    "hondaHeaderType.clientType":   "Mobile",
    "hondaHeaderType.country_code": "US",
    "hondaHeaderType.language_code":"en",
    "hondaHeaderType.version":      "1.0",
}
HDR_CIG_DBD = {
    "hondaHeaderType.businessId":   "HONDALINK CONNECT",
    "hondaHeaderType.systemId":     "com.honda.hondalink.cv_android",
    "hondaHeaderType.siteId":       "8339396b032d4f3cabc98603c46a8775",
    "hondaHeaderType.clientType":   "Mobile",
    "hondaHeaderType.role":         "PRIMARY",
    "hondaHeaderType.country_code": "US",
    "hondaHeaderType.language_code":"en",
    "hondaHeaderType.version":      "1.0",
}
# Write/command endpoints (lock, unlock, climate, lights, horn, target charge)
# use the same headers as dbd/async EXCEPT they omit `role`. Sending role on a
# write command makes Honda's gateway return a generic 500 / 0000-00-1000.
HDR_CIG_CMD = {k: v for k, v in HDR_CIG_DBD.items() if k != "hondaHeaderType.role"}
COMMON_HEADERS = {
    "Accept":          "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Content-Type":    "application/json",
    "User-Agent":      "okhttp/4.12.0",
    "Connection":      "keep-alive",
}

DASHBOARD_FILTERS = [
    "DigitalTwin",
    "EV BATTERY LEVEL",
    "EV CHARGE STATE",
    "EV PLUG STATE",
    "EV PLUG VOLTAGE",
    "VEHICLE RANGE",
    "odometer",
    "TIRE PRESSURE",
    "HV BATTERY CHARGE COMPLETE TIME",
    "TARGET CHARGE LEVEL SETTINGS",
    "GET CHARGE MODE",
    "CHARGER POWER LEVEL",
]

STATE_PATH = STATE_DIR / "state.json"

# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:
            log.exception("Failed to read state file; starting fresh")
    return {}

def save_state(state: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2))

# ---------------------------------------------------------------------------
# HIDAS auth (~180 day token lifespan)
# ---------------------------------------------------------------------------

def hidas_register() -> str:
    log.info("Registering new HIDAS client")
    r = requests.post(
        f"{HIDAS_BASE}/hidas/rs/client/register",
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Accept": "application/json"},
        data={"client_id": ANDROID_CLIENT_ID,
              "client_secret": ANDROID_CLIENT_SECRET,
              "device_description": "HondaLinkBridge HAOS addon"},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["clientregistrationkey"]["client_reg_key"]

def hidas_token(client_reg_key: str) -> dict:
    log.info("Generating HIDAS access token")
    r = requests.post(
        f"{HIDAS_BASE}/hidas/rs/token/generate",
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Accept": "application/json"},
        data={"client_id":      ANDROID_CLIENT_ID,
              "client_reg_key": client_reg_key,
              "grant_type":     "password",
              "username":       HONDA_EMAIL,
              "password":       HONDA_PASSWORD},
        timeout=20,
    )
    r.raise_for_status()
    body = r.json()
    return {
        "access_token": body["token"]["access_token"],
        "expires_at":   int(time.time()) + int(body["token"]["expires_in"]) - 3600,
        "hidas_ident":  body["user"]["hidas_ident"],
    }

def ensure_auth(state: dict) -> dict:
    if "client_reg_key" not in state:
        state["client_reg_key"] = hidas_register()
        save_state(state)
    if "access_token" not in state or state.get("expires_at", 0) < int(time.time()):
        state.update(hidas_token(state["client_reg_key"]))
        save_state(state)
    return state

def force_reauth(state: dict) -> dict:
    log.warning("Forcing HIDAS re-auth")
    state.pop("access_token", None)
    state.pop("expires_at", None)
    return ensure_auth(state)

# ---------------------------------------------------------------------------
# Common request header builder
# ---------------------------------------------------------------------------

def request_headers(endpoint_headers: dict, access_token: str, hidas_ident: str) -> dict:
    return {
        **COMMON_HEADERS,
        **endpoint_headers,
        "Authorization":                  f"Bearer {access_token}",
        "hondaHeaderType.userId":         hidas_ident,
        "hondaHeaderType.messageId":      str(uuid.uuid4()).upper(),
        "hondaHeaderType.collectedTimestamp":
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

# ---------------------------------------------------------------------------
# CIG JWT exchange (the JWT is used as the AWS IoT custom authorizer credential)
# ---------------------------------------------------------------------------

def get_cig_jwt(state: dict) -> tuple[str, str]:
    """Returns (jwt_token, jwt_signature) - the credentials for AWS IoT MQTT."""
    log.info("Exchanging HIDAS bearer for CIG JWT")
    headers = request_headers(HDR_CIG_TOKEN, state["access_token"], state["hidas_ident"])
    headers["hondaHeaderType.hidasId"] = state["hidas_ident"]
    r = requests.post(
        f"{WSC_BASE}/REST/CIG/services/1.0/token",
        headers=headers, json={"device": VIN}, timeout=20,
    )
    if r.status_code == 401:
        raise PermissionError("401 from CIG token exchange")
    r.raise_for_status()
    rb = r.json()["responseBody"]
    log.debug("CIG JWT: token=%s... sig=%s...", rb["token"][:20], rb["tokenSignature"][:20])
    return rb["token"], rb["tokenSignature"]

# ---------------------------------------------------------------------------
# dbd/async trigger - tells the car to push fresh state to its shadow
# ---------------------------------------------------------------------------

def call_outage_check(state: dict) -> dict:
    """POST /REST/NGT/SearchOutage/1.0/ - returns Honda's outage map.

    Response shape: {"division": []} when all clear; non-empty array when
    one or more service divisions are reporting an outage.
    """
    headers = request_headers(HDR_CIG_CMD, state["access_token"], state["hidas_ident"])
    try:
        r = requests.post(f"{WSC_BASE}/REST/NGT/SearchOutage/1.0/",
                          headers=headers, json={}, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        log.debug("Outage check failed", exc_info=True)
        return {"division": []}

def call_dbd_async(state: dict) -> str:
    """POST /REST/NGT/CIG/dbd/async; returns cigServiceRequestId.

    Honda's gateway occasionally returns 500 when the car is in deep sleep
    or briefly unreachable. We retry once after a short delay before giving
    up — most transient errors resolve within a few seconds.
    """
    log.info("Triggering dbd/async refresh")
    headers = request_headers(HDR_CIG_DBD, state["access_token"], state["hidas_ident"])
    body = {"device": VIN, "filters": DASHBOARD_FILTERS}

    last_err: Exception | None = None
    for attempt in (1, 2):
        try:
            r = requests.post(f"{WSC_BASE}/REST/NGT/CIG/dbd/async",
                              headers=headers, json=body, timeout=20)
            if r.status_code == 401:
                raise PermissionError("401 from dbd/async")
            if not r.ok:
                log.warning("dbd/async attempt %d failed: %d %s",
                            attempt, r.status_code, r.text[:300])
                r.raise_for_status()
            req_id = r.json()["responseBody"]["cigServiceRequestId"]
            log.info("dbd/async requestId=%s", req_id)
            return req_id
        except PermissionError:
            raise
        except Exception as e:
            last_err = e
            if attempt == 1:
                log.info("Retrying dbd/async in 5s (Honda transient error)")
                time.sleep(5)
    raise last_err  # type: ignore[misc]

# ---------------------------------------------------------------------------
# AWS IoT MQTT (WebSocket + custom authorizer) - listens for shadow updates
# ---------------------------------------------------------------------------

class HondaIoT:
    """Connects to Honda's AWS IoT broker and yields DASHBOARD_ASYNC messages.

    Auth values learned from a captured WebSocket Upgrade request:
      - Token header name:  prod_key   (Honda's Lambda authorizer config)
      - Signature header:   X-Amz-CustomAuthorizer-Signature, RAW base64 (not URL-encoded)
      - URL path:           /mqtt       (no query string; SDK info goes in User-Agent)
    """

    # The thing name in AWS IoT is `thing_<VIN>` (with the literal `thing_`
    # prefix), not just the VIN. Discovered by decoding a captured MQTT
    # PUBLISH frame from Proxyman.
    THING = f"thing_{VIN}"
    # We subscribe to two named shadows: DASHBOARD_ASYNC (full vehicle state,
    # updated each poll cycle) and ENGINE_START_STOP_ASYNC (engine/climate
    # state, only updates when a climate command runs). For each shadow we
    # subscribe to all three "data delivery" topics.
    SHADOW_TOPICS = [
        f"$aws/things/{THING}/shadow/name/DASHBOARD_ASYNC/get/accepted",
        f"$aws/things/{THING}/shadow/name/DASHBOARD_ASYNC/update/documents",
        f"$aws/things/{THING}/shadow/name/DASHBOARD_ASYNC/update/accepted",
        f"$aws/things/{THING}/shadow/name/ENGINE_START_STOP_ASYNC/get/accepted",
        f"$aws/things/{THING}/shadow/name/ENGINE_START_STOP_ASYNC/update/documents",
        f"$aws/things/{THING}/shadow/name/ENGINE_START_STOP_ASYNC/update/accepted",
    ]

    def __init__(self, jwt_token: str, jwt_signature: str):
        self.jwt = jwt_token
        self.sig = jwt_signature
        # Per-shadow latest payload buffers
        self._payloads: dict[str, dict] = {}
        self._lock = threading.Lock()
        self._dashboard_evt = threading.Event()
        self._connected = threading.Event()
        self._disconnect_rc: int | None = None

        self.client = mqtt.Client(
            client_id=VIN,
            transport="websockets",
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        self.client.tls_set()
        self.client.ws_set_options(
            path="/mqtt",
            headers={
                "X-Amz-CustomAuthorizer-Name":      AWS_IOT_AUTHORIZER,
                "X-Amz-CustomAuthorizer-Signature": self.sig,
                "prod_key":                         self.jwt,
                "User-Agent":                       "?SDK=Android&Version=2.75.0",
            },
        )
        self.client.on_connect    = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message    = self._on_message

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            log.debug("AWS IoT connected")
            for t in self.SHADOW_TOPICS:
                client.subscribe(t, qos=1)
            self._connected.set()
        else:
            log.error("AWS IoT connect refused: rc=%s", rc)

    def _on_disconnect(self, client, userdata, disconnect_flags=None, rc=None, properties=None):
        rc_val = rc if rc is not None else (disconnect_flags if isinstance(disconnect_flags, int) else None)
        self._disconnect_rc = rc_val
        log.debug("AWS IoT disconnected (rc=%s)", rc_val)
        self._connected.clear()

    def _on_message(self, client, userdata, msg):
        log.debug("MQTT IN: %s (%d bytes)", msg.topic, len(msg.payload))
        try:
            payload = json.loads(msg.payload)
        except Exception:
            log.exception("Bad MQTT payload on %s", msg.topic)
            return
        # Identify the shadow this message belongs to by its topic.
        shadow = None
        for name in ("DASHBOARD_ASYNC", "ENGINE_START_STOP_ASYNC"):
            if f"/{name}/" in msg.topic:
                shadow = name
                break
        if shadow is None:
            return
        with self._lock:
            self._payloads[shadow] = payload
        if shadow == "DASHBOARD_ASYNC":
            # Dashboard arrival ends the wait; engine arrives whenever it
            # arrives and is collected at close().
            self._dashboard_evt.set()

    def connect(self, timeout: float = 15.0) -> None:
        log.info("Connecting to AWS IoT %s:443", AWS_IOT_HOST)
        # Disable paho's auto-reconnect; we want one shot per poll cycle.
        self.client.reconnect_delay_set(min_delay=999999, max_delay=999999)
        self.client.connect(AWS_IOT_HOST, 443, keepalive=60)
        self.client.loop_start()
        if not self._connected.wait(timeout):
            raise TimeoutError(f"AWS IoT connect timed out (last rc={self._disconnect_rc})")

    def wait_for_dashboard(self, timeout: float = 30.0) -> dict | None:
        """Block until a DASHBOARD_ASYNC payload arrives, then return it."""
        if self._dashboard_evt.wait(timeout):
            with self._lock:
                return self._payloads.get("DASHBOARD_ASYNC")
        return None

    def get_engine_payload(self) -> dict | None:
        """Return whatever ENGINE_START_STOP_ASYNC payload happened to arrive."""
        with self._lock:
            return self._payloads.get("ENGINE_START_STOP_ASYNC")

    def close(self) -> None:
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass



# ---------------------------------------------------------------------------
# High-level "give me the dashboard" function
# ---------------------------------------------------------------------------

def fetch_dashboard(state: dict) -> tuple[dict | None, dict | None]:
    """Connect to AWS IoT, fire dbd/async, return (dashboard, engine) payloads.

    Engine payload is best-effort — only present if Honda's backend pushed
    an ENGINE_START_STOP_ASYNC update during our subscription window
    (typically only after the user has used climate recently).
    """
    jwt_tok, jwt_sig = get_cig_jwt(state)
    iot = HondaIoT(jwt_tok, jwt_sig)
    try:
        iot.connect(timeout=15)
        time.sleep(0.5)
        try:
            call_dbd_async(state)
        except Exception:
            log.exception("dbd/async trigger failed; will still wait for any push")
        dashboard = iot.wait_for_dashboard(timeout=45)
        engine    = iot.get_engine_payload()
        return dashboard, engine
    finally:
        iot.close()

# Tracks the most recent outage state so it can be merged into the next
# published state. Updated each poll cycle, never blocks the dashboard fetch.
_last_outage = {"outage_active": False, "outage_count": 0}

# Engine/climate state from ENGINE_START_STOP_ASYNC shadow. The shadow
# only updates when a climate command runs, so we persist the last-known
# values across poll cycles to keep HA entities populated.
_last_engine: dict = {}

# Used to wake the main loop early when the user presses the Refresh button.
_refresh_event = threading.Event()

# ---------------------------------------------------------------------------
# Response parsing - shadows have a 'state.reported.<...>' structure
# ---------------------------------------------------------------------------

def _walk(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = _walk(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = _walk(v, key)
            if r is not None:
                return r
    return None

def _num(v):
    if isinstance(v, dict):
        v = v.get("value")
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None

def parse_dashboard(payload: dict) -> dict:
    """Extract EV fields from Honda's shadow document.

    Real shape (captured):
      payload.state.reported.responseBody = {
        "evStatus": {"soc": "21", "evRange": "74.0", "chargeStatus": "charging",
                     "chargeMode": "120", "plugStatus": "plugged"},
        "odometer": {"value": "305", "unit": "Miles"},
        "tireStatus": {"frontLeft": {"pressureData": {"value": "272", "unit": "kPa"}}, ...},
        "getChargeMode": {"generalAwayTargetChargeLevel": {"value": "80", "unit": "%"}},
        "hvBatteryChargeCompleteTime": {"hvBatteryChargeCompleteDay": {"value": "Wednesday"},
                                         "hvBatteryChargeCompleteHour": {"value": "0"},
                                         "hvBatteryChargeCompleteMinute": {"value": "45"}},
        ...
      }
    """
    out = {}
    # AWS IoT delivers shadow updates on two different topics with two
    # different shapes. Handle both:
    #   /update/accepted  -> payload.state.reported.responseBody
    #   /update/documents -> payload.current.state.reported.responseBody
    payload = payload or {}
    rb = ((payload.get("state") or {}).get("reported") or {}).get("responseBody") or {}
    if not rb:
        rb = (((payload.get("current") or {}).get("state") or {})
              .get("reported") or {}).get("responseBody") or {}
    if not rb:
        # Last-resort fallback for unfamiliar shapes.
        rb = payload

    ev = rb.get("evStatus") or {}
    if "soc" in ev:                 out["battery_level"]  = _num(ev["soc"])
    if "evRange" in ev:             out["range"]          = _num(ev["evRange"])
    if "plugStatus" in ev:          out["plugged_in"]     = str(ev["plugStatus"]).lower() in {"plugged","connected"}
    if "chargeStatus" in ev:        out["charging"]       = str(ev["chargeStatus"]).lower() == "charging"
    if "chargeMode" in ev:          out["charge_mode"]    = str(ev["chargeMode"])

    odo = rb.get("odometer") or {}
    if "value" in odo:              out["odometer"]       = _num(odo["value"])

    gcm = rb.get("getChargeMode") or {}
    target = (gcm.get("generalAwayTargetChargeLevel") or {}).get("value")
    if target is not None:          out["target_charge_level"] = _num(target)

    eta_block = rb.get("hvBatteryChargeCompleteTime") or {}
    day = ((eta_block.get("hvBatteryChargeCompleteDay") or {}).get("value"))
    hour = ((eta_block.get("hvBatteryChargeCompleteHour") or {}).get("value"))
    minute = ((eta_block.get("hvBatteryChargeCompleteMinute") or {}).get("value"))
    is_charging = bool(out.get("charging"))
    if is_charging and (day or hour or minute):
        out["charge_complete_time"] = f"{day or '?'} {hour or '0'}:{(minute or '0').zfill(2)}"
    else:
        out["charge_complete_time"] = ""

    tires = rb.get("tireStatus") or {}
    for corner in ("frontLeft", "frontRight", "rearLeft", "rearRight"):
        corner_data = tires.get(corner) or {}
        v = (corner_data.get("pressureData") or {}).get("value")
        if v is not None:
            out[f"tire_{corner.lower()}"] = _num(v)
        warn = (corner_data.get("warningState") or {}).get("value")
        if warn is not None:
            out[f"tire_{corner.lower()}_warning"] = str(warn).upper() in {"ON", "TRUE", "1"}

    # Last reported time from the vehicle (ISO 8601, ready for HA timestamp class)
    ts = rb.get("timestamp")
    if ts:
        out["last_update"] = str(ts)

    # Estimated charging rate (kW + mi/hr), derived from SOC, target, ETA,
    # and battery capacity. Only computed when actively charging; defaults
    # to 0 otherwise so HA shows a clean zero rather than "unknown".
    if is_charging and out.get("battery_level") is not None \
       and out.get("target_charge_level") is not None and day:
        try:
            hours = _eta_hours_from_now(day, hour, minute)
            if hours and hours > 0:
                soc = out["battery_level"]
                target = out["target_charge_level"]
                if target > soc:
                    kwh_to_add = (target - soc) / 100.0 * BATTERY_CAPACITY_KWH
                    rate_kw = kwh_to_add / hours
                    out["charge_rate_kw"] = round(rate_kw, 2)
                    # Miles added per hour at the current efficiency (summer/winter
                    # auto-selected by month). Example: 1.4 kW × 3.5 mi/kWh = 4.9 mph.
                    out["charge_rate_mph"] = round(rate_kw * _current_efficiency_mi_per_kwh(), 1)
        except Exception:
            log.debug("charge_rate calc failed", exc_info=True)

    out.setdefault("charge_rate_kw", 0)
    out.setdefault("charge_rate_mph", 0)

    # ─── Tier 1: charging context, location, warnings ─────────────
    range_at_target = (rb.get("estRangePerTCL") or {}).get("value")
    if range_at_target:
        out["range_at_target"] = _num(range_at_target)

    precond = (rb.get("highVoltageBatteryPreconditioningStatus") or {}).get("value")
    if precond:
        out["battery_preconditioning"] = str(precond)

    cpl = (rb.get("chargerPowerLevel") or {}).get("value")
    if cpl:
        out["charger_power_level"] = str(cpl)

    climit = (rb.get("hvChargeLimitedReason") or {}).get("value")
    if climit:
        out["charge_limit_reason"] = str(climit)

    at_home = ((rb.get("targetChargeLevelSettings") or {}).get("vehInHomeLocation") or {}).get("value")
    if at_home:
        out["at_home"] = str(at_home).upper() == "TRUE"

    home_stored = (rb.get("homeLocIsStored") or {}).get("value")
    if home_stored:
        out["home_location_stored"] = str(home_stored).upper() == "TRUE"

    # Active warning-lamp messages: collect them all (across language groups)
    # into a single string + count. When empty, count=0 and text="None".
    warning_data = (rb.get("warningLamps") or {}).get("data") or []
    warning_messages: list[str] = []
    for entry in warning_data:
        for m in (entry.get("messages") or []):
            if isinstance(m, str) and m.strip():
                warning_messages.append(m.strip())
            elif isinstance(m, dict) and m.get("message"):
                warning_messages.append(str(m["message"]).strip())
    out["warning_lamps_count"] = len(warning_messages)
    out["warning_lamps"] = ", ".join(warning_messages) if warning_messages else "None"

    # ─── Tier 2: schedule, efficiency, scheduled precondition temp ───
    gcm = rb.get("getChargeMode") or {}
    cmt = (gcm.get("chargeModeType") or {}).get("value")
    if cmt:
        out["charge_mode_type"] = str(cmt)
    sched_day  = (gcm.get("chargingDayOfWeek") or {}).get("value")
    sched_hour = (gcm.get("chargeHourOfDay")  or {}).get("value")
    sched_min  = (gcm.get("chargeMinuteOfHour") or {}).get("value")
    if sched_day:
        out["scheduled_charge_day"] = str(sched_day)
    if sched_hour is not None and sched_min is not None:
        try:
            out["scheduled_charge_time"] = f"{int(sched_hour)}:{str(int(sched_min)).zfill(2)}"
        except (TypeError, ValueError):
            pass

    lt_eff = ((rb.get("energyEfficiency") or {}).get("lifeTimeEfficiency") or {}).get("value")
    lt_num = _num(lt_eff)
    if lt_num is not None and lt_num > 0:
        out["lifetime_efficiency"] = lt_num

    last_trip = rb.get("lastTripFuelEconomy") or {}
    lt_value = (last_trip.get("value") if isinstance(last_trip, dict) else None)
    lt_trip_num = _num(lt_value)
    if lt_trip_num is not None and lt_trip_num > 0:
        out["last_trip_efficiency"] = lt_trip_num

    pc = ((rb.get("cabinPreconditioningTempCustomSetting") or {})
          .get("scheduledCabinPreconditionCustomSetValue") or {})
    pc_val = pc.get("value")
    pc_unit = (pc.get("unit") or "").strip()
    pc_num = _num(pc_val)
    if pc_num is not None:
        # Honda stores this in Celsius (`unit:"Cel"`); convert to °F to match
        # the rest of the bridge's temperature units.
        if pc_unit.lower().startswith("cel"):
            out["scheduled_precondition_temp_f"] = round(pc_num * 9 / 5 + 32, 1)
        else:
            out["scheduled_precondition_temp_f"] = round(pc_num, 1)

    return out

def parse_engine(payload: dict) -> dict:
    """Extract climate / engine fields from an ENGINE_START_STOP_ASYNC payload."""
    out: dict = {}
    payload = payload or {}
    rb = ((payload.get("state") or {}).get("reported") or {}).get("responseBody") or {}
    if not rb:
        rb = (((payload.get("current") or {}).get("state") or {})
              .get("reported") or {}).get("responseBody") or {}
    if not rb:
        return out

    if "ignition" in rb:
        out["ignition"] = str(rb["ignition"])
    if rb.get("errorMessage"):
        out["climate_last_error"] = str(rb["errorMessage"])
    elif rb.get("status") == "SUCCESS":
        out["climate_last_error"] = ""

    vc = rb.get("vehicleControl") or {}
    if "vehicleStartStatus" in vc:
        out["vehicle_start_status"] = str(vc["vehicleStartStatus"])

    cabin = vc.get("cabinTemperature") or {}
    cabin_val = cabin.get("value")
    # Honda reports `0` as "no data available" — treat that as missing.
    if cabin_val not in (None, 0, "0", "0.0"):
        try:
            out["cabin_temperature"] = float(cabin_val)
        except (TypeError, ValueError):
            pass

    event = vc.get("vehicleStartEvent") or {}
    if event.get("eventTime"):
        out["last_climate_event"] = str(event["eventTime"])
    if "rStartCounter" in event:
        try:
            out["remote_start_count"] = int(event["rStartCounter"])
        except (TypeError, ValueError):
            pass

    ac = vc.get("acStatus") or {}
    if ac.get("acDefSetting"):
        out["ac_default_setting"] = str(ac["acDefSetting"])

    return out

# Battery pack usable capacity (kWh). Honda Prologue and Acura ZDX share the
# Ultium platform; both ship with 85 kWh usable. Exposed as a constant so it
# can be overridden later if Honda releases other trims.
BATTERY_CAPACITY_KWH = 85.0

def _current_efficiency_mi_per_kwh() -> float:
    """Return the configured EV efficiency for the current calendar month."""
    return SUMMER_EFFICIENCY if datetime.now().month in SUMMER_MONTHS else WINTER_EFFICIENCY

def _is_daytime() -> bool:
    """True if the current moment is between sunrise and sunset.

    Uses the `astral` library when latitude + longitude are configured.
    Otherwise falls back to a fixed hour window (DAY_START_HOUR..DAY_END_HOUR).
    """
    if LATITUDE != 0 and LONGITUDE != 0:
        try:
            from astral import LocationInfo
            from astral.sun import sun as astral_sun
            loc = LocationInfo(latitude=LATITUDE, longitude=LONGITUDE)
            now_utc = datetime.now(timezone.utc)
            s = astral_sun(loc.observer, date=now_utc.date(), tzinfo=timezone.utc)
            return s["sunrise"] <= now_utc <= s["sunset"]
        except Exception:
            log.exception("astral sunrise/sunset failed; falling back to fixed hours")
    hour = datetime.now().hour
    return DAY_START_HOUR <= hour < DAY_END_HOUR

def _current_poll_interval() -> int:
    """Return seconds to wait before the next cycle, factoring day/night schedule."""
    if not ENABLE_DAY_NIGHT:
        return POLL_INTERVAL
    return POLL_INTERVAL_DAY if _is_daytime() else POLL_INTERVAL_NIGHT

_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

def _eta_hours_from_now(day_name: str, hour: str, minute: str) -> float | None:
    """Convert a day-of-week + hour:minute ETA into hours from now."""
    if not day_name:
        return None
    try:
        target_weekday = _DAYS.index(day_name)
    except ValueError:
        return None
    h = int(hour) if hour else 0
    m = int(minute) if minute else 0
    now = datetime.now()
    days_ahead = (target_weekday - now.weekday()) % 7
    target = (now.replace(hour=h, minute=m, second=0, microsecond=0)
              + timedelta(days=days_ahead))
    if target <= now:
        target += timedelta(days=7)
    return (target - now).total_seconds() / 3600.0

# ---------------------------------------------------------------------------
# Outbound commands (HA control entities -> Honda REST endpoints)
# ---------------------------------------------------------------------------

# Module-level auth state shared with command handlers (set in main()).
_auth: dict = {}

# In-memory state for entities we don't get readbacks for (climate temp).
_climate_temp_f: int = 70

def _post_command(endpoint: str, body: dict) -> None:
    """POST a command to a Honda CIG endpoint with current auth headers."""
    if not _auth.get("access_token"):
        log.error("Command ignored: not authenticated yet")
        return
    headers = request_headers(HDR_CIG_CMD, _auth["access_token"], _auth["hidas_ident"])
    try:
        r = requests.post(f"{WSC_BASE}{endpoint}", headers=headers,
                          json=body, timeout=20)
        log.info("Command %s -> %d %s", endpoint, r.status_code, r.text[:200])
    except Exception:
        log.exception("Command %s failed", endpoint)

def _require_pin(action: str) -> bool:
    if not HONDA_PIN:
        log.warning("%s requires honda_pin in add-on config", action)
        return False
    return True

def cmd_target_charge_level(payload: str) -> None:
    """HA Number -> POST /REST/NGT/TargetChargeLevel/1.0"""
    try:
        val = str(int(round(float(payload))))
    except (TypeError, ValueError):
        log.warning("Bad target_charge_level: %s", payload)
        return
    log.info("Setting target charge level to %s%%", val)
    _post_command("/REST/NGT/TargetChargeLevel/1.0",
                  {"device": VIN, "targetChargeLevel": val})

def cmd_climate(payload: str) -> None:
    """HA Switch -> POST /REST/NGT/CIG/eng/async/{srt|sop}

    Start uses .../srt with command:"Start" and acDefSetting:"autoOn".
    Stop  uses .../sop with command:"Stop"  and acDefSetting:"autoOff".
    Both bodies carry the same vehicleControl/acSetting block.
    """
    if not _require_pin("Climate"):
        return
    is_on = payload.upper() == "ON"
    endpoint = "/REST/NGT/CIG/eng/async/srt" if is_on else "/REST/NGT/CIG/eng/async/sop"
    body = {
        "pin":     HONDA_PIN,
        "extend":  False,
        "device":  VIN,
        "command": "Start" if is_on else "Stop",
        "changed": False,
        "vehicleControl": {
            "acSetting": {
                "acTempUnit":    "F",
                "acDefSetting":  "autoOn" if is_on else "autoOff",
                "acTempVal":     str(_climate_temp_f),
            },
        },
    }
    log.info("Climate %s (target %d°F)", "ON" if is_on else "OFF", _climate_temp_f)
    _post_command(endpoint, body)

def cmd_climate_temp(payload: str) -> None:
    """HA Number (in-memory) — used as the temp value the next time Climate is started."""
    global _climate_temp_f
    try:
        _climate_temp_f = max(60, min(90, int(round(float(payload)))))
        log.info("Climate temperature set to %d°F", _climate_temp_f)
    except (TypeError, ValueError):
        log.warning("Bad climate_temp: %s", payload)

def cmd_lock_doors(_payload: str) -> None:
    """HA Button -> POST /REST/NGT/CIG/lk/async/alk (lock all doors)"""
    if not _require_pin("Lock Doors"):
        return
    log.info("Locking doors")
    _post_command("/REST/NGT/CIG/lk/async/alk", {"device": VIN, "pin": HONDA_PIN})

def cmd_unlock_doors(_payload: str) -> None:
    """HA Button -> POST /REST/NGT/CIG/lk/async/dulk (unlock driver door)"""
    if not _require_pin("Unlock Doors"):
        return
    log.info("Unlocking doors")
    _post_command("/REST/NGT/CIG/lk/async/dulk", {"device": VIN, "pin": HONDA_PIN})

def cmd_lights(_payload: str) -> None:
    """HA Button -> POST /REST/NGT/CIG/cfhl/async/lgt"""
    if not _require_pin("Flash Lights"):
        return
    log.info("Flashing lights")
    _post_command("/REST/NGT/CIG/cfhl/async/lgt", {"device": VIN, "pin": HONDA_PIN})

def cmd_horn(_payload: str) -> None:
    """HA Button -> POST /REST/NGT/CIG/cfhl/async/hrn"""
    if not _require_pin("Horn"):
        return
    log.info("Sounding horn")
    _post_command("/REST/NGT/CIG/cfhl/async/hrn", {"device": VIN, "pin": HONDA_PIN})

def cmd_refresh(_payload: str) -> None:
    """HA Button -> wake the main loop and force an immediate poll."""
    log.info("Manual refresh requested")
    _refresh_event.set()

def _cmd_topic(kind: str) -> str:
    return f"{NODE_ID}/{VIN}/cmd/{kind}"

# Map of command-topic -> handler function.
COMMAND_HANDLERS = {}  # populated below once NODE_ID/VIN are stable

# ---------------------------------------------------------------------------
# MQTT publish to user's broker with HA discovery
# ---------------------------------------------------------------------------

DISCOVERY_PREFIX = "homeassistant"
NODE_ID          = "honda_bridge"

def device_descriptor() -> dict:
    return {
        "identifiers":  [f"honda_{VIN}"],
        "name":         DEVICE_NAME,
        "manufacturer": "Honda",
        "model":        "Prologue / ZDX (BEV3)",
    }

SENSORS = [
    # Read-only entities. (target_charge_level is now a Number control below.)
    ("battery_level",          "Battery",             "battery",            "%",   "measurement",      "battery_level",          False),
    ("range",                  "Range",               "distance",           "mi",  "measurement",      "range",                  False),
    ("odometer",               "Odometer",            "distance",           "mi",  "total_increasing", "odometer",               False),
    ("charge_complete_time",   "Charge Complete ETA", None,                 None,  None,               "charge_complete_time",   False),
    ("charge_mode",            "Charge Mode",         None,                 None,  None,               "charge_mode",            False),
    ("charge_rate_kw",         "Charge Rate",         "power",              "kW",  "measurement",      "charge_rate_kw",         False),
    ("charge_rate_mph",        "Charge Rate (mi/hr)", None,                 "mph", "measurement",      "charge_rate_mph",        False),
    ("last_update",            "Last Update",         "timestamp",          None,  None,               "last_update",            False),
    ("plugged_in",             "Plugged In",          "plug",               None,  None,               "plugged_in",             True),
    ("charging",               "Charging",            "battery_charging",   None,  None,               "charging",               True),
    ("tire_frontleft",         "Tire Front Left",     "pressure",           "kPa", "measurement",      "tire_frontleft",         False),
    ("tire_frontright",        "Tire Front Right",    "pressure",           "kPa", "measurement",      "tire_frontright",        False),
    ("tire_rearleft",          "Tire Rear Left",      "pressure",           "kPa", "measurement",      "tire_rearleft",          False),
    ("tire_rearright",         "Tire Rear Right",     "pressure",           "kPa", "measurement",      "tire_rearright",         False),
    ("tire_frontleft_warning", "FL Tire Warning",     "problem",            None,  None,               "tire_frontleft_warning", True),
    ("tire_frontright_warning","FR Tire Warning",     "problem",            None,  None,               "tire_frontright_warning",True),
    ("tire_rearleft_warning",  "RL Tire Warning",     "problem",            None,  None,               "tire_rearleft_warning",  True),
    ("tire_rearright_warning", "RR Tire Warning",     "problem",            None,  None,               "tire_rearright_warning", True),
    ("outage_active",          "Service Outage",      "problem",            None,  None,               "outage_active",          True),
    ("outage_count",           "Outage Count",        None,                 None,  "measurement",      "outage_count",           False),
    # Engine / climate shadow data (only updates when climate command runs)
    ("vehicle_start_status",   "Vehicle Start Status",None,                 None,  None,               "vehicle_start_status",   False),
    ("ignition",               "Ignition",            None,                 None,  None,               "ignition",               False),
    ("cabin_temperature",      "Cabin Temperature",   "temperature",        "°F",  "measurement",      "cabin_temperature",      False),
    ("last_climate_event",     "Last Climate Event",  "timestamp",          None,  None,               "last_climate_event",     False),
    ("climate_last_error",     "Climate Last Error",  None,                 None,  None,               "climate_last_error",     False),
    ("ac_default_setting",     "AC Default Setting",  None,                 None,  None,               "ac_default_setting",     False),
    ("remote_start_count",     "Remote Start Count",  None,                 None,  "total_increasing", "remote_start_count",     False),
    # Tier 1: charging context + location + warnings
    ("range_at_target",        "Range at Target",     "distance",           "mi",    "measurement",      "range_at_target",        False),
    ("battery_preconditioning","Battery Precondition",None,                 None,    None,               "battery_preconditioning",False),
    ("charger_power_level",    "Charger Power Level", None,                 None,    None,               "charger_power_level",    False),
    ("charge_limit_reason",    "Charge Limit Reason", None,                 None,    None,               "charge_limit_reason",    False),
    ("at_home",                "At Home",             "presence",           None,    None,               "at_home",                True),
    ("home_location_stored",   "Home Location Set",   None,                 None,    None,               "home_location_stored",   True),
    ("warning_lamps_count",    "Warning Lamps Count", None,                 None,    "measurement",      "warning_lamps_count",    False),
    ("warning_lamps",          "Warning Lamps",       None,                 None,    None,               "warning_lamps",          False),
    # Tier 2: schedule + efficiency
    ("charge_mode_type",       "Charge Mode Type",    None,                 None,    None,               "charge_mode_type",       False),
    ("scheduled_charge_day",   "Scheduled Charge Day",None,                 None,    None,               "scheduled_charge_day",   False),
    ("scheduled_charge_time",  "Scheduled Charge Time",None,                None,    None,               "scheduled_charge_time",  False),
    ("lifetime_efficiency",    "Lifetime Efficiency", None,                 "mi/kWh","measurement",      "lifetime_efficiency",    False),
    ("last_trip_efficiency",   "Last Trip Efficiency",None,                 "mi/kWh","measurement",      "last_trip_efficiency",   False),
    ("scheduled_precondition_temp_f","Scheduled Precondition Temp","temperature","°F","measurement","scheduled_precondition_temp_f", False),
]

def publish_discovery(client: mqtt.Client) -> None:
    dev = device_descriptor()
    state_topic = f"{NODE_ID}/{VIN}/state"

    # ---- Read-only sensors / binary sensors ----
    for object_id, name, device_class, unit, state_class, _, is_binary in SENSORS:
        component = "binary_sensor" if is_binary else "sensor"
        topic = f"{DISCOVERY_PREFIX}/{component}/{NODE_ID}/{VIN}_{object_id}/config"
        cfg = {
            "name":           name,
            "unique_id":      f"honda_{VIN}_{object_id}",
            "state_topic":    state_topic,
            "value_template": f"{{{{ value_json.{object_id} }}}}",
            "device":         dev,
        }
        if device_class: cfg["device_class"] = device_class
        if unit:         cfg["unit_of_measurement"] = unit
        if state_class:  cfg["state_class"] = state_class
        if is_binary:
            cfg["payload_on"]  = "true"
            cfg["payload_off"] = "false"
            cfg["value_template"] = (
                f"{{% if value_json.{object_id} %}}true{{% else %}}false{{% endif %}}"
            )
        client.publish(topic, json.dumps(cfg), retain=True)

    # ---- Number: Target Charge (writable; current value comes from state) ----
    client.publish(
        f"{DISCOVERY_PREFIX}/number/{NODE_ID}/{VIN}_target_charge_level/config",
        json.dumps({
            "name":             "Target Charge",
            "unique_id":        f"honda_{VIN}_target_charge_level",
            "state_topic":      state_topic,
            "value_template":   "{{ value_json.target_charge_level }}",
            "command_topic":    _cmd_topic("target_charge_level"),
            "min":  50, "max": 100, "step": 5,
            "unit_of_measurement": "%",
            "mode": "slider",
            "icon": "mdi:battery-charging-80",
            "device": dev,
        }),
        retain=True,
    )

    # ---- Number: Climate Temperature (in-memory; optimistic) ----
    client.publish(
        f"{DISCOVERY_PREFIX}/number/{NODE_ID}/{VIN}_climate_temperature/config",
        json.dumps({
            "name":             "Climate Temperature",
            "unique_id":        f"honda_{VIN}_climate_temperature",
            "command_topic":    _cmd_topic("climate_temp"),
            "min":  60, "max": 90, "step": 1,
            "unit_of_measurement": "°F",
            "mode": "box",
            "icon": "mdi:thermometer",
            "optimistic": True,
            "retain":     True,
            "entity_category": "config",
            "device": dev,
        }),
        retain=True,
    )

    # ---- Switch: Climate Preconditioning (optimistic; no readback from API) ----
    client.publish(
        f"{DISCOVERY_PREFIX}/switch/{NODE_ID}/{VIN}_climate/config",
        json.dumps({
            "name":          "Climate Preconditioning",
            "unique_id":     f"honda_{VIN}_climate",
            "command_topic": _cmd_topic("climate"),
            "payload_on":    "ON",
            "payload_off":   "OFF",
            "icon":          "mdi:air-conditioner",
            "optimistic":    True,
            "device":        dev,
        }),
        retain=True,
    )

    # ---- Remove the old single "lock" entity if it was previously published ----
    # (safe to publish empty payload to its config topic; HA un-registers it)
    client.publish(
        f"{DISCOVERY_PREFIX}/lock/{NODE_ID}/{VIN}_lock/config",
        "", retain=True,
    )

    # ---- Buttons: Lock Doors, Unlock Doors, Flash Lights, Sound Horn, Refresh ----
    for kind, name, icon in [
        ("lock_doors",   "Lock Doors",    "mdi:car-door-lock"),
        ("unlock_doors", "Unlock Doors",  "mdi:car-door-lock-open"),
        ("lights",       "Flash Lights",  "mdi:car-light-high"),
        ("horn",         "Sound Horn",    "mdi:bullhorn"),
        ("refresh",      "Refresh",       "mdi:refresh"),
    ]:
        client.publish(
            f"{DISCOVERY_PREFIX}/button/{NODE_ID}/{VIN}_{kind}/config",
            json.dumps({
                "name":          name,
                "unique_id":     f"honda_{VIN}_{kind}",
                "command_topic": _cmd_topic(kind),
                "payload_press": "PRESS",
                "icon":          icon,
                "device":        dev,
            }),
            retain=True,
        )

    log.info("Published HA discovery configs (%d sensors + 6 controls)", len(SENSORS))

def publish_state(client: mqtt.Client, parsed: dict) -> None:
    client.publish(f"{NODE_ID}/{VIN}/state", json.dumps(parsed), retain=True)
    log.info("Published state: %s", parsed)

def _on_user_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log.info("User MQTT broker connected")
        for topic in COMMAND_HANDLERS:
            client.subscribe(topic, qos=1)
    else:
        log.error("User MQTT connect refused: rc=%s", rc)

def _on_user_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", "replace")
    log.debug("Command received on %s: %s", msg.topic, payload)
    handler = COMMAND_HANDLERS.get(msg.topic)
    if handler:
        try:
            handler(payload)
        except Exception:
            log.exception("Command handler crashed for %s", msg.topic)
    else:
        log.warning("Unhandled command topic: %s", msg.topic)

def make_user_mqtt_client() -> mqtt.Client:
    # Build the command-topic -> handler map now that NODE_ID/VIN are set.
    COMMAND_HANDLERS.update({
        _cmd_topic("target_charge_level"): cmd_target_charge_level,
        _cmd_topic("climate"):             cmd_climate,
        _cmd_topic("climate_temp"):        cmd_climate_temp,
        _cmd_topic("lock_doors"):          cmd_lock_doors,
        _cmd_topic("unlock_doors"):        cmd_unlock_doors,
        _cmd_topic("lights"):              cmd_lights,
        _cmd_topic("horn"):                cmd_horn,
        _cmd_topic("refresh"):             cmd_refresh,
    })
    c = mqtt.Client(
        client_id=f"honda_bridge_{VIN[-6:]}",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    if MQTT_USER:
        c.username_pw_set(MQTT_USER, MQTT_PASSWORD or "")
    c.on_connect = _on_user_connect
    c.on_message = _on_user_message
    c.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    c.loop_start()
    return c

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    global _auth
    if ENABLE_DAY_NIGHT:
        loc_desc = (f"sun@{LATITUDE:.3f},{LONGITUDE:.3f}" if (LATITUDE or LONGITUDE)
                    else f"fixed {DAY_START_HOUR}–{DAY_END_HOUR}h")
        log.info("HondaLink Bridge starting (VIN ...%s, day=%ds night=%ds via %s)",
                 VIN[-6:], POLL_INTERVAL_DAY, POLL_INTERVAL_NIGHT, loc_desc)
    else:
        log.info("HondaLink Bridge starting (VIN ...%s, every %ds)", VIN[-6:], POLL_INTERVAL)
    _auth = load_state()
    _auth = ensure_auth(_auth)

    user_mqtt = make_user_mqtt_client()
    publish_discovery(user_mqtt)

    while True:
        try:
            # Cheap outage probe alongside the dashboard fetch. Result is
            # merged into the next published state regardless of dbd/async
            # success so HA always sees the latest outage status.
            try:
                outage = call_outage_check(_auth)
                divisions = outage.get("division") or []
                _last_outage["outage_active"] = bool(divisions)
                _last_outage["outage_count"]  = len(divisions)
            except Exception:
                log.debug("Outage probe failed", exc_info=True)

            try:
                dashboard, engine = fetch_dashboard(_auth)
            except PermissionError:
                _auth = force_reauth(_auth)
                dashboard, engine = fetch_dashboard(_auth)

            # If a fresh engine shadow arrived, merge it into our persistent buffer.
            if engine:
                if log.isEnabledFor(logging.DEBUG):
                    log.debug("Raw engine: %s", json.dumps(engine)[:1000])
                _last_engine.update(parse_engine(engine))

            if dashboard is None:
                log.warning("No shadow payload received this cycle")
            else:
                if log.isEnabledFor(logging.DEBUG):
                    log.debug("Raw shadow: %s", json.dumps(dashboard)[:1000])
                parsed = parse_dashboard(dashboard)
                if parsed:
                    parsed.update(_last_engine)
                    parsed.update(_last_outage)
                    publish_state(user_mqtt, parsed)
                else:
                    log.warning("Parsed nothing from shadow: %s",
                                json.dumps(dashboard)[:500])
        except Exception:
            log.exception("Poll cycle failed; will retry next interval")
        # Wait either for the poll interval to elapse or for a manual refresh
        # button press, whichever comes first. Interval may differ day vs night.
        interval = _current_poll_interval()
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Sleeping %ds (%s schedule)", interval,
                      "day" if ENABLE_DAY_NIGHT and _is_daytime() else
                      "night" if ENABLE_DAY_NIGHT else "fixed")
        if _refresh_event.wait(interval):
            log.info("Wakeup from manual refresh")
            _refresh_event.clear()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
