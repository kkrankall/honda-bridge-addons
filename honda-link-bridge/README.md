# HondaLink Bridge

Polls Honda's connected-vehicle backend for the BEV3 platform (Honda Prologue,
Acura ZDX) and publishes the data to your MQTT broker with Home Assistant
auto-discovery. Sensors appear automatically under a single device in HA.

## Configure

| Option | Description |
|---|---|
| `honda_email` | Your HondaLink account email. |
| `honda_password` | Your HondaLink account password. Used once to fetch a long-lived token; never re-sent after that. |
| `honda_pin` | Your HondaLink account PIN. **Required** for lock/unlock, climate preconditioning, lights, and horn. Optional if you only want read-only sensors. |
| `vin` | Your vehicle's VIN. Find it in the HondaLink app under Vehicle Profile, on your registration, or behind the windshield. |
| `poll_interval_seconds` | How often to poll when the day/night schedule is disabled. 600 (10 min) is a safe default. |
| `enable_day_night_schedule` | If true, use `poll_interval_day` between sunrise and sunset, `poll_interval_night` otherwise. Default false. |
| `poll_interval_day` | Poll interval (seconds) during daytime. Default 300 (5 min). |
| `poll_interval_night` | Poll interval (seconds) at night. Default 1800 (30 min). |
| `latitude` / `longitude` | Your coordinates. When set, sunrise/sunset are computed via `astral`. Leave at 0 to use `day_start_hour`/`day_end_hour` instead. |
| `day_start_hour` / `day_end_hour` | Fixed-hours fallback when latitude/longitude aren't set (local time, 24-h format). |
| `mqtt_host` | Default `core-mosquitto` works if you use the official Mosquitto add-on. |
| `mqtt_port` | Usually 1883. |
| `mqtt_user` / `mqtt_password` | Required if your broker has auth enabled. |
| `device_name` | Display name for the device in HA. |
| `summer_efficiency_mi_per_kwh` | EV efficiency in summer months. Default `3.5`. Used to compute "Charge Rate (mi/hr)". |
| `winter_efficiency_mi_per_kwh` | EV efficiency in winter months. Default `3.0`. Cold weather and cabin heat reduce efficiency. |
| `summer_months` | Comma-separated list of months considered "summer", 1–12. Default `4,5,6,7,8,9,10` (Apr–Oct). |
| `log_level` | `INFO` for normal operation, `DEBUG` if something isn't working. |

## What it exposes

Once running, a single device named per `device_name` appears in
**Settings → Devices & Services → MQTT**, with these entities auto-created:

**Read-only sensors:**

| Entity | Type | Notes |
|---|---|---|
| Battery | sensor (%) | High-voltage state of charge |
| Range | sensor (mi) | Estimated remaining EV range |
| Odometer | sensor (mi) | Lifetime miles |
| Charge Complete ETA | sensor | Day + time the car expects to finish charging |
| Charge Mode | sensor | Charger power level (e.g. `120`) |
| Charge Rate | sensor (kW) | Calculated from SOC, target, and ETA |
| Charge Rate (mi/hr) | sensor (mph) | Charge rate × seasonal efficiency (mi/kWh). Auto-switches summer/winter. |
| Last Update | sensor (timestamp) | When the car last reported |
| Plugged In | binary_sensor | EVSE connected? |
| Charging | binary_sensor | Currently drawing power? |
| Tire Front Left / Right / Rear Left / Right | sensor (kPa) | Per-corner pressure |
| Tire \* Warning | binary_sensor | Per-corner low/fault warning |

**Controls** (require `honda_pin` except where noted):

| Entity | Type | Notes |
|---|---|---|
| Target Charge | number (%, 50–100, no PIN) | Slider; sets the car's charge limit |
| Climate Preconditioning | switch | Starts/stops cabin preconditioning |
| Climate Temperature | number (°F, 60–90) | Target temp the next time Climate is started |
| Lock Doors | button | Locks all doors |
| Unlock Doors | button | Unlocks the driver door |
| Flash Lights | button | Briefly flashes the headlights |
| Sound Horn | button | Sounds the horn |

## How it works

1. On first start, the add-on registers a client with Honda's identity service
   and exchanges your credentials for a long-lived access token (~6 month
   lifespan). The token is persisted to `/data/state.json` so it survives
   restarts and upgrades.
2. Each poll cycle: the access token is exchanged for a short-lived JWT, the
   add-on connects to Honda's AWS IoT MQTT broker as a custom-authorized
   client, subscribes to the dashboard shadow topic, then triggers a
   refresh via the REST async endpoint. Honda's backend pushes a fresh
   shadow document, the add-on parses the EV / odometer / tire fields, and
   publishes them to your local MQTT broker.

## Notes & limitations

This add-on talks to an undocumented Honda API. It works today; Honda may
change the protocol at any time and break it without warning.

Only the BEV3 platform (Prologue / ZDX) is supported. Other Honda EVs use
different backends.

If you change your HondaLink password, delete `/data/state.json` (or
uninstall + reinstall the add-on) so it re-bootstraps with the new
credentials.

This is an independent project and is not affiliated with, endorsed by, or
sponsored by American Honda Motor Co., Inc.

## Troubleshooting

**Sensors show as "Unavailable"** — check the add-on log. If you see
`Published state: {...}` at least once, the bridge is publishing correctly
and the issue is at HA's discovery layer (Settings → Devices & Services →
MQTT → Configure → enable discovery, prefix `homeassistant`). If you don't
see published state, check earlier log lines for a 401 (re-auth needed) or
a CIG / AWS IoT error.

**No shadow payload received** — Honda's backend can occasionally take
30+ seconds to push the shadow update after the async trigger. The add-on
waits 45 seconds. If timeouts persist, the most likely cause is that
Honda has changed the underlying authorizer or topic structure; raise an
issue with a debug-level log.

**`401` errors** — the access token is expired or revoked. Restart the
add-on; it will re-auth automatically.
