# Honda Bridge Add-ons

Unofficial Home Assistant add-ons for Honda connected vehicles.

---

## Add-ons in this repository

### HondaLink Bridge — `honda-link-bridge/`

A Home Assistant add-on that polls Honda's connected-vehicle backend for the
**BEV3** telematics platform (Honda Prologue, Acura ZDX) and publishes the
data to your MQTT broker with auto-discovery. A single device with all
sensors appears automatically in Home Assistant — no YAML editing required.

**Sensors exposed:**

| Sensor | Unit | Notes |
|---|---|---|
| Battery | % | High-voltage state of charge |
| Range | mi | Estimated EV range |
| Odometer | mi | Lifetime miles |
| Target Charge | % | Configured charge limit |
| Charge Complete ETA | — | Day + time the car expects to finish charging |
| Charge Mode | — | Charger power level (e.g. `120`) |
| Plugged In | binary | Is the EVSE connected? |
| Charging | binary | Is power currently flowing? |
| Tire Pressure (×4) | kPa | One sensor per corner |

See [the add-on's README](./honda-link-bridge/README.md) for full
configuration and sensor details.

---

## Installation

1. In Home Assistant, go to **Settings → Add-ons → Add-on Store**.
2. Top-right ⋮ menu → **Repositories**.
3. Paste this URL and click **Add**:

   ```
   https://github.com/kkrankall/honda-bridge-addons
   ```

4. Refresh the Add-on Store. A new "Honda Bridge Add-ons" section appears at
   the bottom — click **HondaLink Bridge** → **Install**.
5. Open the **Configuration** tab. Fill in your HondaLink email, password,
   and VIN. **Save**, then **Start**.

Within a minute or two, a device named "Honda Prologue" will appear under
**Settings → Devices & Services → MQTT** with all sensors populated.

---

## Requirements

- **Home Assistant OS, Supervised, or Container** — anywhere the add-on
  store works.
- The official **Mosquitto broker** add-on, or any MQTT broker reachable from
  Home Assistant. Defaults assume `core-mosquitto`.
- Home Assistant's **MQTT integration** configured against that broker, with
  discovery enabled and the prefix set to `homeassistant` (the default).
- A **Honda Prologue or Acura ZDX** with an active HondaLink subscription,
  and the HondaLink credentials for the account it's enrolled to.

---

## Status

Functional but unofficial. Honda's connected-vehicle backend is undocumented
and may change at any time without notice. If something breaks, please open
an issue.

Pull requests welcome — especially additional sensor mappings (the JSON
response contains a lot more fields than are currently exposed),
non-Prologue/ZDX platform support, and bug fixes.

---

## Disclaimer

This is an independent open-source project. It is **not affiliated with,
endorsed by, or sponsored by** American Honda Motor Co., Inc. or its
subsidiaries.

The add-on automates the same authenticated requests that Honda's own
mobile apps make on your behalf, using your own HondaLink credentials.
Use is governed by Honda's terms of service. Use at your own risk.

---

## License

Released under the [MIT License](./LICENSE).
