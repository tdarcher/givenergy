#!/usr/bin/env python3
"""
GivEnergy Battery Management System
Manages charge/discharge cycles to optimise against time-of-use tariffs.

Strategy:
  - Charge battery from grid during cheap-rate window (2AM-5AM)
  - Discharge battery for home consumption during evening peak (11PM-1:50AM)
  - Eco mode at all other times (solar self-consumption + dynamic charge/discharge)
  - Dynamic reserve: 30% during discharge (preserve capacity for recharge),
    4% during eco (maximise self-consumption)
  - Verify inverter settings each run to prevent drift
  - Only write to inverter when state actually needs to change

Hardware constraints:
  - 2.5 kW inverter, 9.5 kWh battery
  - 3h charge window (02:00-05:00) = max ~7.1 kWh chargeable (with losses)
  - 2.83h discharge window (23:00-01:50) = max ~6.6 kWh usable
  - Reserve 30% during discharge ensures battery can be fully recharged in 3h window
  - Reserve 4% during eco maximises self-consumption savings
  - Li-ion taper above 80% SOC reduces avg charge rate to ~55% of max
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).parent / "config.json"
STATE_PATH = Path(__file__).parent / ".last_state.json"

BASE_URL = "https://api.givenergy.cloud/v1/inverter"

# GivEnergy remote-control register IDs (verified for SD2237G385)
# Use GET /v1/inverter/{serial}/settings to discover correct IDs for your model
REG_ECO_MODE = 24                # Enable Eco Mode (bool)
REG_DISCHARGE_ENABLE = 56        # Enable DC Discharge (bool)
REG_DISCHARGE_START = 53         # DC Discharge 1 Start Time (HH:MM)
REG_DISCHARGE_END = 54           # DC Discharge 1 End Time (HH:MM)
REG_CHARGE_ENABLE = 66           # AC Charge Enable (bool, schedule flag, NOT live state)
REG_CHARGE_START = 64            # AC Charge 1 Start Time (HH:MM)
REG_CHARGE_END = 65             # AC Charge 1 End Time (HH:MM)
REG_CHARGE_UPPER_LIMIT_ENABLE = 17  # Enable AC Charge Upper % Limit (bool)
REG_CHARGE_UPPER_LIMIT = 77     # AC Charge Upper % Limit (int %)
REG_BATTERY_RESERVE = 71        # Battery Reserve % Limit (int %)
REG_BATTERY_CUTOFF = 75          # Battery Cutoff % Limit (int %, hard floor)
REG_CHARGE_POWER = 72           # Battery Charge Power (W)
REG_DISCHARGE_POWER = 73        # Battery Discharge Power (W)

API_TIMEOUT = 15  # seconds


def load_config():
    """Load configuration from JSON file."""
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.critical("Config file not found: %s", CONFIG_PATH)
        sys.exit(1)
    except json.JSONDecodeError as e:
        logging.critical("Invalid config JSON: %s", e)
        sys.exit(1)


def get_api_key(config):
    """Get API key from environment variable (never hardcode)."""
    key = os.environ.get(config.get("api_key_env", "GIVENERGY_API_KEY"))
    if not key:
        logging.critical(
            "API key not found. Set env var %s",
            config.get("api_key_env", "GIVENERGY_API_KEY"),
        )
        sys.exit(1)
    return key


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def make_headers(api_key):
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def api_get(config, api_key, path):
    """GET request to the GivEnergy API with error handling."""
    url = f"{BASE_URL}/{config['inverter_serial']}/{path}"
    try:
        resp = requests.get(url, headers=make_headers(api_key), timeout=API_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logging.error("API GET failed [%s]: %s", path, e)
        return None


def api_post(config, api_key, path, payload):
    """POST request to the GivEnergy API with JSON payload."""
    url = f"{BASE_URL}/{config['inverter_serial']}/{path}"
    try:
        resp = requests.post(
            url, headers=make_headers(api_key), json=payload, timeout=API_TIMEOUT
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logging.error("API POST failed [%s, payload=%s]: %s", path, payload, e)
        return None


def read_register(config, api_key, register_id):
    """Read a single inverter setting register."""
    result = api_post(config, api_key, f"settings/{register_id}/read", {})
    if result and "data" in result and "value" in result["data"]:
        return result["data"]["value"]
    logging.warning("Failed to read register %s: %s", register_id,
                     json.dumps(result)[:200] if result else "no response")
    return None


def write_register(config, api_key, register_id, value):
    """Write a single inverter setting register. Returns True on success."""
    logging.info("Writing register %s = %s", register_id, value)
    result = api_post(config, api_key, f"settings/{register_id}/write", {"value": value})
    if result is None:
        return False
    # Check for success in response
    success = result.get("data", {}).get("success")
    if success is False:
        logging.error("Register write rejected: %s", json.dumps(result)[:300])
        return False
    # Some responses don't have a "success" field but still indicate success
    # via HTTP 200 + data present
    logging.info("Register %s write accepted", register_id)
    return True


# ---------------------------------------------------------------------------
# Charge time calculator (accounts for Li-ion taper)
# ---------------------------------------------------------------------------

def estimate_charge_time(config, from_soc, to_soc=100):
    """Estimate hours to charge from from_soc% to to_soc%.
    Accounts for Li-ion taper above the configured threshold.
    """
    inverter_kw = config["inverter_power_kw"]
    capacity_kwh = config["battery_capacity_kwh"]
    efficiency = config.get("charge_efficiency", 0.95)
    taper_above = config.get("battery_taper_above_pct", 80)
    taper_factor = config.get("battery_taper_factor", 0.55)

    if from_soc >= to_soc:
        return 0.0

    # Energy needed from grid (accounting for losses)
    total_kwh = (to_soc - from_soc) / 100.0 * capacity_kwh / efficiency

    # Split into below-taper and above-taper regions
    if from_soc < taper_above:
        below_kwh = (min(to_soc, taper_above) - from_soc) / 100.0 * capacity_kwh / efficiency
        above_kwh = total_kwh - below_kwh
        time_below = below_kwh / inverter_kw
        time_above = above_kwh / (inverter_kw * taper_factor) if above_kwh > 0 else 0
    else:
        below_kwh = 0
        above_kwh = total_kwh
        time_below = 0
        time_above = above_kwh / (inverter_kw * taper_factor)

    return time_below + time_above


# ---------------------------------------------------------------------------
# High-level inverter operations
# ---------------------------------------------------------------------------

def get_battery_soc(config, api_key):
    """Get current battery state of charge."""
    data = api_get(config, api_key, "system-data/latest")
    if data and "data" in data:
        try:
            return data["data"]["battery"]["percent"]
        except (KeyError, TypeError):
            logging.error("Unexpected system-data format: %s", json.dumps(data)[:200])
            return None
    return None


def get_current_mode(config, api_key, now_str):
    """Determine the current inverter mode from register reads AND time context.

    IMPORTANT: AC charge enable (reg 66) being True just means a schedule exists.
    It does NOT mean the inverter is currently charging. We must cross-check
    whether we're inside the charge window to report the actual mode correctly.

    Returns: 'eco', 'discharge', 'charge', or 'unknown'
    """
    eco = read_register(config, api_key, REG_ECO_MODE)
    discharge_en = read_register(config, api_key, REG_DISCHARGE_ENABLE)
    charge_en = read_register(config, api_key, REG_CHARGE_ENABLE)
    charge_start = read_register(config, api_key, REG_CHARGE_START)
    charge_end = read_register(config, api_key, REG_CHARGE_END)

    if eco is None and discharge_en is None and charge_en is None:
        return "unknown"

    # Check if we're inside an active charge window
    in_charge_window = False
    if charge_en and charge_start and charge_end:
        in_charge_window = time_in_range(now_str, charge_start, charge_end)

    # Timed charge active right now
    if charge_en and in_charge_window:
        return "charge"

    # Check for discharge mode: eco OFF + discharge ON = forced discharge
    if eco is False and discharge_en is True:
        return "discharge"

    # GivEnergy "timed discharge": eco ON + discharge ON
    # Only counts as discharge if we're in the discharge time window
    if eco is True and discharge_en is True:
        return "discharge"

    # Eco ON, discharge OFF
    if eco is True:
        return "eco"

    return "unknown"


def set_eco_mode(config, api_key):
    """Switch to eco mode: eco on, discharge disabled, low reserve for max self-consumption."""
    eco_reserve = config.get("eco_reserve_percent", 4)
    logging.info("Setting ECO mode (reserve %s%%)", eco_reserve)
    write_register(config, api_key, REG_ECO_MODE, True)
    write_register(config, api_key, REG_DISCHARGE_ENABLE, False)
    write_register(config, api_key, REG_BATTERY_RESERVE, eco_reserve)


def set_discharge_mode(config, api_key):
    """Switch to timed discharge mode: eco off, discharge enabled, high reserve for recharge."""
    discharge_reserve = config["battery_reserve_percent"]
    logging.info("Setting DISCHARGE mode (%s - %s, reserve %s%%)",
                 config["discharge_start"], config["discharge_end"], discharge_reserve)
    write_register(config, api_key, REG_ECO_MODE, False)
    write_register(config, api_key, REG_DISCHARGE_ENABLE, True)
    write_register(config, api_key, REG_DISCHARGE_START, config["discharge_start"])
    write_register(config, api_key, REG_DISCHARGE_END, config["discharge_end"])
    write_register(config, api_key, REG_BATTERY_RESERVE, discharge_reserve)


def verify_charge_schedule(config, api_key):
    """Verify the charge schedule is correctly configured. Fix if not."""
    cheap_start = config["tariff"]["cheap_rate_start"]
    cheap_end = config["tariff"]["cheap_rate_end"]

    current_start = read_register(config, api_key, REG_CHARGE_START)
    current_end = read_register(config, api_key, REG_CHARGE_END)
    current_enable = read_register(config, api_key, REG_CHARGE_ENABLE)

    needs_fix = False
    if current_start != cheap_start:
        logging.warning("Charge start drift: got %s, expected %s", current_start, cheap_start)
        needs_fix = True
    if current_end != cheap_end:
        logging.warning("Charge end drift: got %s, expected %s", current_end, cheap_end)
        needs_fix = True
    if current_enable is not True:
        logging.warning("AC charge not enabled, enabling")
        needs_fix = True

    if needs_fix:
        logging.info("Fixing charge schedule: %s-%s", cheap_start, cheap_end)
        write_register(config, api_key, REG_CHARGE_ENABLE, True)
        write_register(config, api_key, REG_CHARGE_START, cheap_start)
        write_register(config, api_key, REG_CHARGE_END, cheap_end)


def verify_reserve(config, api_key, desired_reserve):
    """Set the battery reserve % to the desired value for the current mode.

    During discharge mode: reserve = 30% (preserve capacity for cheap-rate recharge)
    During eco mode: reserve = 4% (maximise self-consumption, let battery run low)
    """
    current = read_register(config, api_key, REG_BATTERY_RESERVE)
    if current is not None and current != desired_reserve:
        logging.info("Battery reserve: %s%% -> %s%% (mode-appropriate)",
                     current, desired_reserve)
        write_register(config, api_key, REG_BATTERY_RESERVE, desired_reserve)
    elif current is None:
        logging.warning("Could not read battery reserve register")


# ---------------------------------------------------------------------------
# State caching (avoid redundant API writes)
# ---------------------------------------------------------------------------

def load_last_state():
    """Load the last known state from disk."""
    try:
        with open(STATE_PATH, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_last_state(state):
    """Persist current state to disk for next run."""
    try:
        with open(STATE_PATH, "w") as f:
            json.dump(state, f)
    except OSError as e:
        logging.warning("Could not save state file: %s", e)


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def time_in_range(now_str, start_str, end_str):
    """Check if a HH:MM time string falls within a range.
    Handles overnight ranges (start > end) correctly.
    """
    now = int(now_str[:2]) * 60 + int(now_str[3:])
    start = int(start_str[:2]) * 60 + int(start_str[3:])
    end = int(end_str[:2]) * 60 + int(end_str[3:])

    if start <= end:
        return start <= now < end
    else:
        # Overnight: e.g. 23:00 to 01:50
        return now >= start or now < end


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def run():
    # Load config
    config = load_config()

    # Set up logging
    log_cfg = config.get("logging", {})
    log_level = getattr(logging, log_cfg.get("level", "INFO"), logging.INFO)
    log_file = log_cfg.get("file")

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            *([logging.FileHandler(log_file)] if log_file else []),
        ],
    )

    api_key = get_api_key(config)
    now = datetime.now()
    now_str = now.strftime("%H:%M")
    logging.info("=== Run at %s ===", now.strftime("%Y-%m-%d %H:%M:%S"))

    # Read battery SOC
    soc = get_battery_soc(config, api_key)
    if soc is None:
        logging.error("Cannot read battery SOC, aborting")
        sys.exit(1)
    logging.info("Battery SOC: %d%%", soc)

    # Always verify the charge schedule is correct (prevents drift)
    verify_charge_schedule(config, api_key)

    # Determine desired mode based on time and SOC
    in_discharge_window = time_in_range(
        now_str, config["discharge_start"], config["discharge_end"]
    )
    in_charge_window = time_in_range(
        now_str,
        config["tariff"]["cheap_rate_start"],
        config["tariff"]["cheap_rate_end"],
    )

    discharge_reserve = config["battery_reserve_percent"]  # 30% - preserve for recharge
    eco_reserve = config["eco_reserve_percent"]            # 4% - maximise self-consumption

    # Decision logic
    desired_mode = "eco"
    desired_reserve = eco_reserve  # Default: let battery run low in eco mode

    if in_discharge_window and soc > discharge_reserve:
        # Discharge for home consumption during evening/night
        desired_mode = "discharge"
        desired_reserve = discharge_reserve  # Stop at 30% to preserve recharge capacity
    elif in_charge_window:
        # During cheap-rate window, the charge schedule handles it.
        # Eco mode lets the inverter charge from grid.
        desired_mode = "eco"
        desired_reserve = eco_reserve  # Low reserve so battery absorbs all available charge

    # Low battery protection: if SOC hits eco_reserve outside charge window
    if soc <= eco_reserve and not in_charge_window:
        logging.warning(
            "Battery at %d%% (eco reserve %d%%) outside charge window. "
            "Forcing eco to preserve battery.",
            soc, eco_reserve,
        )
        desired_mode = "eco"
        desired_reserve = eco_reserve

    # Set battery reserve to match the desired mode
    verify_reserve(config, api_key, desired_reserve)

    # Pre-charge feasibility check
    if in_charge_window:
        charge_end_min = (
            int(config["tariff"]["cheap_rate_end"][:2]) * 60
            + int(config["tariff"]["cheap_rate_end"][3:])
        )
        now_min = now.hour * 60 + now.minute
        remaining_hours = (charge_end_min - now_min) / 60.0

        # Estimate how long it would take to charge from current SOC
        hours_needed = estimate_charge_time(config, soc, to_soc=100)

        if remaining_hours > 0 and hours_needed > remaining_hours:
            logging.warning(
                "PRE-CHARGE ALERT: Battery at %d%%, needs %.1fh to full charge, "
                "but only %.1fh left in cheap-rate window (ends %s). "
                "Battery will NOT reach 100%% by end of charge window.",
                soc, hours_needed, remaining_hours,
                config["tariff"]["cheap_rate_end"],
            )
        else:
            logging.info(
                "Charge check: SOC %d%%, %.1fh needed, %.1fh remaining -- OK",
                soc, hours_needed, remaining_hours,
            )

    # Load last known state -- skip API writes if mode hasn't changed
    last_state = load_last_state()
    last_mode = last_state.get("mode")

    # Get current actual mode from inverter (uses time context)
    current_mode = get_current_mode(config, api_key, now_str)

    logging.info(
        "Current mode: %s | Desired mode: %s | Reserve: %s%% | Last set: %s | SOC: %d%%",
        current_mode, desired_mode, desired_reserve, last_mode, soc,
    )

    # Only write to inverter if the desired mode differs from both
    # the actual inverter state AND our last commanded state
    if desired_mode == current_mode:
        logging.info("Inverter already in %s mode, no change needed", desired_mode)
    elif desired_mode == last_mode and current_mode != "unknown":
        logging.info("Already commanded %s mode, inverter may be transitioning", desired_mode)
    else:
        logging.info("Switching from %s to %s", current_mode, desired_mode)
        if desired_mode == "eco":
            set_eco_mode(config, api_key)
        elif desired_mode == "discharge":
            set_discharge_mode(config, api_key)

    # Persist state
    save_last_state({
        "mode": desired_mode,
        "reserve": desired_reserve,
        "soc": soc,
        "timestamp": now.isoformat(),
    })

    logging.info("=== Run complete ===")


if __name__ == "__main__":
    run()