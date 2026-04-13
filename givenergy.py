#!/usr/bin/env python3
"""
GivEnergy Battery Management System
Manages charge/discharge cycles to optimise against time-of-use tariffs.

Strategy:
  - Charge battery from grid during cheap-rate window (2AM-5AM)
  - Always charge to 100% (solar forecast disabled -- simple & reliable)
  - Discharge battery for home consumption during evening/night
  - Dynamic discharge: start time calculated from current SOC so battery
    hits 30% reserve exactly at the cheap-rate start (02:00)
  - Eco mode at all other times (solar self-consumption + dynamic charge/discharge)
  - Verify inverter settings each run to prevent drift
  - Only write to inverter when state actually needs to change

Hardware constraints:
  - 2.5 kW inverter, 9.5 kWh battery
  - 3h charge window (02:00-05:00) = max ~7.1 kWh chargeable (with losses)
  - Discharge window end 01:50 (10 min before cheap rate) = buffer
  - Reserve 30% during discharge ensures battery can be fully recharged in 3h window
  - Reserve 4% during eco maximises self-consumption savings
  - Li-ion taper above 80% SOC reduces avg charge rate to ~55% of max

Dynamic discharge timing:
  - drain_time = (SOC - 30%) * 9.5 / inverter_power_kw (2.5 kW)
  - In forced discharge, inverter outputs 2.5kW regardless of house load
    (house takes ~0.45kW, rest exports to grid at 25c/kWh -- all valuable)
  - start_time = 01:50 - drain_time (with safety margin)
  - Adjusts every 10 min via cron as SOC changes throughout the day
  - Minimum discharge window 30min
"""

import csv
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).parent / "config.json"
STATE_PATH = Path(__file__).parent / ".last_state.json"
FORECAST_LOG_PATH = Path(__file__).parent / "forecast_log.csv"
DAILY_LOG_PATH = Path(__file__).parent / "daily_log.csv"

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
FORECAST_TIMEOUT = 10  # seconds for forecast.solar API


# ---------------------------------------------------------------------------
# Solar Forecast (forecast.solar)
# ---------------------------------------------------------------------------

def fetch_solar_forecast(config):
    """Fetch tomorrow's solar production forecast from forecast.solar API.

    Returns the predicted daily energy in kWh, or None on failure.
    Uses the free tier (12 req/hr, no API key).

    API URL: api.forecast.solar/estimate/{lat}/{lon}/{tilt}/{azimuth}/{kwp}
    Azimuth convention: 0=South, positive=West, negative=East
    Compass 217deg (from N clockwise) = 37deg West of South = +37
    """
    fc = config.get("solar_forecast", {})
    if not fc.get("enabled", False):
        return None

    lat = fc.get("latitude", 53.34)
    lon = fc.get("longitude", -6.26)
    tilt = fc.get("tilt", 37)
    azim = fc.get("azimuth", 37)
    kwp = fc.get("kwp", 2.5)

    url = f"https://api.forecast.solar/estimate/{lat}/{lon}/{tilt}/{azim}/{kwp}"
    try:
        resp = requests.get(url, timeout=FORECAST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        wh_day = data.get("result", {}).get("watt_hours_day", {})
        if not wh_day:
            logging.warning("No watt_hours_day in forecast response")
            return None

        # Get tomorrow's forecast (the second date key, sorted chronologically)
        dates = sorted(wh_day.keys())
        tomorrow_wh = wh_day.get(dates[-1]) if len(dates) >= 2 else None
        today_wh = wh_day.get(dates[0]) if len(dates) >= 1 else None

        # If we can't get tomorrow's, fall back to today's remaining
        target_wh = tomorrow_wh or today_wh
        if target_wh is None:
            logging.warning("Could not extract forecast Wh from response")
            return None

        kwh = target_wh / 1000.0
        date_str = dates[-1] if len(dates) >= 2 else dates[0] if dates else "unknown"
        logging.info(
            "Solar forecast for %s: %d Wh = %.1f kWh (today: %s)",
            date_str, target_wh, kwh,
            f"{today_wh} Wh" if today_wh else "N/A",
        )
        return kwh

    except requests.RequestException as e:
        logging.warning("Solar forecast API failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Forecast & Daily Tracking Logs
# ---------------------------------------------------------------------------

def log_forecast(config, forecast_kwh, target_date, charge_limit, daytime_load):
    """Record a solar forecast prediction to the CSV log.

    Columns: date, target_date, forecast_kwh, actual_kwh, error_pct,
             charge_limit, daytime_load, actual_filled (backfilled later)

    This lets us measure forecast accuracy over time and tune the
    confidence factor and add bias corrections.
    """
    row = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "target_date": target_date,
        "forecast_kwh": f"{forecast_kwh:.2f}" if forecast_kwh else "",
        "actual_kwh": "",           # backfilled by backfill_forecast_actuals()
        "error_pct": "",            # backfilled
        "charge_limit": charge_limit,
        "daytime_load": f"{daytime_load:.1f}" if daytime_load else "",
        "actual_filled": "no",
    }
    file_exists = FORECAST_LOG_PATH.exists()
    try:
        with open(FORECAST_LOG_PATH, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
        logging.info("Forecast logged: target=%s forecast=%.1f kWh charge_limit=%d%%",
                     target_date, forecast_kwh or 0, charge_limit)
    except OSError as e:
        logging.warning("Could not write forecast log: %s", e)


def backfill_forecast_actuals(config, api_key):
    """Fill in actual solar production for past forecast entries.

    Reads the forecast_log.csv, finds rows where actual_kwh is empty,
    fetches real solar from the GivEnergy energy-flows API for that date,
    and writes it back. Also calculates error percentage.
    """
    if not FORECAST_LOG_PATH.exists():
        return

    rows = []
    updated = 0
    try:
        with open(FORECAST_LOG_PATH, "r", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                if not row.get("actual_kwh") and row.get("target_date"):
                    target = row["target_date"]
                    # Only backfill if the target date is in the past
                    try:
                        target_dt = datetime.strptime(target, "%Y-%m-%d")
                    except ValueError:
                        rows.append(row)
                        continue
                    if target_dt.date() >= datetime.now().date():
                        rows.append(row)
                        continue

                    # Fetch actual solar from energy-flows API
                    next_day = (target_dt + timedelta(days=1)).strftime("%Y-%m-%d")
                    actual = _fetch_actual_solar(config, api_key, target, next_day)
                    if actual is not None:
                        row["actual_kwh"] = f"{actual:.2f}"
                        row["actual_filled"] = "yes"
                        # Calculate error
                        try:
                            forecast_val = float(row["forecast_kwh"])
                            if forecast_val > 0:
                                error = (actual - forecast_val) / forecast_val * 100
                                row["error_pct"] = f"{error:.1f}"
                        except (ValueError, ZeroDivisionError):
                            pass
                        updated += 1
                rows.append(row)

        if updated > 0:
            with open(FORECAST_LOG_PATH, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            logging.info("Backfilled %d forecast actuals", updated)

    except (OSError, csv.Error) as e:
        logging.warning("Forecast log backfill failed: %s", e)


def _fetch_actual_solar(config, api_key, start_date, end_date):
    """Fetch actual solar generation (kWh) for a date from energy-flows API.

    Returns total kWh or None. Solar = type 0 + type 1 + type 2.
    """
    serial = config["inverter_serial"]
    url = f"{BASE_URL}/{serial}/energy-flows"
    try:
        resp = requests.post(
            url,
            headers=make_headers(api_key),
            json={"start_time": start_date, "end_time": end_date, "grouping": 0},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})
        total_solar = 0.0
        for slot in data.values():
            sd = slot.get("data", {})
            total_solar += float(sd.get("0", 0))  # PV to Home
            total_solar += float(sd.get("1", 0))  # PV to Battery
            total_solar += float(sd.get("2", 0))  # PV to Grid
        return total_solar if total_solar > 0 else None
    except Exception as e:
        logging.warning("Failed to fetch actual solar for %s: %s", start_date, e)
        return None


def log_daily(config, api_key, soc):
    """Record daily stats to CSV: consumption, solar, grid, battery, export.

    Called once per day on the first run after midnight. Pulls yesterday's
    energy-flows data and writes one summary row.
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")

    serial = config["inverter_serial"]
    url = f"{BASE_URL}/{serial}/energy-flows"
    try:
        resp = requests.post(
            url,
            headers=make_headers(api_key),
            json={"start_time": yesterday, "end_time": today, "grouping": 0},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})
    except Exception as e:
        logging.warning("Daily log: failed to fetch energy-flows for %s: %s", yesterday, e)
        return

    charge_start_h = int(config["tariff"]["cheap_rate_start"][:2])
    charge_end_h = int(config["tariff"]["cheap_rate_end"][:2])
    charge_hours = set(range(charge_start_h, charge_end_h))

    totals = {t: 0.0 for t in range(7)}
    for slot in data.values():
        sd = slot.get("data", {})
        for t, val in sd.items():
            totals[int(t)] += float(val)

    # House consumption (excluding charge-window grid charging)
    # Grid>Home during charge hours includes battery charge, so separate it
    house_cons = totals[0] + totals[4]  # PV>Home + Batt>Home (always real)
    # Grid>Home outside charge window
    grid_home_real = 0.0
    grid_home_charge_window = 0.0
    for slot in data.values():
        start_time = slot.get("start_time", "")
        h = int(start_time[11:13]) if len(start_time) > 13 else -1
        sd = slot.get("data", {})
        gh = float(sd.get("5", 0))
        if h in charge_hours:
            grid_home_charge_window += gh
        else:
            grid_home_real += gh
    house_cons += grid_home_real

    row = {
        "date": yesterday,
        "solar_kwh": f"{totals[0] + totals[1] + totals[2]:.2f}",
        "house_consumption_kwh": f"{house_cons:.2f}",
        "pv_to_home_kwh": f"{totals[0]:.2f}",
        "pv_to_battery_kwh": f"{totals[1]:.2f}",
        "pv_to_grid_kwh": f"{totals[2]:.2f}",
        "grid_to_battery_kwh": f"{totals[3]:.2f}",
        "battery_to_home_kwh": f"{totals[4]:.2f}",
        "grid_to_home_kwh": f"{grid_home_real:.2f}",
        "grid_home_charge_window_kwh": f"{grid_home_charge_window:.2f}",
        "export_kwh": f"{totals[2]:.2f}",
        "soc_morning": "",       # TODO: capture from first run of day
        "soc_evening": "",       # TODO: capture from last run before midnight
    }

    file_exists = DAILY_LOG_PATH.exists()
    try:
        with open(DAILY_LOG_PATH, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
        logging.info("Daily log written for %s: solar=%.1f house=%.1f",
                     yesterday, totals[0] + totals[1] + totals[2], house_cons)
    except OSError as e:
        logging.warning("Could not write daily log: %s", e)


def fetch_consumption_profile(config, api_key, days=7):
    """Fetch real consumption from the GivEnergy energy-flows API.

    Pulls the last N days of half-hourly energy flow data and calculates:
    - Average daytime house consumption (08:00-18:00)
    - Average daily total house consumption
    - Average evening/night consumption

    Energy flow types from the API:
      0 = PV to Home (solar directly consumed)
      1 = PV to Battery (solar stored)
      2 = PV to Grid (solar exported)
      3 = Grid to Battery (night grid charging)
      4 = Battery to Home (battery powering house)
      5 = Grid to Home (grid powering house)

    House consumption = type 0 + type 4 + type 5 OUTSIDE the charge window.
    During the 02:00-05:00 charge window, type 5 (Grid>Home) includes battery
    charging current, so we exclude it and only count type 0 + type 4.

    Returns dict with 'daytime_load_kwh', 'daily_total_kwh', 'days_sampled',
    or None on failure.
    """
    serial = config["inverter_serial"]
    url = f"{BASE_URL}/{serial}/energy-flows"

    # Fetch data in weekly chunks to avoid large responses
    from datetime import timedelta
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    all_daily = {}

    # Fetch in chunks of 7 days max
    chunk_start = start_date
    while chunk_start < end_date:
        chunk_end = min(chunk_start + timedelta(days=8), end_date)
        try:
            resp = requests.post(
                url,
                headers=make_headers(api_key),
                json={
                    "start_time": chunk_start.strftime("%Y-%m-%d"),
                    "end_time": chunk_end.strftime("%Y-%m-%d"),
                    "grouping": 0,  # half-hourly slots
                },
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
        except Exception as e:
            logging.warning("Energy-flows fetch failed for %s: %s",
                             chunk_start.strftime("%Y-%m-%d"), e)
            chunk_start = chunk_end
            continue

        charge_start_h = int(config["tariff"]["cheap_rate_start"][:2])
        charge_end_h = int(config["tariff"]["cheap_rate_end"][:2])
        charge_hours = set(range(charge_start_h, charge_end_h))

        # Aggregate per day
        for slot_key, slot in data.items():
            start_time = slot.get("start_time", "")
            date = start_time[:10]
            if date not in all_daily:
                all_daily[date] = {
                    "daytime_cons": 0.0,   # 08:00-18:00 house consumption
                    "total_cons": 0.0,     # full day house consumption
                }

            hour = int(start_time[11:13]) if len(start_time) > 13 else -1
            slot_data = slot.get("data", {})
            pv_home = float(slot_data.get("0", 0))
            batt_home = float(slot_data.get("4", 0))
            grid_home = float(slot_data.get("5", 0))

            # During charge window, Grid>Home includes charging current
            # so only count PV>Home + Batt>Home
            if hour in charge_hours:
                house_this_slot = pv_home + batt_home
            else:
                house_this_slot = pv_home + batt_home + grid_home

            all_daily[date]["total_cons"] += house_this_slot
            if 8 <= hour < 18:  # solar daytime hours
                all_daily[date]["daytime_cons"] += house_this_slot

        chunk_start = chunk_end

    if not all_daily:
        logging.warning("No consumption data retrieved from energy-flows API")
        return None

    # Calculate averages
    daytime_loads = [d["daytime_cons"] for d in all_daily.values()
                     if d["daytime_cons"] > 0]
    daily_totals = [d["total_cons"] for d in all_daily.values()
                    if d["total_cons"] > 0]

    if not daytime_loads:
        return None

    avg_daytime = sum(daytime_loads) / len(daytime_loads)
    avg_daily = sum(daily_totals) / len(daily_totals) if daily_totals else 0

    result = {
        "daytime_load_kwh": round(avg_daytime, 1),
        "daily_total_kwh": round(avg_daily, 1),
        "days_sampled": len(daytime_loads),
        "sampled_dates": sorted(all_daily.keys())[:3] + ["..."] + sorted(all_daily.keys())[-3:] if len(all_daily) > 6 else sorted(all_daily.keys()),
    }
    logging.info(
        "Consumption profile: %d days sampled, avg daytime %.1f kWh, "
        "avg daily total %.1f kWh",
        result["days_sampled"], result["daytime_load_kwh"], result["daily_total_kwh"],
    )
    return result


def get_effective_daytime_load(config, last_state):
    """Get the daytime load to use for charge limit calculations.

    Priority:
    1. Cached value from recent API fetch (if less than 24h old)
    2. Config file default
    """
    fc = config.get("solar_forecast", {})
    config_default = fc.get("daytime_load_kwh", 5.0)

    cached = last_state.get("consumption_profile", {})
    if cached:
        from datetime import timedelta
        try:
            cached_time = datetime.fromisoformat(cached["fetched_at"])
            age_hours = (datetime.now() - cached_time).total_seconds() / 3600
            if age_hours < 24:
                logging.info(
                    "Using cached daytime load %.1f kWh (fetched %.1fh ago, %d days sampled)",
                    cached["daytime_load_kwh"], age_hours,
                    cached.get("days_sampled", 0),
                )
                return cached["daytime_load_kwh"]
        except (ValueError, KeyError):
            pass

    logging.info("Using config default daytime load %.1f kWh", config_default)
    return config_default


def calculate_dynamic_charge_limit(config, forecast_kwh, daytime_load=None):
    """Calculate the AC Charge Upper Limit % based on solar forecast.

    Logic: If tomorrow's solar will produce more than the house uses during
    daytime, the excess needs somewhere to go. By leaving battery headroom
    equal to that excess, eco mode can store the surplus instead of exporting.

    SAFETY: We apply a confidence factor (default 0.6) to hedge against
    optimistic forecasts. The cost of undercharging (buying at 38c) is ~2.5x
    the cost of overcharging (losing 13c/kWh export arbitrage). So we only
    reduce the charge target by 60% of the predicted headroom.

    Args:
        config: Full config dict
        forecast_kwh: Tomorrow's predicted solar (kWh), or None
        daytime_load: Override daytime house consumption (kWh). If None,
                      uses get_effective_daytime_load() from config/state.

    Returns: target SOC % for the AC Charge Upper Limit (min_charge-100)
    """
    if forecast_kwh is None:
        return 100  # No forecast -> charge full

    fc = config.get("solar_forecast", {})
    if daytime_load is None:
        daytime_load = fc.get("daytime_load_kwh", 5.0)
    min_charge = fc.get("min_charge_percent", 70)  # conservative floor
    confidence = fc.get("forecast_confidence", 0.6)  # hedge factor
    capacity = config["battery_capacity_kwh"]
    charge_eff = config.get("charge_efficiency", 0.95)

    # Excess solar = production minus what the house will consume during solar hours
    excess_solar = max(0, forecast_kwh - daytime_load)

    if excess_solar <= 0:
        logging.info(
            "Forecast %.1f kWh <= daytime load %.1f kWh -> charge to 100%%",
            forecast_kwh, daytime_load,
        )
        return 100

    # How much battery capacity (in %) should we reserve for solar?
    # Apply confidence factor: only trust 60% of the forecast excess
    headroom_kwh = excess_solar * charge_eff * confidence
    headroom_pct = min(30, headroom_kwh / capacity * 100)  # cap at 30% headroom

    target_soc = 100 - headroom_pct
    target_soc = max(min_charge, min(100, int(target_soc)))

    logging.info(
        "Dynamic charge: forecast %.1f kWh, daytime load %.1f kWh, excess %.1f kWh, "
        "confident headroom %.1f kWh (%.0f%%) -> charge to %d%%",
        forecast_kwh, daytime_load, excess_solar, headroom_kwh, headroom_pct, target_soc,
    )
    return target_soc


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
    """Read a single inverter setting register.

    Returns the register value, or None on failure.
    Filters out API error values (-6 = comms timeout, -1 = unknown).
    """
    result = api_post(config, api_key, f"settings/{register_id}/read", {})
    if result and "data" in result and "value" in result["data"]:
        value = result["data"]["value"]
        success = result["data"].get("success", True)
        # API returns -6 for comms timeout, -1 for unknown errors
        if value in (-6, -1) or success is False:
            logging.warning(
                "Register %s read returned error value %s (success=%s)",
                register_id, value, success,
            )
            return None
        return value
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


def calculate_discharge_start(soc, config):
    """Calculate the optimal discharge start time so battery hits reserve
    at the charge window start.

    Works backwards from the discharge end time (10 min before cheap rate):
      1. How much energy must drain? (SOC - reserve) * capacity
      2. How long at inverter power? drain_kwh / inverter_power_kw
         In forced discharge, the inverter pumps out at full rate regardless
         of house load. House takes what it needs, rest exports to grid --
         both are financially valuable.
      3. Apply safety margin (start slightly earlier)
      4. start_time = discharge_end - drain_time

    Returns: "HH:MM" string for the calculated start time.

    Clamp rules:
      - Never shorter than `min_discharge_minutes` (minimum discharge window)
      - If SOC already at/below reserve, don't discharge at all (return None)
    """
    dyn = config.get("dynamic_discharge", {})
    if not dyn.get("enabled", False):
        # Fallback to fixed time from config
        return config.get("discharge_start", "23:00")

    reserve = config["battery_reserve_percent"]
    capacity = config["battery_capacity_kwh"]
    inverter_power_kw = config.get("inverter_power_kw", 2.5)
    min_minutes = dyn.get("min_discharge_minutes", 30)
    safety = dyn.get("safety_margin", 0.9)  # multiply drain time by this to start earlier

    # If already at or below reserve, no discharge needed
    if soc <= reserve:
        logging.info(
            "Dynamic discharge: SOC %d%% <= reserve %d%%, no discharge needed",
            soc, reserve,
        )
        return None

    # Energy to drain from SOC down to reserve
    drain_pct = soc - reserve
    drain_kwh = drain_pct / 100.0 * capacity

    # Time to drain at inverter discharge power
    # In forced discharge mode, the inverter outputs at full power;
    # house absorbs ~0.45kW, the rest exports to grid at 25c/kWh
    drain_hours = drain_kwh / inverter_power_kw

    # Apply safety margin: start earlier so we hit reserve *before* charge window
    # safety < 1 means "I only trust 90% of the estimate, so start 10% earlier"
    drain_hours_adjusted = drain_hours / safety

    # Clamp to minimum discharge window
    drain_minutes_adjusted = max(drain_hours_adjusted * 60, min_minutes)
    drain_hours_adjusted = drain_minutes_adjusted / 60.0

    # Discharge end time in minutes from midnight (10 min before cheap rate)
    discharge_end = config.get("discharge_end", config["tariff"]["cheap_rate_start"])
    end_min = int(discharge_end[:2]) * 60 + int(discharge_end[3:])

    # Discharge start = end time - adjusted drain time
    start_min = end_min - int(drain_hours_adjusted * 60)

    # Handle overnight wrap: if start_min goes negative, it means start previous day
    # E.g., cheap_start=02:00 (120min), drain 5h (300min) -> start=-180 -> previous day 21:00
    if start_min < 0:
        start_min += 24 * 60  # wrap to previous day

    # Note: no earliest-start clamp -- pure calculation.
    # Inverter-rate discharge empties the battery fast (2.5kW),
    # so the start time is naturally close to the end time.
    # If we clamped to 18:00, the battery would hit reserve hours early
    # and sit idle instead of providing eco mode self-consumption.

    start_hhmm = f"{start_min // 60:02d}:{start_min % 60:02d}"

    logging.info(
        "Dynamic discharge: SOC %d%% -> reserve %d%% = %.2f kWh to drain, "
        "inverter %.1f kW, drain_time %.1fh (adj %.1fh), "
        "start %s -> end %s",
        soc, reserve, drain_kwh, inverter_power_kw,
        drain_hours, drain_hours_adjusted,
        start_hhmm, discharge_end,
    )

    return start_hhmm


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


def set_discharge_mode(config, api_key, start_time=None):
    """Switch to timed discharge mode: eco off, discharge enabled, high reserve for recharge.
    
    Args:
        start_time: HH:MM string for discharge start. If None, uses config default.
    """
    discharge_reserve = config["battery_reserve_percent"]
    actual_start = start_time or config.get("discharge_start", "23:00")
    discharge_end = config.get("discharge_end", config["tariff"]["cheap_rate_start"])
    logging.info("Setting DISCHARGE mode (%s - %s, reserve %s%%)",
                 actual_start, discharge_end, discharge_reserve)
    write_register(config, api_key, REG_ECO_MODE, False)
    write_register(config, api_key, REG_DISCHARGE_ENABLE, True)
    write_register(config, api_key, REG_DISCHARGE_START, actual_start)
    write_register(config, api_key, REG_DISCHARGE_END, discharge_end)
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


def verify_charge_limit(config, api_key, desired_limit):
    """Set the AC Charge Upper Limit % to control how full the battery charges.

    100% = charge fully (default, no solar headroom)
    Lower = leave headroom for solar capture during the day.

    Must also enable the charge limit (reg 17) when setting < 100%.
    """
    current_limit = read_register(config, api_key, REG_CHARGE_UPPER_LIMIT)
    current_enable = read_register(config, api_key, REG_CHARGE_UPPER_LIMIT_ENABLE)

    needs_update = False

    if current_limit != desired_limit:
        logging.info("Charge upper limit: %s%% -> %s%%",
                     current_limit, desired_limit)
        write_register(config, api_key, REG_CHARGE_UPPER_LIMIT, desired_limit)
        needs_update = True

    # Enable/disable the limit flag based on whether limit < 100
    should_enable = desired_limit < 100
    if current_enable is not True and should_enable:
        logging.info("Enabling charge upper limit (reg 17)")
        write_register(config, api_key, REG_CHARGE_UPPER_LIMIT_ENABLE, True)
    elif current_enable is not False and not should_enable:
        logging.info("Disabling charge upper limit (reg 17 -- back to 100%%)")
        write_register(config, api_key, REG_CHARGE_UPPER_LIMIT_ENABLE, False)

    return needs_update


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

def check_heartbeat(config):
    """Check if the system is healthy by examining recent run timestamps.

    Returns (ok, message) tuple. If the last successful run was too long ago,
    or if there have been too many consecutive errors, raises an alert.
    """
    last_state = load_last_state()
    now = datetime.now()

    last_ts_str = last_state.get("timestamp")
    if not last_ts_str:
        return True, "No previous state (first run?)"

    try:
        last_ts = datetime.fromisoformat(last_ts_str)
    except (ValueError, TypeError):
        return True, "Could not parse last timestamp"

    age_minutes = (now - last_ts).total_seconds() / 60.0
    consecutive_errors = last_state.get("consecutive_errors", 0)

    # If last successful run was > 1 hour ago, something may be stuck
    if age_minutes > 60:
        return False, f"Last successful run was {age_minutes:.0f} min ago -- STALE"

    # If 3+ consecutive errors, system may be stuck in a loop
    if consecutive_errors >= 3:
        return False, f"{consecutive_errors} consecutive errors -- UNHEALTHY"

    return True, f"Last run {age_minutes:.0f} min ago, {consecutive_errors} errors"


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

    # Heartbeat check
    hb_ok, hb_msg = check_heartbeat(config)
    if hb_ok:
        logging.info("Heartbeat: OK (%s)", hb_msg)
    else:
        logging.warning("Heartbeat: ALERT -- %s", hb_msg)

    # Read battery SOC
    soc = get_battery_soc(config, api_key)
    if soc is None:
        logging.error("Cannot read battery SOC, aborting")
        # Track consecutive errors
        last_state = load_last_state()
        errors = last_state.get("consecutive_errors", 0) + 1
        save_last_state({
            **last_state,
            "consecutive_errors": errors,
            "last_error": now.isoformat(),
        })
        sys.exit(1)
    logging.info("Battery SOC: %d%%", soc)

    # Always verify the charge schedule is correct (prevents drift)
    verify_charge_schedule(config, api_key)

    # -----------------------------------------------------------------------
    # Charge limit: always 100% (dynamic forecast disabled)
    # -----------------------------------------------------------------------
    in_charge_window = time_in_range(
        now_str,
        config["tariff"]["cheap_rate_start"],
        config["tariff"]["cheap_rate_end"],
    )

    fc = config.get("solar_forecast", {})
    forecast_enabled = fc.get("enabled", False)
    desired_charge_limit = 100
    daytime_load = fc.get("daytime_load_kwh", 5.0)

    if forecast_enabled:
        # Dynamic forecast mode: fetch forecast, calculate charge limit
        last_state = load_last_state()
        daytime_load = get_effective_daytime_load(config, last_state)

        # Refresh consumption profile once per day
        need_refresh = False
        cached_profile = last_state.get("consumption_profile", {})
        if not cached_profile:
            need_refresh = True
        else:
            try:
                cached_time = datetime.fromisoformat(cached_profile.get("fetched_at", ""))
                if (datetime.now() - cached_time).total_seconds() > 24 * 3600:
                    need_refresh = True
            except (ValueError, TypeError):
                need_refresh = True

        if need_refresh and in_charge_window:
            logging.info("Refreshing consumption profile from energy-flows API...")
            profile = fetch_consumption_profile(config, api_key, days=7)
            if profile:
                profile["fetched_at"] = now.isoformat()
                last_state["consumption_profile"] = profile
                daytime_load = profile["daytime_load_kwh"]
                logging.info(
                    "Updated daytime load from API: %.1f kWh (was %.1f kWh default)",
                    daytime_load, fc.get("daytime_load_kwh", 5.0),
                )

        # Fetch forecast and calculate dynamic charge limit
        if in_charge_window or now.hour < 5:
            forecast_kwh = fetch_solar_forecast(config)
            desired_charge_limit = calculate_dynamic_charge_limit(
                config, forecast_kwh, daytime_load=daytime_load,
            )
            if forecast_kwh and in_charge_window and now.hour < 3:
                target_dt = now + timedelta(days=1)
                forecast_target_date = target_dt.strftime("%Y-%m-%d")
                log_forecast(config, forecast_kwh, forecast_target_date,
                             desired_charge_limit, daytime_load)
        else:
            logging.info("Outside forecast window -- charge limit stays at 100%%")

        # Backfill forecast actuals + daily log (once per day, early morning)
        if in_charge_window and now.hour == 2:
            backfill_forecast_actuals(config, api_key)
            log_daily(config, api_key, soc)
    else:
        logging.info("Solar forecast disabled -- always charging to 100%%")

    verify_charge_limit(config, api_key, desired_charge_limit)

    # -----------------------------------------------------------------------
    # Mode decision
    # -----------------------------------------------------------------------
    # Calculate dynamic discharge start time (based on current SOC)
    dynamic_start = calculate_discharge_start(soc, config)
    discharge_end = config.get("discharge_end", config["tariff"]["cheap_rate_start"])

    # If dynamic start is None, SOC is already at/below reserve - skip discharge
    if dynamic_start is None:
        in_discharge_window = False
        logging.info("No discharge window: SOC already at/below reserve")
    else:
        in_discharge_window = time_in_range(now_str, dynamic_start, discharge_end)

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

    # Pre-charge feasibility check (uses dynamic charge limit, not hardcoded 100)
    if in_charge_window:
        charge_end_min = (
            int(config["tariff"]["cheap_rate_end"][:2]) * 60
            + int(config["tariff"]["cheap_rate_end"][3:])
        )
        now_min = now.hour * 60 + now.minute
        remaining_hours = (charge_end_min - now_min) / 60.0

        # Estimate how long it would take to charge to the dynamic limit
        hours_needed = estimate_charge_time(config, soc, to_soc=desired_charge_limit)

        if remaining_hours > 0 and hours_needed > remaining_hours:
            logging.warning(
                "PRE-CHARGE ALERT: Battery at %d%%, needs %.1fh to %d%% charge, "
                "but only %.1fh left in cheap-rate window (ends %s). "
                "Battery will NOT reach %d%% by end of charge window.",
                soc, hours_needed, desired_charge_limit, remaining_hours,
                config["tariff"]["cheap_rate_end"], desired_charge_limit,
            )
        else:
            logging.info(
                "Charge check: SOC %d%%, %.1fh needed to %d%%, %.1fh remaining -- OK",
                soc, hours_needed, desired_charge_limit, remaining_hours,
            )

    # Load last known state -- skip API writes if mode hasn't changed
    # (already loaded earlier for consumption profile, reload to be safe)
    last_state = load_last_state()
    last_mode = last_state.get("mode")

    # Get current actual mode from inverter (uses time context)
    current_mode = get_current_mode(config, api_key, now_str)

    logging.info(
        "Current mode: %s | Desired mode: %s | Reserve: %s%% | "
        "Charge limit: %d%% | Discharge: %s-%s | Last set: %s | SOC: %d%%",
        current_mode, desired_mode, desired_reserve,
        desired_charge_limit, dynamic_start or "N/A", discharge_end,
        last_mode, soc,
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
            set_discharge_mode(config, api_key, start_time=dynamic_start)

    # Persist state (reset consecutive_errors on success)
    # Preserve consumption_profile cache for next run
    save_last_state({
        "mode": desired_mode,
        "reserve": desired_reserve,
        "charge_limit": desired_charge_limit,
        "discharge_start": dynamic_start,
        "soc": soc,
        "timestamp": now.isoformat(),
        "consecutive_errors": 0,
        "heartbeat_ok": hb_ok,
        "consumption_profile": last_state.get("consumption_profile", {}),
        "daytime_load_kwh": daytime_load,
    })

    logging.info("=== Run complete ===")


if __name__ == "__main__":
    run()