#!/usr/bin/env python3
"""Quick status check of the GivEnergy inverter."""
import json, os, requests

with open("/home/bob/givenergy/config.json") as f:
    config = json.load(f)

api_key = os.environ.get("GIVENERGY_API_KEY", "")
if not api_key:
    print("ERROR: GIVENERGY_API_KEY not set")
    exit(1)

headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}
serial = config["inverter_serial"]

# Get latest system data
resp = requests.get(
    f"https://api.givenergy.cloud/v1/inverter/{serial}/system-data/latest",
    headers=headers, timeout=15
)
data = resp.json().get("data", {})

# Print all top-level keys and nested
for k, v in data.items():
    if isinstance(v, dict):
        for k2, v2 in v.items():
            print(f"  {k}.{k2} = {v2}")
    else:
        print(f"  {k} = {v}")

print()
# Also read key registers
BASE = f"https://api.givenergy.cloud/v1/inverter/{serial}"
registers = {
    24: "Eco Mode",
    56: "Discharge Enable",
    53: "Discharge Start",
    54: "Discharge End",
    66: "AC Charge Enable",
    64: "AC Charge Start",
    65: "AC Charge End",
    77: "Charge Upper Limit %",
    17: "Charge Limit Enable",
    71: "Battery Reserve %",
    75: "Battery Cutoff %",
}

for reg, name in registers.items():
    try:
        r = requests.post(
            f"{BASE}/settings/{reg}/read",
            headers=headers, json={}, timeout=10
        )
        val = r.json().get("data", {}).get("value", "???")
        print(f"  Reg {reg:2d} ({name:20s}) = {val}")
    except Exception as e:
        print(f"  Reg {reg:2d} ({name:20s}) = ERROR: {e}")