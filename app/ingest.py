import requests, pandas as pd
from .config import CITIES

BASE = "https://api.open-meteo.com/v1/forecast"

def _fetch_city(city):
    r = requests.get(
        BASE,
        params={
            "latitude":  city["lat"],
            "longitude": city["lon"],
            "hourly":    "temperature_2m",
            "timezone":  "UTC",
        },
        timeout=15,
    )
    r.raise_for_status()
    hourly = r.json()["hourly"]
    df = pd.DataFrame({"ts": pd.to_datetime(hourly["time"], utc=True),
                       "value": hourly["temperature_2m"]})
    # Map to existing schema (amount := temperature)
    df["order_id"]     = df["ts"].dt.strftime(f"{city['name']}_%Y%m%d%H")
    df["customer_id"]  = city["name"]
    df["amount"]       = df["value"]
    df["currency"]     = "°C"
    df["created_at"]   = df["ts"]
    df["last_updated"] = df["ts"]
    return df[["order_id","customer_id","amount","currency","created_at","last_updated"]]

def extract_batch(since_iso=None):
    frames = []
    for c in CITIES:
        df = _fetch_city(c)
        if since_iso:
            df = df[df["last_updated"] > pd.to_datetime(since_iso, utc=True)]
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["order_id","customer_id","amount","currency","created_at","last_updated"])
