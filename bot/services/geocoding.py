# services/geocoding.py
import os
import time
import requests
from ..storage.cache import get_cache_json, set_cache_json

NOMINATIM_URL   = os.getenv("NOMINATIM_URL", "https://nominatim.openstreetmap.org/reverse")
USER_AGENT_EMAIL= os.getenv("USER_AGENT_EMAIL", "youremail@example.com")
GEOCODING_DELAY = float(os.getenv("GEOCODING_DELAY", "1.0"))

def reverse_geocode(lat, lon):
    key = f"nominatim_{lat:.5f}_{lon:.5f}"
    cached = get_cache_json(key, ttl=7*24*3600)
    if cached: return cached
    params  = {"lat": lat, "lon": lon, "format": "jsonv2", "zoom": 14, "addressdetails": 1}
    headers = {"User-Agent": f"LandScoreBot/0.1 (+{USER_AGENT_EMAIL})", "Accept-Language": "ru"}
    time.sleep(GEOCODING_DELAY)
    try:
        r = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=20)
        if r.status_code == 429:
            time.sleep(1.0)
            r = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException:
        data = {"display_name": f"{lat:.5f},{lon:.5f}", "address": {}}
    set_cache_json(key, data)
    return data