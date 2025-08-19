# services/osm.py
import os
import time
import requests
from ..storage.cache import get_cache_json, set_cache_json

USER_AGENT_EMAIL = os.getenv("USER_AGENT_EMAIL", "you@domain.tld")

def _endpoints():
    env = os.getenv("OVERPASS_URLS", "").strip()
    if env:
        return [u.strip() for u in env.split(",") if u.strip()]
    return [
        "https://overpass.kumi.systems/api/interpreter",
        "https://overpass-api.de/api/interpreter",
        "https://lz4.overpass-api.de/api/interpreter",
        "https://overpass.openstreetmap.fr/api/interpreter",
        "https://overpass.nchc.org.tw/api/interpreter",
    ]

def fetch_overpass(bbox, timeout=45):
    key = f"overpass_{','.join([f'{x:.5f}' for x in bbox])}"
    cached = get_cache_json(key, ttl=24*3600)
    if cached:
        return cached

    minx, miny, maxx, maxy = bbox
    query = f"""
    [out:json][timeout:30];
    (
      way["highway"]({miny},{minx},{maxy},{maxx});
      way["power"="line"]({miny},{minx},{maxy},{maxx});
      node["power"="substation"]({miny},{minx},{maxy},{maxx});
      way["waterway"]({miny},{minx},{maxy},{maxx});
      way["natural"="water"]({miny},{minx},{maxy},{maxx});
      way["landuse"="reservoir"]({miny},{minx},{maxy},{maxx});
      node["highway"="bus_stop"]({miny},{minx},{maxy},{maxx});
      node["public_transport"="stop_position"]({miny},{minx},{maxy},{maxx});
      node["place"~"hamlet|village|town"]({miny},{minx},{maxy},{maxx});
      way["railway"="rail"]({miny},{minx},{maxy},{maxx});
      way["man_made"="pipeline"]({miny},{minx},{maxy},{maxx});
      way["pipeline"="gas"]({miny},{minx},{maxy},{maxx});
      way["landuse"="industrial"]({miny},{minx},{maxy},{maxx});
      way["landuse"="landfill"]({miny},{minx},{maxy},{maxx});
      way["man_made"="wastewater_plant"]({miny},{minx},{maxy},{maxx});
      node["man_made"="wastewater_plant"]({miny},{minx},{maxy},{maxx});
      way["amenity"="waste_disposal"]({miny},{minx},{maxy},{maxx});
      way["amenity"="sewage_plant"]({miny},{minx},{maxy},{maxx});
      way["amenity"="grave_yard"]({miny},{minx},{maxy},{maxx});
      way["landuse"="cemetery"]({miny},{minx},{maxy},{maxx});
      node["amenity"="grave_yard"]({miny},{minx},{maxy},{maxx});
    );
    out body geom;
    """.strip()

    headers = {"User-Agent": f"LandScoreBot/0.1 ({USER_AGENT_EMAIL})"}
    errors = []
    for url in _endpoints():
        try:
            time.sleep(1.0)
            r = requests.post(url, data={"data": query}, headers=headers, timeout=timeout)
            if r.status_code == 200:
                try:
                    data = r.json()
                except ValueError:
                    errors.append(f"{url} -> bad JSON"); continue
                set_cache_json(key, data)
                return data
            errors.append(f"{url} -> HTTP {r.status_code}")
        except requests.exceptions.RequestException as e:
            errors.append(f"{url} -> {type(e).__name__}: {e}")
            continue
    raise RuntimeError("Overpass недоступен: " + " | ".join(errors[:3]))