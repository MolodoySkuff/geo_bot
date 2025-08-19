# services/dem.py
import os
import math
import hashlib
import time
import requests
import numpy as np
from typing import List, Tuple

from shapely.geometry import Point
from shapely.ops import transform
from pyproj import Transformer, CRS

from ..storage.cache import get_cache_json, set_cache_json

OPENTOPO_DATASET = os.getenv("OPENTOPO_DATASET", "srtm90m").strip()
OPENTOPO_URL     = os.getenv("OPENTOPO_URL", "https://api.opentopodata.org/v1").strip()
OPEN_ELEV_URL    = os.getenv("OPEN_ELEV_URL", "https://api.open-elevation.com/api/v1/lookup").strip()
DEM_HTTP_TIMEOUT = int(os.getenv("DEM_HTTP_TIMEOUT", "30"))
USER_AGENT_EMAIL = os.getenv("USER_AGENT_EMAIL", "you@domain.tld")
HDRS = {"User-Agent": f"LandScoreBot/0.1 (+{USER_AGENT_EMAIL})"}

def _utm_crs_for(lon, lat):
    zone = int((lon + 180) / 6) + 1
    epsg = 32600 + zone if lat >= 0 else 32700 + zone
    return CRS.from_epsg(epsg)

def _project_to_utm(geom_wgs84):
    lon, lat = geom_wgs84.centroid.x, geom_wgs84.centroid.y
    crs_utm = _utm_crs_for(lon, lat)
    to_utm = Transformer.from_crs("EPSG:4326", crs_utm, always_xy=True).transform
    to_wgs = Transformer.from_crs(crs_utm, "EPSG:4326", always_xy=True).transform
    return transform(to_utm, geom_wgs84), to_utm, to_wgs, crs_utm

def _expand_grid(bounds_utm, step_m=60.0, max_pts=500):
    minx, miny, maxx, maxy = bounds_utm
    w, h = maxx - minx, maxy - miny
    nx = max(5, int(w / step_m))
    ny = max(5, int(h / step_m))
    if nx * ny > max_pts:
        k = math.sqrt((nx * ny) / max_pts)
        nx = max(5, int(nx / k))
        ny = max(5, int(ny / k))
    xs = np.linspace(minx, maxx, nx)
    ys = np.linspace(miny, maxy, ny)
    return xs, ys

def _chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def _cache_key(prefix: str, pts: List[Tuple[float, float]]):
    s = "|".join(f"{lat:.5f},{lon:.5f}" for lat, lon in pts)
    h = hashlib.md5(s.encode()).hexdigest()
    return f"{prefix}_{h}"

def _fetch_opentopo(latlons: List[Tuple[float, float]], dataset: str) -> List[float]:
    all_elev = []
    for batch in _chunk(latlons, 90):
        key = _cache_key(f"opentopo_{dataset}", batch)
        cached = get_cache_json(key, ttl=24*3600)
        if cached is not None:
            all_elev.extend(cached); continue
        qs = "|".join(f"{lat:.5f},{lon:.5f}" for lat, lon in batch)
        url = f"{OPENTOPO_URL}/{dataset}"
        r = requests.get(url, params={"locations": qs}, headers=HDRS, timeout=DEM_HTTP_TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"OpenTopoData HTTP {r.status_code}: {r.text[:200]}")
        data = r.json()
        if data.get("status") != "OK" and not data.get("results"):
            raise RuntimeError(f"OpenTopoData bad response: {data}")
        arr = [(res.get("elevation") if res else None) for res in data.get("results", [])]
        while len(arr) < len(batch): arr.append(None)
        set_cache_json(key, arr)
        all_elev.extend(arr)
        time.sleep(0.2)
    return all_elev

def _fetch_open_elevation(latlons: List[Tuple[float, float]]) -> List[float]:
    all_elev = []
    headers = {"Content-Type": "application/json", **HDRS}
    for batch in _chunk(latlons, 100):
        key = _cache_key("openelev", batch)
        cached = get_cache_json(key, ttl=12*3600)
        if cached is not None:
            all_elev.extend(cached); continue
        payload = {"locations": [{"latitude": lat, "longitude": lon} for lat, lon in batch]}
        r = requests.post(OPEN_ELEV_URL, headers=headers, json=payload, timeout=DEM_HTTP_TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"Open-Elevation HTTP {r.status_code}: {r.text[:200]}")
        data = r.json()
        results = data.get("results", [])
        arr = [(row.get("elevation") if row else None) for row in results]
        while len(arr) < len(batch): arr.append(None)
        set_cache_json(key, arr)
        all_elev.extend(arr)
        time.sleep(0.2)
    return all_elev

def _get_elevations(latlons: List[Tuple[float, float]]) -> List[float]:
    try:
        return _fetch_opentopo(latlons, OPENTOPO_DATASET)
    except Exception:
        try:
            return _fetch_open_elevation(latlons)
        except Exception:
            return [None] * len(latlons)

def compute_dem_stats(geom_wgs84, step_m=60.0, buffer_m=200) -> dict:
    parcel_utm, to_utm, to_wgs, crs_utm = _project_to_utm(geom_wgs84)
    area_buffer = transform(to_utm, geom_wgs84).buffer(buffer_m)
    xs, ys = _expand_grid(area_buffer.bounds, step_m=step_m, max_pts=500)

    pts_utm, pts_inside = [], []
    for x in xs:
        for y in ys:
            p = Point(x, y)
            if not area_buffer.contains(p): continue
            pts_utm.append(p)
            pts_inside.append(parcel_utm.contains(p))

    if not pts_utm:
        c = transform(to_utm, geom_wgs84.centroid)
        pts_utm, pts_inside = [c], [True]

    to_wgs_tf = Transformer.from_crs(crs_utm, "EPSG:4326", always_xy=True).transform
    latlons = []
    for p in pts_utm:
        lon, lat = to_wgs_tf(p.x, p.y)
        latlons.append((lat, lon))

    elev = _get_elevations(latlons)
    elev = [e for e in elev if e is not None]
    if not elev:
        return {"elev_min": 0.0, "elev_max": 0.0, "elev_med": 0.0, "elev_p95": 0.0,
                "slope_indicative_pct": 0.0, "rel_lowness_m": 0.0}

    elev_in, elev_all = [], []
    j = 0
    for i in range(len(pts_inside)):
        if j >= len(elev): break
        h = elev[j]
        if h is None: j += 1; continue
        elev_all.append(h)
        if pts_inside[i]: elev_in.append(h)
        j += 1
    if not elev_in: elev_in = elev_all[:]

    elev_in_arr = np.array(elev_in, dtype=float)
    elev_all_arr = np.array(elev_all, dtype=float)

    diffs = np.diff(np.sort(elev_in_arr)) if len(elev_in_arr) > 3 else np.array([0.0])
    slope_indicative = float(np.clip(np.std(diffs), 0, 100))

    return {
        "elev_min": float(np.min(elev_in_arr)),
        "elev_max": float(np.max(elev_in_arr)),
        "elev_med": float(np.median(elev_in_arr)),
        "elev_p95": float(np.percentile(elev_in_arr, 95)),
        "slope_indicative_pct": slope_indicative,
        "rel_lowness_m": float(np.median(elev_in_arr) - float(np.median(elev_all_arr))),
    }