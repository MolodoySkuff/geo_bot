# bot/providers/nspd.py
import os
import json
import time
import logging
from typing import Optional, Tuple, List

import requests
from shapely.geometry import shape, Polygon, MultiPolygon
from shapely.ops import transform
from pyproj import Transformer

CACHE_DIR        = os.getenv("CACHE_DIR", "./cache")
NSPD_URL         = os.getenv("NSPD_URL", "https://nspd.gov.ru/api/geoportal/v2/search/geoportal").strip()
NSPD_TIMEOUT     = int(os.getenv("NSPD_TIMEOUT", "12"))
NSPD_TTL_DAYS    = int(os.getenv("NSPD_TTL_DAYS", "7"))
NSPD_PROXY_URL   = os.getenv("NSPD_PROXY_URL", "").strip()
NSPD_REFERER     = os.getenv("NSPD_REFERER", "https://nspd.gov.ru/geoportal").strip()
USER_AGENT_EMAIL = os.getenv("USER_AGENT_EMAIL", "you@example.com")

UA_BROWSER = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

def _proxies():
    return {"http": NSPD_PROXY_URL, "https": NSPD_PROXY_URL} if NSPD_PROXY_URL else None

def _cache_path(cn: str) -> str:
    safe = cn.replace(":", "_")
    return os.path.join(CACHE_DIR, f"nspd_{safe}.json")

def _load_cache(cn: str) -> Optional[dict]:
    path = _cache_path(cn)
    try:
        st = os.stat(path)
        if time.time() - st.st_mtime > NSPD_TTL_DAYS * 86400:
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def _save_cache(cn: str, data: dict):
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(_cache_path(cn), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def _maybe_to_wgs84(geom):
    try:
        if isinstance(geom, (Polygon, MultiPolygon)):
            ring = list(geom.exterior.coords) if isinstance(geom, Polygon) else list(list(geom.geoms)[0].exterior.coords)
            if ring and (abs(ring[0][0]) > 1e5 or abs(ring[0][1]) > 1e5):  # EPSG:3857
                to_wgs = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True).transform
                return transform(to_wgs, geom)
    except Exception:
        pass
    return geom

def _pick_feature(features: List[dict], cadnum: str) -> Optional[dict]:
    for f in features:
        opt = ((f.get("properties") or {}).get("options") or {})
        if (opt.get("cad_num") or "").strip() == cadnum.strip():
            return f
    for f in features:
        p = f.get("properties") or {}
        if any((p.get(k) or "").strip() == cadnum.strip() for k in ("label", "descr", "externalKey")):
            return f
    return features[0] if features else None

def _shape_from_feature(feat: dict):
    gj = feat.get("geometry")
    if not gj:
        raise ValueError("NSPD: нет geometry в feature")
    return _maybe_to_wgs84(shape(gj))

def _fmt_num(n, suffix=""):
    if n in (None, "", "-", "—"): return "—"
    try:
        n = float(n); s = f"{int(n):,}".replace(",", " ")
        return f"{s} {suffix}".strip()
    except Exception:
        return str(n)

def _fmt_date(s):
    if not s: return "—"
    try:
        y, m, d = s.split("-")
        return f"{d}.{m}.{y}"
    except Exception:
        return s

def _normalize_attrs(feat: dict) -> dict:
    p = feat.get("properties") or {}
    o = p.get("options") or {}
    addr = (
        o.get("readable_address") or
        p.get("readable_address") or
        o.get("address") or
        p.get("address") or
        o.get("fullAddress") or
        p.get("fullAddress") or
        "—"
    )
    return {
        "Вид объекта недвижимости": o.get("land_record_type") or "Земельный участок",
        "Вид земельного участка": o.get("land_record_subtype") or "—",
        "Дата постановки на учёт": _fmt_date(o.get("land_record_reg_date")),
        "Кадастровый номер": o.get("cad_num") or p.get("label") or p.get("descr") or "—",
        "Кадастровый квартал": o.get("quarter_cad_number") or "—",
        "Адрес": addr,
        "Площадь уточненная": _fmt_num(o.get("specified_area"), "кв. м") if o.get("specified_area") else "—",
        "Площадь декларированная": _fmt_num(o.get("declared_area"), "кв. м"),
        "Площадь по записи": _fmt_num(o.get("land_record_area"), "кв. м"),
        "Статус": o.get("status") or "—",
        "Категория земель": o.get("land_record_category_type") or "—",
        "Вид разрешенного использования": o.get("permitted_use_established_by_document") or "—",
        "Форма собственности": o.get("ownership_type") or "—",
        "Тип права": o.get("right_type") or "—",
        "Кадастровая стоимость": _fmt_num(o.get("cost_value"), "руб."),
        "Удельный показатель кадастровой стоимости": _fmt_num(o.get("cost_index")),
        "Дата применения КС": _fmt_date(o.get("cost_application_date")),
        "Дата регистрации КС": _fmt_date(o.get("cost_registration_date")),
        "Дата определения КС": _fmt_date(o.get("cost_determination_date")),
        "Основание определения КС": o.get("determination_couse") or "—",
        "Категория набора": p.get("categoryName") or "—",
        "Дата обновления записи": (p.get("systemInfo") or {}).get("updated") or "—",
    }

def get_geometry_and_meta_by_cadnum_nspd(cadnum: str) -> Tuple[Polygon | MultiPolygon, dict]:
    cached = _load_cache(cadnum)
    if cached:
        feat = cached.get("feature") or cached
        geom = _shape_from_feature(feat)
        meta = {"source": "nspd", "source_label": "NSPD (точный контур)", "attrs": _normalize_attrs(feat), "raw": feat}
        return geom, meta

    s = requests.Session()
    try:
        s.get("https://nspd.gov.ru/", headers={
            "User-Agent": UA_BROWSER,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }, timeout=NSPD_TIMEOUT, proxies=_proxies())
    except Exception as e:
        logging.info("NSPD warmup failed: %s", e)

    params = {"query": cadnum, "thematicSearchId": 1}
    headers = {
        "User-Agent": UA_BROWSER,
        "Accept": "application/json, text/plain, */*",
        "Referer": NSPD_REFERER,
        "Origin": NSPD_REFERER.split("/geoportal")[0] if "/geoportal" in NSPD_REFERER else NSPD_REFERER.rstrip("/"),
        "X-Requested-With": "XMLHttpRequest",
    }
    logging.info("NSPD GET %s %s", NSPD_URL, params)
    r = s.get(NSPD_URL, params=params, headers=headers, timeout=NSPD_TIMEOUT, proxies=_proxies())
    if r.status_code == 403:
        h2 = headers.copy(); h2.pop("X-Requested-With", None)
        r = s.get(NSPD_URL, params=params, headers=h2, timeout=NSPD_TIMEOUT, proxies=_proxies())
    r.raise_for_status()

    data = r.json()
    features = (data.get("data") or {}).get("features") or data.get("features") or []
    if not features:
        raise RuntimeError("NSPD: объект не найден")

    feat = _pick_feature(features, cadnum)
    geom = _shape_from_feature(feat)
    _save_cache(cadnum, {"feature": feat})
    meta = {"source": "nspd", "source_label": "NSPD (точный контур)", "attrs": _normalize_attrs(feat), "raw": feat}
    return geom, meta