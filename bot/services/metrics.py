# services/metrics.py
import json
import math
import os
from typing import List, Callable

import numpy as np
from shapely.geometry import shape, Polygon, MultiPolygon, Point, LineString
from shapely.ops import unary_union, transform
from pyproj import Transformer, CRS

ROAD_TAGS_MAJOR = {"motorway", "trunk", "primary", "secondary"}
ROAD_TAGS_ALL = ROAD_TAGS_MAJOR | {"tertiary", "unclassified", "residential", "service"}

# ---------------- I/O геометрий ----------------
def read_polygon_from_file(path: str):
    ext = os.path.splitext(path)[1].lower()
    if ext.endswith("json") or ext.endswith("geojson"):
        with open(path, "r", encoding="utf-8") as f:
            gj = json.load(f)
        g = gj.get("geometry", gj)
        poly = shape(g)
        if not isinstance(poly, (Polygon, MultiPolygon)):
            raise ValueError("GeoJSON не Polygon/MultiPolygon")
        return poly
    elif ext.endswith("kml"):
        return _read_kml_polygon(path)
    else:
        raise ValueError("Поддерживаются только GeoJSON/KML")

def _read_kml_polygon(path: str):
    import xml.etree.ElementTree as ET
    ns = {"kml": "http://www.opengis.net/kml/2.2"}
    tree = ET.parse(path); root = tree.getroot()
    coords = None
    for elem in root.findall(".//kml:Polygon//kml:outerBoundaryIs//kml:LinearRing//kml:coordinates", ns):
        coords = (elem.text or "").strip(); break
    if not coords: raise ValueError("Polygon не найден в KML")
    pts = []
    for t in coords.replace("\n", " ").split():
        parts = t.split(",")
        if len(parts) >= 2:
            pts.append((float(parts[0]), float(parts[1])))
    if len(pts) < 3: raise ValueError("Слишком мало точек в KML")
    return Polygon(pts)

# ---------------- Гео‑вспомогательные ----------------
def _utm_crs_for(lon, lat):
    zone = int((lon + 180) / 6) + 1
    epsg = 32600 + zone if lat >= 0 else 32700 + zone
    return CRS.from_epsg(epsg)

def project_to_utm(geom_wgs84):
    lon, lat = geom_wgs84.centroid.x, geom_wgs84.centroid.y
    crs_utm = _utm_crs_for(lon, lat)
    to_utm = Transformer.from_crs("EPSG:4326", crs_utm, always_xy=True).transform
    to_wgs = Transformer.from_crs(crs_utm, "EPSG:4326", always_xy=True).transform
    return transform(to_utm, geom_wgs84), to_utm, to_wgs, crs_utm

def expand_bbox(bbox_wgs84, meters=2000):
    (minx, miny, maxx, maxy) = bbox_wgs84
    lat = (miny + maxy) / 2.0
    dlat = meters / 111_000.0
    dlon = meters / (111_000.0 * max(math.cos(math.radians(lat)), 0.1))
    return (minx - dlon, miny - dlat, maxx + dlon, maxy + dlat)

# ---------------- Парсинг Overpass ----------------
def _collect_geoms(overpass_data: dict, filter_fn: Callable) -> List:
    geoms = []
    for el in overpass_data.get("elements", []):
        tags = el.get("tags", {})
        typ = el.get("type")
        if not filter_fn(tags, typ): continue
        if "geometry" in el:
            coords = [(p["lon"], p["lat"]) for p in el["geometry"]]
            if typ == "way":
                # полигональные water/landfill/industrial — замкнём
                if tags.get("area") == "yes" or tags.get("natural") == "water" or tags.get("landuse") in ("reservoir","landfill","industrial","cemetery"):
                    if coords and coords[0] != coords[-1]:
                        coords = coords + [coords[0]]
                    try:
                        geoms.append(Polygon(coords))
                    except Exception:
                        geoms.append(LineString(coords))
                else:
                    geoms.append(LineString(coords))
        elif typ == "node":
            geoms.append(Point(el["lon"], el["lat"]))
    return geoms

# ---------------- Риски ----------------
def flood_risk_pct(d_water_m, rel_low_m, slope_pct) -> int:
    r = 0
    if d_water_m is not None:
        d = float(d_water_m)
        if d < 10:   r += 60
        elif d < 30: r += 45
        elif d < 100: r += 25
        elif d < 300: r += 10
    if rel_low_m is not None:
        v = float(rel_low_m)
        if v <= -2.0: r += 35
        elif v <= -1.0: r += 20
        elif v <= -0.5: r += 10
    if slope_pct is not None and float(slope_pct) <= 0.5:
        r += 5
    return int(min(100, r))

def _risk_level_label(pct: int) -> str:
    return "низкий" if pct < 35 else ("средний" if pct < 65 else "высокий")

def build_risks(metric_set: dict):
    m = metric_set
    dem = m.get("dem", {}) or {}
    risks, checks = [], []

    # Доступ
    if not m.get("touches_road"):
        risks.append("Нет примыкания к дороге — может потребоваться сервитут/правовой доступ.")
        checks.append("Проверить правовой доступ: статус дороги, сервитут/проезд.")

    # Подтопление
    fr = (m.get("risk") or {}).get("flood_pct")
    if fr is None:
        fr = flood_risk_pct(m.get("d_water_m"), dem.get("rel_lowness_m"), dem.get("slope_indicative_pct"))
        m.setdefault("risk", {})["flood_pct"] = fr
    if fr >= 65:
        risks.append(f"Высокий риск подтопления ({fr}%). Участок близко к воде/в низине.")
    elif fr >= 35:
        risks.append(f"Средний риск подтопления ({fr}%).")
    checks.append("Проверить карты паводков/ПЗУ и водоохранные зоны (региональные ГИС).")

    # Водоохранная зона
    if m.get("d_water_m") is not None and float(m["d_water_m"]) < 100:
        risks.append("Близко к воде (<100 м) — вероятны ограничения водоохранной/прибрежной зоны.")
        checks.append("Сверить границы и режимы водоохранной/прибрежной зон.")

    # ЛЭП
    if m.get("d_power_m") is not None and float(m["d_power_m"]) < 50:
        risks.append("ЛЭП ближе 50 м — возможны ограничения охранной зоны.")
        checks.append("Уточнить класс напряжения и ширину охранной зоны ЛЭП.")

    # Ж/д
    if m.get("d_rail_m") is not None and float(m["d_rail_m"]) < 200:
        risks.append("Близость железной дороги (<200 м) — шум/вибрация.")
        checks.append("Проверить шум/вибрации, ночные грузовые поезда.")

    # Газопровод
    if m.get("d_gas_m") is not None and float(m["d_gas_m"]) < 50:
        risks.append("Газопровод ближе 50 м — ограничения охранной зоны.")
        checks.append("Проверить наличие охранной зоны и запреты на строительство.")

    # Промзона
    if m.get("d_industrial_m") is not None and float(m["d_industrial_m"]) < 500:
        risks.append("Промзона рядом (<500 м) — шум/запах/трафик.")
        checks.append("Проверить тип предприятий и СЗЗ (санитарно‑защитные зоны).")

    # Полигон ТБО / очистные
    if m.get("d_landfill_m") is not None and float(m["d_landfill_m"]) < 1000:
        risks.append("Полигон ТБО/свалка вблизи (<1 км) — запах/ветровой мусор.")
        checks.append("Проверить статус полигона, розу ветров.")
    if m.get("d_wastewater_m") is not None and float(m["d_wastewater_m"]) < 700:
        risks.append("Очистные сооружения рядом (<700 м) — возможен запах.")
        checks.append("Проверить СЗЗ очистных.")

    # Кладбище (чувствительный объект)
    if m.get("d_cemetery_m") is not None and float(m["d_cemetery_m"]) < 300:
        risks.append("Кладбище рядом (<300 м) — чувствительный объект.")
        checks.append("Учесть субъективные факторы, СЗЗ.")

    # Уклон
    slope = float(dem.get("slope_indicative_pct") or 0.0)
    if slope >= 8.0:
        risks.append("Крутой уклон (>8%) — значительные земляные работы.")
        checks.append("Оценить планировку/дренаж, удобный въезд.")
    elif slope <= 0.3:
        risks.append("Очень малый уклон (<0.3%) — возможен застой воды.")
        checks.append("Предусмотреть ливнёвку/подсыпку.")

    # Узкий фасад
    if m.get("touches_road") and float(m.get("facade_len_m", 0)) < 8:
        risks.append("Узкий фасад (<8 м) — сложный въезд/разворот техники.")
        checks.append("Проверить вариант размещения ворот/въезда, нормы отступов.")

    # Далёкая инфраструктура
    far_infra = (m.get("d_stop_m") and float(m["d_stop_m"]) > 2000) or (m.get("d_place_m") and float(m["d_place_m"]) > 5000)
    if far_infra:
        risks.append("Слабая инфраструктура — далеко до остановки/населённого пункта.")
        checks.append("Оценить время в пути, сезонную доступность/снегочистку.")

    checks = list(dict.fromkeys(checks))
    return risks, checks

# ---------------- Тексты ----------------
def _dist_human(d):
    if d is None: return "—"
    d = float(d)
    if d <= 1: return "на участке"
    return f"{int(d)} м" if d < 950 else f"{d/1000:.1f} км"

def _score_label(v: int) -> str:
    if v >= 80: return "отлично"
    if v >= 60: return "хорошо"
    if v >= 40: return "удовлетворительно"
    return "слабо"

def format_brief(metric_set, addr):
    loc = addr.get("display_name", "нет адреса") if isinstance(addr, dict) else str(addr)
    area = metric_set["area_ha"]
    s = metric_set["score"]
    flood_pct = (metric_set.get("risk") or {}).get("flood_pct")
    flood_txt = f"{flood_pct}% — " + ("низкий" if flood_pct < 35 else "средний" if flood_pct < 65 else "высокий") if flood_pct is not None else "—"
    return (
        f"{loc}\n"
        f"Площадь: {area:.2f} га\n"
        f"Общая оценка: {s['total']}/100 — {_score_label(int(s['total']))}\n"
        f"Доступ {int(s['access'])}/100 • Уклон {int(s['slope'])}/100 • Вода {int(s['flood'])}/100 • Инфраструктура {int(s['infra'])}/100\n"
        f"Риск подтопления: {flood_txt}\n"
        f"Рядом (по прямой): дорога {_dist_human(metric_set.get('d_road_m'))}, вода {_dist_human(metric_set.get('d_water_m'))}, "
        f"остановка {_dist_human(metric_set.get('d_stop_m'))}, населённый пункт {_dist_human(metric_set.get('d_place_m'))}\n"
        f"Подъезд: {'есть' if metric_set['touches_road'] else 'нет'} (фасад {int(metric_set['facade_len_m'])} м) • "
        f"Дом 10×10: {'влезает' if metric_set['can_house_10x10'] else 'сомнительно'}"
    )

def format_explain(metric_set):
    dem = metric_set["dem"]
    f = (metric_set.get("risk") or {}).get("flood_pct")
    flevel = "—" if f is None else ("низкий" if f < 35 else "средний" if f < 65 else "высокий")
    parts = [
        "Как читать оценки (0–100): чем выше балл, тем лучше.",
        f"- Доступ: близость к дорогам и наличие примыкания. До дороги {_dist_human(metric_set.get('d_road_m'))}.",
        f"- Уклон: ~{dem.get('slope_indicative_pct',0):.1f}% (0–5% — комфортно для ИЖС). Большой уклон = расходы на выравнивание/дренаж.",
        f"- Вода: до реки/ручья/водоёма {_dist_human(metric_set.get('d_water_m'))}. Относительная высота: {dem.get('rel_lowness_m',0):+.1f} м. "
        f"Риск подтопления: {f}% ({flevel}).",
        f"- Инфраструктура: остановка {_dist_human(metric_set.get('d_stop_m'))}, ближайший населённый пункт {_dist_human(metric_set.get('d_place_m'))}.",
        "Примечание: расстояния по прямой; оценка подтопления — индикативная (не юридическая)."
    ]
    # Компактный чек‑лист
    checks = metric_set.get("checks_list") or []
    if checks:
        parts.append("Что проверить перед покупкой:")
        for it in checks:
            parts.append("• " + it)
    return "\n".join(parts)

# ---------------- Основной расчёт ----------------
def compute_all(geom_wgs84, osm_data, dem_stats):
    parcel_utm, to_utm, to_wgs, crs_utm = project_to_utm(geom_wgs84)
    area_m2 = parcel_utm.area
    area_ha = area_m2 / 10_000.0

    # OSM слои
    roads_major = _collect_geoms(osm_data, lambda t, typ: t.get("highway") in ROAD_TAGS_MAJOR and typ == "way")
    roads_all = _collect_geoms(osm_data, lambda t, typ: t.get("highway") in ROAD_TAGS_ALL and typ == "way")
    waters = _collect_geoms(osm_data, lambda t, typ: (t.get("waterway") or t.get("natural") == "water" or t.get("landuse") == "reservoir"))
    powers = _collect_geoms(osm_data, lambda t, typ: t.get("power") == "line")
    subst = _collect_geoms(osm_data, lambda t, typ: t.get("power") == "substation")
    stops = _collect_geoms(osm_data, lambda t, typ: (t.get("highway") == "bus_stop" or t.get("public_transport") == "stop_position"))
    places = _collect_geoms(osm_data, lambda t, typ: t.get("place") in ("hamlet", "village", "town"))
    # дополнительные риски
    rails = _collect_geoms(osm_data, lambda t, typ: t.get("railway") == "rail")
    gas_pipes = _collect_geoms(osm_data, lambda t, typ: (t.get("man_made") == "pipeline" or t.get("pipeline") == "gas"))
    industrial = _collect_geoms(osm_data, lambda t, typ: t.get("landuse") == "industrial")
    landfill = _collect_geoms(osm_data, lambda t, typ: t.get("landuse") == "landfill")
    wastewater = _collect_geoms(osm_data, lambda t, typ: t.get("man_made") == "wastewater_plant" or t.get("amenity") in ("sewage_plant","waste_disposal"))
    cemetery = _collect_geoms(osm_data, lambda t, typ: t.get("amenity") == "grave_yard" or t.get("landuse") == "cemetery")

    # Проекция в UTM
    def proj_list(lst): return [transform(to_utm, g) for g in lst]
    r_major_u = proj_list(roads_major)
    r_all_u = proj_list(roads_all)
    waters_u = proj_list(waters)
    powers_u = proj_list(powers) + proj_list(subst)
    stops_u = proj_list(stops)
    places_u = proj_list(places)
    rails_u = proj_list(rails)
    gas_u = proj_list(gas_pipes)
    industrial_u = proj_list(industrial)
    landfill_u = proj_list(landfill)
    wastewater_u = proj_list(wastewater)
    cemetery_u = proj_list(cemetery)

    def min_distance(geom, candidates):
        if not candidates: return None
        u = unary_union(candidates)
        return float(geom.distance(u))

    d_road = min_distance(parcel_utm, r_major_u) or min_distance(parcel_utm, r_all_u)
    d_water = min_distance(parcel_utm, waters_u)
    d_power = min_distance(parcel_utm, powers_u)
    d_stop = min_distance(parcel_utm, stops_u)
    d_place = min_distance(parcel_utm, places_u)
    d_rail = min_distance(parcel_utm, rails_u)
    d_gas = min_distance(parcel_utm, gas_u)
    d_industrial = min_distance(parcel_utm, industrial_u)
    d_landfill = min_distance(parcel_utm, landfill_u)
    d_wastewater = min_distance(parcel_utm, wastewater_u)
    d_cemetery = min_distance(parcel_utm, cemetery_u)

    # Касание дороги и фасад
    facade_len_m = 0.0
    touches_road = False
    if r_all_u:
        roads_buf = unary_union([g.buffer(10) for g in r_all_u])
        inter = parcel_utm.boundary.intersection(roads_buf)
        facade_len_m = float(inter.length) if not inter.is_empty else 0.0
        touches_road = facade_len_m > 0.5

    # Габариты (дом 10×10)
    mrr = parcel_utm.minimum_rotated_rectangle
    coords = list(mrr.exterior.coords)
    edges = [math.dist(coords[i], coords[(i+1)%4]) for i in range(4)]
    width, height = sorted(edges)[:2]
    can_house_10x10 = (width >= 10 and height >= 10)

    # Уклон/подтопление
    slope_pct = float(dem_stats.get("slope_indicative_pct", 5.0))
    flood_pct = flood_risk_pct(d_water, dem_stats.get("rel_lowness_m", 0.0), slope_pct)

    # Нормировка и скоринг
    def norm_inv_dist(d, good, bad):
        if d is None: return 30
        if d <= good: return 100
        if d >= bad: return 0
        return 100 * (bad - d) / (bad - good)

    score_access = norm_inv_dist(d_road or 5000, 300, 5000)
    score_slope = max(0, 100 - min(100, abs(slope_pct - 3) * 15))
    score_flood = max(0, 100 - flood_pct)
    score_infra = 0.6 * norm_inv_dist(d_stop or 4000, 500, 4000) + 0.4 * norm_inv_dist(d_place or 15000, 2000, 15000)
    score_power = norm_inv_dist(d_power or 5000, 300, 5000)

    score_total = round(
        0.25 * score_access + 0.20 * score_flood + 0.20 * score_slope +
        0.15 * score_infra + 0.10 * score_power + 0.10 * (100 if touches_road else 40)
    )

    # Риски и чек‑лист
    tmp = {
        "touches_road": touches_road,
        "facade_len_m": facade_len_m,
        "d_road_m": d_road, "d_water_m": d_water, "d_stop_m": d_stop, "d_place_m": d_place,
        "d_power_m": d_power, "d_rail_m": d_rail, "d_gas_m": d_gas,
        "d_industrial_m": d_industrial, "d_landfill_m": d_landfill, "d_wastewater_m": d_wastewater,
        "d_cemetery_m": d_cemetery,
        "dem": dem_stats,
        "risk": {"flood_pct": flood_pct},
    }
    risks, checks = build_risks(tmp)

    return {
        "area_m2": float(area_m2),
        "area_ha": float(area_ha),
        "touches_road": touches_road,
        "facade_len_m": float(facade_len_m),
        "can_house_10x10": can_house_10x10,

        "d_road_m": float(d_road) if d_road is not None else None,
        "d_water_m": float(d_water) if d_water is not None else None,
        "d_power_m": float(d_power) if d_power is not None else None,
        "d_stop_m": float(d_stop) if d_stop is not None else None,
        "d_place_m": float(d_place) if d_place is not None else None,
        "d_rail_m": float(d_rail) if d_rail is not None else None,
        "d_gas_m": float(d_gas) if d_gas is not None else None,
        "d_industrial_m": float(d_industrial) if d_industrial is not None else None,
        "d_landfill_m": float(d_landfill) if d_landfill is not None else None,
        "d_wastewater_m": float(d_wastewater) if d_wastewater is not None else None,
        "d_cemetery_m": float(d_cemetery) if d_cemetery is not None else None,

        "dem": dem_stats,
        "risk": {"flood_pct": int(flood_pct)},
        "score": {
            "access": float(score_access),
            "flood": float(score_flood),
            "slope": float(score_slope),
            "infra": float(score_infra),
            "power": float(score_power),
            "total": int(score_total),
        },
        "risks_list": risks,
        "checks_list": checks,
    }

# ---------------- Утилита для квадрата ----------------
def square_from_point_area(lat, lon, area_sot):
    area_m2 = area_sot * 100.0  # 1 сотка = 100 м²
    side = math.sqrt(area_m2)
    center = Point(lon, lat)
    poly_utm, to_utm, to_wgs, crs = project_to_utm(center.buffer(1))
    c = transform(to_utm, center)
    s = side / 2.0
    rect = Polygon([(c.x - s, c.y - s), (c.x + s, c.y - s), (c.x + s, c.y + s), (c.x - s, c.y + s)])
    rect_wgs = transform(Transformer.from_crs(crs, "EPSG:4326", always_xy=True).transform, rect)
    return rect_wgs