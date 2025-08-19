# services/map_render.py
import os
import uuid
import math
from staticmap import StaticMap, Polygon as SMPolygon, Line

def _extract_ring_coords(geom):
    g = geom
    if g.geom_type == "MultiPolygon":
        g = list(g.geoms)[0]
    coords = list(g.exterior.coords)
    if coords and (coords[0][0] != coords[-1][0] or coords[0][1] != coords[-1][1]):
        coords.append(coords[0])
    return [(float(x), float(y)) for (x, y) in coords]

def _deg_pad(meters, lat):
    dlat = meters / 111_000.0
    dlon = meters / (111_000.0 * max(math.cos(math.radians(lat)), 0.1))
    return dlon, dlat

def _compute_zoom(width_m, height_m, lat, viewport=(800, 600), margin=0.12):
    def z_for(target_mpp):
        return math.log2(156543.03392 * math.cos(math.radians(lat)) / max(target_mpp, 0.1))
    vw, vh = viewport
    target_x = width_m / max(vw * (1 - margin), 1)
    target_y = height_m / max(vh * (1 - margin), 1)
    z = min(z_for(target_x), z_for(target_y))
    return max(12, min(19, int(round(z))))

def _bbox_expand(bounds, pad_m=1500):
    minx, miny, maxx, maxy = bounds
    lat = (miny + maxy) / 2.0
    dlon, dlat = _deg_pad(pad_m, lat)
    return (minx - dlon, miny - dlat, maxx + dlon, maxy + dlat)

def _in_bbox(lon, lat, bbox):
    minx, miny, maxx, maxy = bbox
    return (minx <= lon <= maxx) and (miny <= lat <= maxy)

def render_static_map(geom_wgs84, osm_data, out_dir="cache/maps"):
    os.makedirs(out_dir, exist_ok=True)

    width, height = 800, 600
    m = StaticMap(width, height, url_template="https://a.tile.openstreetmap.org/{z}/{x}/{y}.png")

    ring = _extract_ring_coords(geom_wgs84)
    lon_vals = [p[0] for p in ring]
    lat_vals = [p[1] for p in ring]
    minx, maxx = min(lon_vals), max(lon_vals)
    miny, maxy = min(lat_vals), max(lat_vals)
    center_lon = sum(lon_vals) / len(lon_vals)
    center_lat = sum(lat_vals) / len(lat_vals)

    lat_mid = center_lat
    m_per_deg_lat = 111_000.0
    m_per_deg_lon = 111_000.0 * max(math.cos(math.radians(lat_mid)), 0.1)
    width_m = max(30.0, (maxx - minx) * m_per_deg_lon)
    height_m = max(30.0, (maxy - miny) * m_per_deg_lat)

    zoom = _compute_zoom(width_m * 1.6, height_m * 1.6, lat_mid, viewport=(width, height))
    filter_bbox = _bbox_expand((minx, miny, maxx, maxy), pad_m=1500)

    # У staticmap в вашей версии нет альфы для заливки,
    # поэтому используем очень светлый цвет + обводка и «ореол», чтобы не перекрывать детали.
    m.add_polygon(SMPolygon(ring, "#1f78b4", "#d9ecff"))  # очень светлая заливка
    m.add_line(Line(ring, "#ffffff", 5))  # белый ореол
    m.add_line(Line(ring, "#1f78b4", 2))  # синий контур

    drawn = 0
    for el in osm_data.get("elements", []):
        if drawn > 1000:
            break
        if el.get("type") == "way" and el.get("tags", {}).get("highway") and "geometry" in el:
            coords = [(p["lon"], p["lat"]) for p in el["geometry"]]
            if not any(_in_bbox(lon, lat, filter_bbox) for lon, lat in coords):
                continue
            m.add_line(Line(coords, "#666666", 1))
            drawn += 1

    try:
        image = m.render(zoom=zoom, center=(center_lon, center_lat))
    except Exception:
        image = m.render(zoom=None)

    path = os.path.join(out_dir, f"map_{uuid.uuid4().hex}.png")
    image.save(path)
    return path