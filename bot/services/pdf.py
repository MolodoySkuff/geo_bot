import os
import datetime
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

FONT_DIR = os.getenv("FONT_DIR", "assets/fonts")
FONT_REG = os.path.join(FONT_DIR, "DejaVuSans.ttf")
FONT_BLD = os.path.join(FONT_DIR, "DejaVuSans-Bold.ttf")

if os.path.exists(FONT_REG) and os.path.exists(FONT_BLD):
    pdfmetrics.registerFont(TTFont("DejaVu", FONT_REG))
    pdfmetrics.registerFont(TTFont("DejaVu-Bold", FONT_BLD))
    F_MAIN, F_BOLD = "DejaVu", "DejaVu-Bold"
else:
    F_MAIN, F_BOLD = "Helvetica", "Helvetica-Bold"

def wrap_lines(c, text, width, font= "DejaVu", size=10):
    from reportlab.pdfbase.pdfmetrics import stringWidth
    if not text: return []
    words = str(text).split()
    lines, line = [], ""
    for w in words:
        t = (line + " " + w).strip()
        if stringWidth(t, font, size) <= width:
            line = t
        else:
            if line: lines.append(line)
            line = w
    if line: lines.append(line)
    return lines

def draw_paragraph(c, x, y, text, width, line_h, font= "DejaVu", size=10, margin=2*cm):
    height = A4[1]
    lines = wrap_lines(c, text, width, font, size)
    for ln in lines:
        if y < margin:
            c.showPage()
            y = height - margin
            c.setFont(F_MAIN, size)
        c.setFont(font, size); c.drawString(x, y, ln); y -= line_h
    return y

def draw_list(c, x, y, items, width, line_h, bullet="• ", margin=2*cm, size=10):
    for it in items:
        y = draw_paragraph(c, x, y, bullet + it, width, line_h, F_MAIN, size, margin)
    return y

def _d(v):
    if v is None: return "—"
    v = float(v)
    if v <= 1: return "на участке"
    return f"{int(v)} м" if v < 950 else f"{v/1000:.1f} км"

def _score_label(v: int) -> str:
    return "отлично" if v >= 80 else ("хорошо" if v >= 60 else ("удовлетворительно" if v >= 40 else "слабо"))

def render_report(metric_set, addr, source="", map_path="", legal: dict | None = None):
    from . import metrics as mutils
    risks, checks = mutils.build_risks(metric_set)

    out_dir = "cache/reports"; os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")

    c = canvas.Canvas(out_path, pagesize=A4)
    width, height = A4
    margin = 2*cm
    x0, y = margin, height - margin
    col_w = width - 2*margin

    c.setFont(F_BOLD, 16); c.drawString(x0, y, "Отчёт по участку"); y -= 0.9*cm
    c.setFont(F_MAIN, 10); c.drawString(x0, y, f"Дата: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"); y -= 0.6*cm
    if source:
        c.drawString(x0, y, f"Источник геометрии: {source}"); y -= 0.6*cm

    c.setFont(F_BOLD, 12); c.drawString(x0, y, "Адрес/локация"); y -= 0.6*cm
    y = draw_paragraph(c, x0, y, addr.get("display_name", "—") if isinstance(addr, dict) else str(addr),
                       col_w, 0.48*cm, F_MAIN, 10, margin)

    if map_path and os.path.exists(map_path):
        img_w, img_h = col_w, 9*cm
        c.drawImage(map_path, x0, y - img_h, img_w, img_h, preserveAspectRatio=True, anchor='sw')
        y -= (img_h + 0.6*cm)

    s = metric_set.get("score", {})
    flood_pct = metric_set.get("risk", {}).get("flood_pct")
    flood_level = "—" if flood_pct is None else ("низкий" if flood_pct < 35 else "средний" if flood_pct < 65 else "высокий")
    summary = [
        f"Общая оценка: {int(s.get('total',0))}/100 — {_score_label(int(s.get('total',0)))}. Шкала 0–100: выше — лучше.",
        f"Доступ: {int(s.get('access',0))}/100. До дороги: {_d(metric_set.get('d_road_m'))}. "
        + ("Примыкание есть." if metric_set.get('touches_road') else "Примыкания нет."),
        f"Уклон: {int(s.get('slope',0))}/100. Индикативный уклон ~{metric_set.get('dem',{}).get('slope_indicative_pct',0):.1f}%.",
        f"Вода: {int(s.get('flood',0))}/100. Ближайшая вода: {_d(metric_set.get('d_water_m'))}. "
        f"Риск подтопления: {flood_pct}% ({flood_level}).",
        f"Инфраструктура: {int(s.get('infra',0))}/100. Остановка: {_d(metric_set.get('d_stop_m'))}, "
        f"населённый пункт: {_d(metric_set.get('d_place_m'))}.",
        f"Площадь по контуру: {metric_set.get('area_ha',0):.2f} га. Фасад вдоль дороги: {int(metric_set.get('facade_len_m',0))} м."
    ]
    c.setFont(F_BOLD, 12); c.drawString(x0, y, "Короткое резюме"); y -= 0.6*cm
    y = draw_list(c, x0, y, summary, col_w, 0.5*cm, "• ", margin, 10)

    # Что рядом и рельеф
    y -= 0.2*cm
    c.setFont(F_BOLD, 12); c.drawString(x0, y, "Что рядом (по прямой)"); y -= 0.6*cm
    for k, v in [
        ("До автодороги", _d(metric_set.get("d_road_m"))),
        ("До воды (река/ручей/водоём)", _d(metric_set.get("d_water_m"))),
        ("До остановки", _d(metric_set.get("d_stop_m"))),
        ("До ближайшего населённого пункта", _d(metric_set.get("d_place_m"))),
        ("До ЛЭП/подстанции", _d(metric_set.get("d_power_m"))),
    ]:
        y = draw_paragraph(c, x0, y, f"{k}: {v}", col_w, 0.48*cm, F_MAIN, 10, margin)

    y -= 0.2*cm
    c.setFont(F_BOLD, 12); c.drawString(x0, y, "Рельеф"); y -= 0.6*cm
    dem = metric_set.get("dem", {}) or {}
    y = draw_list(c, x0, y, [
        f"Мин/мед/макс высота: {dem.get('elev_min',0):.0f} / {dem.get('elev_med',0):.0f} / {dem.get('elev_max',0):.0f} м.",
        f"Уклон (индикативный): {dem.get('slope_indicative_pct',0):.1f}%.",
        f"Относительная высота участка: {dem.get('rel_lowness_m',0):+.1f} м.",
    ], col_w, 0.5*cm, "• ", margin, 10)

    # Стр.2: риски/проверить/реестр
    c.showPage()
    x0, y = margin, A4[1] - margin
    c.setFont(F_BOLD, 14); c.drawString(x0, y, "Риски и что проверить"); y -= 0.8*cm

    c.setFont(F_BOLD, 12); c.drawString(x0, y, "Риски"); y -= 0.6*cm
    y = draw_list(c, x0, y, risks if risks else ["Существенных рисков не выявлено по доступным данным."], col_w, 0.5*cm, "⚠️ ", margin, 11)

    y -= 0.4*cm
    c.setFont(F_BOLD, 12); c.drawString(x0, y, "Что проверить"); y -= 0.6*cm
    base_checks = [
        "Правовой доступ: статус дороги, сервитут/проезд.",
        "ЗОУИТ/ООПТ — по региональным ГИС.",
        "Оффлайн‑проверка рельефа: лужи/следы подтоплений.",
        "Подключение к сетям: техусловия, сроки, стоимость.",
        "Границы участка в натуре: вынос межевых знаков."
    ]
    full_checks = list(dict.fromkeys((metric_set.get("checks_list") or []) + base_checks))
    y = draw_list(c, x0, y, full_checks, col_w, 0.5*cm, "• ", margin, 11)

    if legal:
        y -= 0.4*cm
        c.setFont(F_BOLD, 12); c.drawString(x0, y, "Сведения из реестра"); y -= 0.6*cm
        for key in [
            "Вид объекта недвижимости", "Вид земельного участка", "Дата постановки на учёт",
            "Кадастровый номер", "Кадастровый квартал", "Адрес",
            "Площадь уточненная", "Площадь декларированная", "Площадь по записи",
            "Статус", "Категория земель", "Вид разрешенного использования",
            "Форма собственности", "Тип права",
            "Кадастровая стоимость", "Удельный показатель кадастровой стоимости",
            "Дата применения КС", "Дата регистрации КС", "Дата определения КС", "Основание определения КС",
            "Категория набора", "Дата обновления записи"
        ]:
            val = legal.get(key)
            if val:
                y = draw_paragraph(c, x0, y, f"{key}: {val}", col_w, 0.48*cm, F_MAIN, 10, margin)

    y -= 0.4*cm
    c.setFont(F_BOLD, 12); c.drawString(x0, y, "Примечания и источники"); y -= 0.6*cm
    y = draw_list(c, x0, y, [
        "Расстояния — по прямой. Время в пути не рассчитывается.",
        "OSM может быть неполным. Оценка подтопления — индикативная.",
        "Источники: OSM (ODbL), Nominatim, DEM: OpenTopoData/Open‑Elevation, реестр: NSPD."
    ], col_w, 0.5*cm, "• ", margin, 10)

    c.showPage(); c.save()
    return out_path