import os
import json
import asyncio
import logging
from dotenv import load_dotenv

from shapely.geometry import shape
from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, FSInputFile,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web

from . import states
from .services import geocoding, osm, dem, metrics, pdf, map_render
from .storage.cache import ensure_dirs

load_dotenv()
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
PORT = int(os.getenv("PORT", "8080"))
WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip()

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)
ensure_dirs()

# ------------- UI -------------
def main_keyboard() -> InlineKeyboardMarkup:
    rows = []
    if WEBAPP_URL.startswith("https://"):
        rows.append([
            InlineKeyboardButton(text="🗺️ Открыть карту", web_app=WebAppInfo(url=WEBAPP_URL)),
            InlineKeyboardButton(text="📄 Загрузить GeoJSON/KML", callback_data="upload_help"),
        ])
    else:
        rows.append([InlineKeyboardButton(text="📄 Загрузить GeoJSON/KML", callback_data="upload_help")])
    rows.append([
        InlineKeyboardButton(text="📍 Точка + площадь", callback_data="point_area"),
        InlineKeyboardButton(text="🔎 КН → контур", callback_data="cadnum"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def location_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Отправить мою геолокацию", request_location=True)]],
        resize_keyboard=True, one_time_keyboard=True
    )

# ------------- Handlers -------------
@router.message(CommandStart())
@router.message(Command("help"))
async def cmd_start(m: types.Message):
    hint = "" if WEBAPP_URL.startswith("https://") else "Кнопка карты скрыта — нужен HTTPS. Используйте «Точка + площадь» или загрузите GeoJSON/KML."
    await m.answer(
        "Гео‑скоринг участков: рисуйте полигон или используйте «КН → контур» в карте. "
        "Посчитаем уклон, доступность, риски подтопления, сформируем понятный PDF.\n" + hint,
        reply_markup=main_keyboard()
    )

@router.callback_query(F.data == "upload_help")
async def upload_help(c: types.CallbackQuery):
    await c.message.answer("Пришлите файл .geojson или .kml как документ. Можно несколько.")
    await c.answer()

# ---- Point + area
@router.callback_query(F.data == "point_area")
async def point_area_start(c: types.CallbackQuery, state: FSMContext):
    await state.set_state(states.PointArea.waiting_location)
    await c.message.answer(
        "Отправьте геопозицию (на телефоне появится кнопка ниже) или введите координаты: «55.75, 37.61».",
        reply_markup=location_kb()
    )
    await c.answer()

@router.message(StateFilter(states.PointArea.waiting_location), F.location)
async def point_area_loc(m: types.Message, state: FSMContext):
    await state.update_data(lat=m.location.latitude, lon=m.location.longitude)
    await state.set_state(states.PointArea.waiting_area)
    await m.answer("Введите площадь в сотках (например, 10).", reply_markup=ReplyKeyboardRemove())

@router.message(StateFilter(states.PointArea.waiting_location))
async def point_area_loc_text(m: types.Message, state: FSMContext):
    try:
        txt = m.text.replace(";", ",").replace("|", ",")
        lat, lon = [float(x.strip()) for x in txt.split(",")[:2]]
        await state.update_data(lat=lat, lon=lon)
        await state.set_state(states.PointArea.waiting_area)
        await m.answer("Введите площадь в сотках (например, 10).", reply_markup=ReplyKeyboardRemove())
    except Exception:
        await m.answer("Формат: «55.75, 37.61» или пришлите геопозицию кнопкой.", reply_markup=location_kb())

@router.message(StateFilter(states.PointArea.waiting_area))
async def point_area_area(m: types.Message, state: FSMContext):
    try:
        area_sot = float(m.text.replace(",", "."))
        data = await state.get_data()
        lat, lon = data["lat"], data["lon"]
        poly = metrics.square_from_point_area(lat, lon, area_sot)
        await state.clear()
        await run_pipeline_and_reply(m, poly, source="point+area")
    except Exception as e:
        await m.answer(f"Ошибка: {e}. Попробуйте снова или /start")
        await state.clear()

# ---- Кадастровый номер (подсказка использовать WebApp)
@router.callback_query(F.data == "cadnum")
async def cadnum_hint(c: types.CallbackQuery):
    await c.message.answer("Откройте карту (кнопка сверху) → введите КН → «КН → контур», затем «Отправить».")
    await c.answer()

# ---- Документ с GeoJSON/KML
@router.message(F.document)
async def doc_handler(m: types.Message):
    doc = m.document
    dest_dir = "cache/uploads"; os.makedirs(dest_dir, exist_ok=True)
    filename = f"{doc.file_id}_{doc.file_name or 'upload'}"
    path = os.path.join(dest_dir, filename)
    try:
        await bot.download(doc, destination=path)
        poly = metrics.read_polygon_from_file(path)
        await run_pipeline_and_reply(m, poly, source=os.path.basename(path))
    except Exception as e:
        await m.answer(f"Не удалось прочитать геометрию из файла: {e}")

# ---- WebApp: web_app_data (если прилетит)
@router.message(F.content_type == types.ContentType.WEB_APP_DATA)
async def webapp_data_ct(m: types.Message):
    try:
        payload = json.loads(m.web_app_data.data)
        if payload.get("type") == "Feature" and "geometry" in payload:
            g = shape(payload["geometry"])
            props = payload.get("properties", {}) or {}
        elif payload.get("type") in ("Polygon", "MultiPolygon"):
            g = shape(payload); props = {}
        else:
            raise ValueError("Ожидался GeoJSON Feature/Polygon")

        cad = (props.get("cad_num") or "").strip()
        src = (props.get("source") or "webapp").strip()

        packed = props.get("legal")
        legal = None
        if packed:
            legal = {
                "Вид объекта недвижимости": packed.get("t"),
                "Вид земельного участка": packed.get("s"),
                "Дата постановки на учёт": packed.get("d"),
                "Кадастровый номер": packed.get("cn"),
                "Кадастровый квартал": packed.get("q"),
                "Адрес": packed.get("a"),
                "Площадь уточненная": packed.get("pu"),
                "Площадь декларированная": packed.get("pd"),
                "Площадь по записи": packed.get("lra"),
                "Статус": packed.get("st"),
                "Категория земель": packed.get("cat"),
                "Вид разрешенного использования": packed.get("vri"),
                "Форма собственности": packed.get("own"),
                "Тип права": packed.get("rt"),
                "Кадастровая стоимость": packed.get("cost"),
                "Удельный показатель кадастровой стоимости": packed.get("cidx"),
                "Дата применения КС": packed.get("c_app"),
                "Дата регистрации КС": packed.get("c_reg"),
                "Дата определения КС": packed.get("c_det"),
                "Основание определения КС": packed.get("c_base"),
                "Категория набора": packed.get("catn"),
                "Дата обновления записи": packed.get("upd"),
            }
        src_label = f"КН {cad} • {src}" if cad else src
        await run_pipeline_and_reply(m, g, source=src_label, legal=legal)
    except Exception as e:
        logging.exception("WEB_APP_DATA error")
        from html import escape
        await m.answer(f"Ошибка WebApp данных: {escape(str(e))}", parse_mode=None)

# ------------- Диагностика NSPD -------------
@router.message(Command("nspd_test"))
async def nspd_test(m: types.Message):
    """
    Быстрый тест провайдера NSPD (бэкенд).
    Использование: /nspd_test 81:04:0510001:46
    Важно: на Replit этот путь может таймаутиться — основной путь через WebApp.
    """
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("Использование: /nspd_test <КН>")
        return
    cad = parts[1].strip()
    await m.answer("NSPD: пробую получить контур…")
    try:
        from .providers.nspd import get_geometry_and_meta_by_cadnum_nspd
        geom, meta = await asyncio.to_thread(get_geometry_and_meta_by_cadnum_nspd, cad)
        attrs = meta.get("attrs") or {}
        await m.answer(
            "OK: NSPD (точный контур)\n"
            f"Bounds: {geom.bounds}\n"
            f"Кадастровый номер: {attrs.get('Кадастровый номер')}\n"
            f"Адрес: {attrs.get('Адрес')}\n"
            f"Категория земель: {attrs.get('Категория земель')}\n"
            f"ВРИ: {attrs.get('Вид разрешенного использования')}\n"
            f"Кадастровая стоимость: {attrs.get('Кадастровая стоимость')}"
        )
    except Exception as e:
        from html import escape
        await m.answer("NSPD ошибка: " + escape(str(e)), parse_mode=None)

# ------------- Pipeline -------------
def _legal_brief(legal: dict | None) -> str:
    if not legal: return ""
    def g(k): return legal.get(k) or "—"
    return (
        "Справка из реестра:\n"
        f"- Вид объекта: {g('Вид объекта недвижимости')}\n"
        f"- ВРИ: {g('Вид разрешенного использования')}\n"
        f"- Категория земель: {g('Категория земель')}\n"
        f"- Тип права: {g('Тип права')}\n"
        f"- Площадь (декл.): {g('Площадь декларированная')}\n"
        f"- Площадь по записи: {g('Площадь по записи')}\n"
        f"- Кадастровая стоимость: {g('Кадастровая стоимость')}\n"
        f"- Даты КС: примен. {g('Дата применения КС')}, рег. {g('Дата регистрации КС')}, опред. {g('Дата определения КС')}\n"
        f"- Основание КС: {g('Основание определения КС')}\n"
        f"- Обновление записи: {g('Дата обновления записи')}"
    )

async def run_pipeline_and_reply(m: types.Message, geom_wgs84, source: str = "", legal: dict | None = None):
    from html import escape
    try:
        await m.answer("Обрабатываем участок… это займёт ~5–20 секунд.")
        centroid = geom_wgs84.centroid
        addr = await asyncio.to_thread(geocoding.reverse_geocode, centroid.y, centroid.x)

        # подставляем адрес из Nominatim, если в реестровых данных он отсутствует
        if legal is not None:
            if not legal.get("Адрес") or legal.get("Адрес") in ("—", "-"):
                if isinstance(addr, dict) and addr.get("display_name"):
                    legal["Адрес"] = addr["display_name"]

        bbox = metrics.expand_bbox(geom_wgs84.bounds, meters=2000)
        osm_data = await asyncio.to_thread(osm.fetch_overpass, bbox)
        dem_stats = await asyncio.to_thread(dem.compute_dem_stats, geom_wgs84)
        metric_set = await asyncio.to_thread(metrics.compute_all, geom_wgs84, osm_data, dem_stats)
        map_path = await asyncio.to_thread(map_render.render_static_map, geom_wgs84, osm_data, "cache/maps")
        pdf_path = await asyncio.to_thread(pdf.render_report, metric_set, addr, source, map_path, legal)

        caption = metrics.format_brief(metric_set, addr)
        await m.answer_photo(photo=FSInputFile(map_path), caption=caption)

        explain = metrics.format_explain(metric_set)
        await m.answer(explain, parse_mode=None)

        lb = _legal_brief(legal)
        if lb:
            await m.answer(lb, parse_mode=None)

        if pdf_path and os.path.exists(pdf_path):
            await m.answer_document(document=FSInputFile(pdf_path))
    except Exception as e:
        logging.exception("run_pipeline_and_reply failed")
        await m.answer("Ошибка при обработке: " + escape(str(e)), parse_mode=None)

# ------------- AIOHTTP web server (POST /api/webapp) -------------
async def process_and_send_with_legal(chat_id: int, geom_wgs84, source: str, legal: dict | None):
    from html import escape
    try:
        await bot.send_message(chat_id, "Обрабатываем участок… это займёт ~5–20 секунд.")
        centroid = geom_wgs84.centroid
        addr = await asyncio.to_thread(geocoding.reverse_geocode, centroid.y, centroid.x)
        bbox = metrics.expand_bbox(geom_wgs84.bounds, meters=2000)
        osm_data = await asyncio.to_thread(osm.fetch_overpass, bbox)
        dem_stats = await asyncio.to_thread(dem.compute_dem_stats, geom_wgs84)
        metric_set = await asyncio.to_thread(metrics.compute_all, geom_wgs84, osm_data, dem_stats)
        map_path = await asyncio.to_thread(map_render.render_static_map, geom_wgs84, osm_data, "cache/maps")
        pdf_path = await asyncio.to_thread(pdf.render_report, metric_set, addr, source, map_path, legal)

        caption = metrics.format_brief(metric_set, addr)
        await bot.send_photo(chat_id, FSInputFile(map_path), caption=caption)
        await bot.send_message(chat_id, metrics.format_explain(metric_set), parse_mode=None)

        lb = _legal_brief(legal)
        if lb:
            await bot.send_message(chat_id, lb, parse_mode=None)

        if pdf_path and os.path.exists(pdf_path):
            await bot.send_document(chat_id, FSInputFile(pdf_path))
    except Exception as e:
        logging.exception("process_and_send_with_legal failed")
        await bot.send_message(chat_id, "Ошибка при обработке: " + escape(str(e)), parse_mode=None)

async def api_webapp(request: web.Request):
    try:
        body = await request.json()
        feat = body.get("feature") or body
        user_id = int(body.get("user_id") or 0)
        if not user_id:
            return web.json_response({"ok": False, "error": "no user_id"}, status=400)

        g = shape(feat["geometry"]) if feat.get("type") == "Feature" else shape(feat)
        props = feat.get("properties") or {}
        cad = props.get("cad_num") or ""
        src = props.get("source") or "webapp"
        src_label = f"КН {cad} • {src}" if cad else src

        packed = props.get("legal")
        legal = None
        if packed:
            legal = {
                "Вид объекта недвижимости": packed.get("t"),
                "Вид земельного участка": packed.get("s"),
                "Дата постановки на учёт": packed.get("d"),
                "Кадастровый номер": packed.get("cn"),
                "Кадастровый квартал": packed.get("q"),
                "Адрес": packed.get("a"),
                "Площадь уточненная": packed.get("pu"),
                "Площадь декларированная": packed.get("pd"),
                "Площадь по записи": packed.get("lra"),
                "Статус": packed.get("st"),
                "Категория земель": packed.get("cat"),
                "Вид разрешенного использования": packed.get("vri"),
                "Форма собственности": packed.get("own"),
                "Тип права": packed.get("rt"),
                "Кадастровая стоимость": packed.get("cost"),
                "Удельный показатель кадастровой стоимости": packed.get("cidx"),
                "Дата применения КС": packed.get("c_app"),
                "Дата регистрации КС": packed.get("c_reg"),
                "Дата определения КС": packed.get("c_det"),
                "Основание определения КС": packed.get("c_base"),
                "Категория набора": packed.get("catn"),
                "Дата обновления записи": packed.get("upd"),
            }

        asyncio.create_task(process_and_send_with_legal(user_id, g, src_label, legal))
        return web.json_response({"ok": True})
    except Exception as e:
        logging.exception("api_webapp error")
        return web.json_response({"ok": False, "error": str(e)}, status=500)

async def start_web():
    app = web.Application()
    async def index(request): return web.FileResponse(path="webapp/index.html")
    app.router.add_get("/", index)
    app.router.add_static("/", path="webapp", show_index=False)
    app.router.add_get("/health", lambda request: web.Response(text="ok"))
    app.router.add_post("/api/webapp", api_webapp)
    runner = web.AppRunner(app); await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT); await site.start()
    logging.info(f"Web server started on port {PORT}; WebApp: {WEBAPP_URL or '(set WEBAPP_URL)'}")

@router.message(Command("debug"))
async def debug(m: types.Message):
    wa = getattr(m, "web_app_data", None)
    await m.answer(f"WEBAPP_URL={WEBAPP_URL}\nhas web_app_data? {'yes' if wa else 'no'}")

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не указан")
    await start_web()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())