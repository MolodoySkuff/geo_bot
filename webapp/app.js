const tg = window.Telegram.WebApp;
tg.expand(); tg.ready();
tg.BackButton.show(); tg.onEvent('backButtonClicked', () => { try { tg.close(); } catch(e){} });

const map = L.map('map').setView([55.75, 37.61], 10);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19 }).addTo(map);

const drawnItems = new L.FeatureGroup().addTo(map);
const drawControl = new L.Control.Draw({
  draw: { polygon: true, rectangle: true, marker: false, circle: false, circlemarker: false, polyline: false },
  edit: { featureGroup: drawnItems }
});
map.addControl(drawControl);

let feature = null;

// ----- рисование вручную -----
map.on(L.Draw.Event.CREATED, (e) => {
  drawnItems.clearLayers();
  const layer = e.layer;
  drawnItems.addLayer(layer);
  feature = layer.toGeoJSON();
  document.getElementById('send').disabled = false;
});

document.getElementById('clear').addEventListener('click', () => {
  drawnItems.clearLayers(); feature = null; document.getElementById('send').disabled = true;
});
document.getElementById('close').addEventListener('click', () => {
  try { tg.close(); } catch(e){} setTimeout(()=>{ try { tg.close(); } catch(e){} }, 150);
});

// ---------- утилиты ----------
function roundCoord(v){ return Math.round(v*1e5)/1e5; }
function thinRing(coords, step){
  if (coords.length <= 4) return coords;
  const closed = coords[0][0]===coords.at(-1)[0] && coords[0][1]===coords.at(-1)[1];
  const core = closed ? coords.slice(0, -1) : coords.slice();
  const th = core.filter((_,i)=> i%step===0);
  if (th.length < 3) return coords;
  if (th[0][0]!==th.at(-1)[0] || th[0][1]!==th.at(-1)[1]) th.push(th[0]);
  return th;
}
function compressFeature(feat, maxBytes=3600){
  let f = JSON.parse(JSON.stringify(feat));
  if (f.type==='Feature' && f.geometry?.type==='Polygon'){
    let rings = f.geometry.coordinates;
    rings = rings.map(r => r.map(([x,y]) => [roundCoord(x), roundCoord(y)]));
    f.geometry.coordinates = rings;
    const enc = new TextEncoder();
    let step = 2;
    while (enc.encode(JSON.stringify(f)).length > maxBytes && step < 12){
      rings[0] = thinRing(rings[0], step);
      f.geometry.coordinates = rings;
      step += 1;
    }
  }
  return f;
}

// компактная упаковка legal (короткие ключи → меньше байт)
function packLegal(legal){
  if (!legal) return null;
  return {
    t:   legal["Вид объекта недвижимости"],
    s:   legal["Вид земельного участка"],
    d:   legal["Дата постановки на учёт"],
    cn:  legal["Кадастровый номер"],
    q:   legal["Кадастровый квартал"],
    a:   legal["Адрес"],
    pu:  legal["Площадь уточненная"],
    pd:  legal["Площадь декларированная"],
    st:  legal["Статус"],
    cat: legal["Категория земель"],
    vri: legal["Вид разрешенного использования"],
    own: legal["Форма собственности"],
    cost:legal["Кадастровая стоимость"],
    cidx:legal["Удельный показатель кадастровой стоимости"],
    // расширенные поля
    lra:  legal["Площадь по записи"],
    rt:   legal["Тип права"],
    pp:   legal["Ранее учтённый"],
    c_app:legal["Дата применения КС"],
    c_reg:legal["Дата регистрации КС"],
    c_det:legal["Дата определения КС"],
    c_base:legal["Основание определения КС"],
    catn: legal["Категория набора"],
    upd:  legal["Дата обновления записи"]
  };
}

// ----- отправка в бот по двум каналам -----
async function sendFeatureToBot(feat){
  const enc = new TextEncoder();
  let f = compressFeature(feat, 3600);
  const bytes = enc.encode(JSON.stringify(f)).length;

  // 1) POST на бэкенд (надёжно)
  const uid = tg.initDataUnsafe?.user?.id || null;
  try{
    if (uid){
      await fetch('/api/webapp', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ feature: f, user_id: uid })
      });
    }
  }catch(_){}

  // 2) web_app_data (если влезает)
  if (bytes <= 4000){
    try { tg.sendData(JSON.stringify({ type:'Feature', properties:f.properties, geometry:f.geometry })); } catch(_){}
  } else {
    try { tg.showAlert('Геометрия большая — отправили через резервный канал. Вернитесь в чат.'); } catch(_){}
  }

  try { tg.showAlert('Участок отправлен. Вернитесь в чат — бот сформирует отчёт.'); } catch(_){}
  try { tg.close(); } catch(_){}
  setTimeout(()=>{ try { tg.close(); } catch(_){ } }, 150);
}

document.getElementById('send').addEventListener('click', () => {
  if (!feature) return;
  sendFeatureToBot(feature);
});

// ---------- КН → контур (NSPD) на фронте ----------
const cadInput = document.getElementById('cad-input');
document.getElementById('cad-fetch').addEventListener('click', async (ev) => {
  ev.preventDefault();
  const cad = (cadInput.value || '').trim();
  if (!cad){ try{ tg.showAlert('Введите кадастровый номер'); }catch(_){} return; }

  const url = 'https://nspd.gov.ru/api/geoportal/v2/search/geoportal?thematicSearchId=1&query=' + encodeURIComponent(cad);
  try{
    let resp = await fetch(url, { method:'GET', headers: { 'Accept':'application/json' } });
    if (!resp.ok){
      const prox = 'https://cors.isomorphic-git.org/' + url; // CORS‑fallback (опц.)
      resp = await fetch(prox, { method:'GET', headers:{ 'Accept':'application/json' } });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
    }
    const data = await resp.json();
    const feats = (data.data && data.data.features) || data.features || [];
    if (!feats.length) throw new Error('Объект не найден');

    let feat = feats.find(f => (f.properties?.options?.cad_num || '').trim() === cad) ||
               feats.find(f => [f.properties?.label, f.properties?.descr, f.properties?.externalKey].some(v => (v||'').trim()===cad)) ||
               feats[0];

    const gj = feat.geometry;
    if (!gj || gj.type !== 'Polygon') throw new Error('Геометрия не Polygon');

    // EPSG:3857 → EPSG:4326
    const R = 6378137.0;
    function mercToLonLat([x,y]){
      const lon = x / R * 180 / Math.PI;
      const lat = (2 * Math.atan(Math.exp(y / R)) - Math.PI/2) * 180 / Math.PI;
      return [lon, lat];
    }
    const ring = gj.coordinates[0].map(mercToLonLat);
    if (ring.length && (ring[0][0] !== ring.at(-1)[0] || ring[0][1] !== ring.at(-1)[1])) ring.push(ring[0]);

    // Нормализация “Адреса” с надёжными фоллбэками
    const p = feat.properties || {};
    const o = (p.options || {});
    const addr = (
      o.readable_address ||
      p.readable_address ||
      o.address ||
      p.address ||
      o.fullAddress ||
      p.fullAddress ||
      "-"
    );

    // Сбор “юридических” полей из NSPD
    const legal = {
      "Вид объекта недвижимости": o.land_record_type || "Земельный участок",
      "Вид земельного участка": o.land_record_subtype || "—",
      "Дата постановки на учёт": o.land_record_reg_date || "—",
      "Кадастровый номер": o.cad_num || p.label || p.descr || cad,
      "Кадастровый квартал": o.quarter_cad_number || "—",
      "Адрес": addr,
      "Площадь уточненная": o.specified_area != null ? `${o.specified_area} кв. м` : "—",
      "Площадь декларированная": o.declared_area != null ? `${o.declared_area} кв. м` : "—",
      "Статус": o.status || "—",
      "Категория земель": o.land_record_category_type || "—",
      "Вид разрешенного использования": o.permitted_use_established_by_document || "—",
      "Форма собственности": o.ownership_type || "—",
      "Кадастровая стоимость": o.cost_value != null ? `${o.cost_value} руб.` : "—",
      "Удельный показатель кадастровой стоимости": o.cost_index != null ? String(o.cost_index) : "—",
      // расширенные:
      "Площадь по записи": o.land_record_area != null ? `${o.land_record_area} кв. м` : "—",
      "Тип права": o.right_type || "—",
      "Ранее учтённый": o.previously_posted || "—",
      "Дата применения КС": o.cost_application_date || "—",
      "Дата регистрации КС": o.cost_registration_date || "—",
      "Дата определения КС": o.cost_determination_date || "—",
      "Основание определения КС": o.determination_couse || "—",
      "Категория набора": p.categoryName || "—",
      "Дата обновления записи": (p.systemInfo && p.systemInfo.updated) || "—"
    };

    const f = {
      type:'Feature',
      properties:{ source:'NSPD', cad_num: cad, legal: packLegal(legal) },
      geometry:{ type:'Polygon', coordinates:[ring] }
    };

    // визуализация
    drawnItems.clearLayers();
    const poly = L.polygon(ring.map(([lon,lat]) => [lat, lon]), { color:'#1f78b4', weight:2, fillColor:'#d9ecff', fillOpacity:0.2 });
    drawnItems.addLayer(poly);
    map.fitBounds(poly.getBounds());

    feature = f;
    document.getElementById('send').disabled = false;

  }catch(e){
    console.error(e);
    try { tg.showAlert('Не удалось получить контур по КН: ' + e.message); } catch(_){}
  }
});