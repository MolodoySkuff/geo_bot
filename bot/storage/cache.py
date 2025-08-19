import os
import json
import time
import hashlib
from typing import Any, Callable, Optional

CACHE_DIR = os.getenv("CACHE_DIR", "./cache")
TILE_CACHE_DIR = os.getenv("TILE_CACHE_DIR", "./cache/tiles")
CACHE_DISABLE = os.getenv("CACHE_DISABLE", "0").strip().lower() in ("1", "true", "on", "yes")


def ensure_dirs():
    """
    Создаём основные каталоги кэша и статиков, если их ещё нет.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(TILE_CACHE_DIR, exist_ok=True)
    os.makedirs(os.path.join(CACHE_DIR, "uploads"), exist_ok=True)
    os.makedirs(os.path.join(CACHE_DIR, "maps"), exist_ok=True)
    os.makedirs(os.path.join(CACHE_DIR, "reports"), exist_ok=True)
    os.makedirs(os.path.join(CACHE_DIR, "diag"), exist_ok=True)


def _path_for(key: str) -> str:
    """
    Возвращает путь к файлу кэша для данного ключа.
    Используем md5 от ключа, чтобы имена были безопасными и короткими.
    """
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    return os.path.join(CACHE_DIR, f"{h}.json")


def get_cache_json(key: str, ttl: Optional[int] = None) -> Optional[Any]:
    """
    Читает JSON из кэша.
    - ttl=None → игнорируем возраст (бессрочный кэш).
    - ttl=0 → кэш отключён для этого запроса (вернёт None).
    Возвращает объект или None (если нет/просрочен/поломан).
    """
    if CACHE_DISABLE or ttl == 0:
        return None

    path = _path_for(key)
    try:
        st = os.stat(path)
        if ttl is not None and ttl > 0:
            if time.time() - st.st_mtime > ttl:
                # просрочен — удалим, чтобы не копился хлам
                try:
                    os.remove(path)
                except OSError:
                    pass
                return None
        with open(path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                # битый кэш — удалим
                try:
                    os.remove(path)
                except OSError:
                    pass
                return None
    except FileNotFoundError:
        return None
    except OSError:
        # любой другой I/O сбой — как будто кэша нет
        return None


def set_cache_json(key: str, data: Any) -> None:
    """
    Сохраняет объект как JSON атомарно (tmp → os.replace), чтобы избежать битых файлов.
    Если кэш глобально отключён — просто выходим.
    """
    if CACHE_DISABLE:
        return

    path = _path_for(key)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    tmp_path = f"{path}.tmp.{os.getpid()}.{int(time.time())}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)  # атомарно
    except Exception:
        # на любой ошибке попробуем хотя бы без атомарности (как было раньше)
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception:
            pass
        # почистим tmp, если остался
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass


def get_or_set_json(key: str, ttl: Optional[int], fetcher: Callable[[], Any]) -> Any:
    """
    Удобный помощник:
      - пробует взять из кэша,
      - если нет/просрочен — вызовет fetcher(), положит в кэш и вернёт результат.
    """
    cached = get_cache_json(key, ttl=ttl)
    if cached is not None:
        return cached
    data = fetcher()
    set_cache_json(key, data)
    return data


def purge_cache(max_age_seconds: int) -> int:
    """
    Удаляет файлы кэша старше заданного возраста (в секундах).
    Возвращает число удалённых файлов. Полезно для периодической уборки.
    """
    removed = 0
    now = time.time()
    try:
        for name in os.listdir(CACHE_DIR):
            if not name.endswith(".json"):
                continue
            path = os.path.join(CACHE_DIR, name)
            try:
                st = os.stat(path)
                if now - st.st_mtime > max_age_seconds:
                    os.remove(path)
                    removed += 1
            except OSError:
                continue
    except FileNotFoundError:
        pass
    return removed