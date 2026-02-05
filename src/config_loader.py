"""
Загрузка конфигурации: учётные данные из .env, остальные параметры из config.json.
"""

import json
import os
from pathlib import Path
from typing import Any, Optional, Set, Tuple

from dotenv import load_dotenv

# Значения по умолчанию для config.json
DEFAULTS = {
    "scan": {
        "concurrency": 32,
        "request_timeout_sec": 60,
        "channel_timeout_sec": 100,
    },
    "private_chats": {
        "private_timeout_sec": 600,
        "private_timeout_ids": [],
        "private_text_timeout_sec": 2000,
        "private_text_timeout_ids": [],
        "delete_private_chat_ids": [],
    },
    "photos": {
        "photos_timeout_sec": 100,
        "photos_long_timeout_sec": 300,
        "photos_timeout_ids": [],
    },
    "stories": {
        "stories_timeout_sec": 100,
        "stories_long_timeout_sec": 300,
        "stories_timeout_ids": [],
    },
    "unsubscribe": {
        "unsubscribe_ids": [],
    },
}
DEFAULT_WORK_MODE = "full"

VALID_WORK_MODES = {"full", "stats_only", "photos_only", "stories_only", "unsubscribe_only"}


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _parse_id_list(value: Any) -> Set[int]:
    """Преобразует список ID из JSON (list of int) в set[int]. Принимает также list of str."""
    if not value:
        return set()
    result: Set[int] = set()
    for item in value:
        if isinstance(item, int):
            result.add(item)
        elif isinstance(item, str):
            item = item.strip()
            if item:
                try:
                    result.add(int(item))
                except ValueError:
                    pass
    return result


def load_env_credentials() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Загружает только критические параметры из .env (API ID, API Hash, номер телефона).
    Файл .env должен быть в корне проекта и добавлен в .gitignore.
    """
    env_path = _project_root() / ".env"
    load_dotenv(env_path)
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    phone = os.getenv("TELEGRAM_PHONE")
    return api_id, api_hash, phone


def load_app_config(logger: Any = None) -> dict:
    """
    Загружает настройки работы программы из config.json.
    При отсутствии файла или полей используются значения по умолчанию.
    Возвращает плоский словарь с ключами, готовый для передачи в ChannelScanner и main.
    """
    config_path = _project_root() / "config.json"
    raw: dict = {}
    if config_path.is_file():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            if logger:
                logger.warning("Не удалось прочитать config.json: %s. Используются значения по умолчанию.", e)
            raw = {}

    def get_section(section: str, key: str, default: Any) -> Any:
        return raw.get(section, {}).get(key, DEFAULTS.get(section, {}).get(key, default))

    # Сканирование
    concurrency = get_section("scan", "concurrency", DEFAULTS["scan"]["concurrency"])
    try:
        concurrency = int(concurrency)
        if concurrency <= 0:
            concurrency = DEFAULTS["scan"]["concurrency"]
    except (TypeError, ValueError):
        concurrency = DEFAULTS["scan"]["concurrency"]

    request_timeout = get_section("scan", "request_timeout_sec", DEFAULTS["scan"]["request_timeout_sec"])
    try:
        request_timeout = int(request_timeout)
        if request_timeout <= 0:
            request_timeout = DEFAULTS["scan"]["request_timeout_sec"]
    except (TypeError, ValueError):
        request_timeout = DEFAULTS["scan"]["request_timeout_sec"]

    channel_timeout = get_section("scan", "channel_timeout_sec", DEFAULTS["scan"]["channel_timeout_sec"])
    try:
        channel_timeout = float(channel_timeout)
        if channel_timeout <= 0:
            channel_timeout = DEFAULTS["scan"]["channel_timeout_sec"]
    except (TypeError, ValueError):
        channel_timeout = DEFAULTS["scan"]["channel_timeout_sec"]

    # Личные чаты
    private_timeout = get_section("private_chats", "private_timeout_sec", DEFAULTS["private_chats"]["private_timeout_sec"])
    try:
        private_timeout = int(private_timeout)
        if private_timeout <= 0:
            private_timeout = DEFAULTS["private_chats"]["private_timeout_sec"]
    except (TypeError, ValueError):
        private_timeout = DEFAULTS["private_chats"]["private_timeout_sec"]

    private_timeout_ids = _parse_id_list(get_section("private_chats", "private_timeout_ids", []))
    private_text_timeout = get_section("private_chats", "private_text_timeout_sec", DEFAULTS["private_chats"]["private_text_timeout_sec"])
    try:
        private_text_timeout = int(private_text_timeout)
        if private_text_timeout <= 0:
            private_text_timeout = DEFAULTS["private_chats"]["private_text_timeout_sec"]
    except (TypeError, ValueError):
        private_text_timeout = DEFAULTS["private_chats"]["private_text_timeout_sec"]

    private_text_timeout_ids = _parse_id_list(get_section("private_chats", "private_text_timeout_ids", []))
    delete_private_chat_ids = _parse_id_list(get_section("private_chats", "delete_private_chat_ids", []))

    # Фотографии
    photos_timeout = get_section("photos", "photos_timeout_sec", DEFAULTS["photos"]["photos_timeout_sec"])
    try:
        photos_timeout = float(photos_timeout)
        if photos_timeout <= 0:
            photos_timeout = DEFAULTS["photos"]["photos_timeout_sec"]
    except (TypeError, ValueError):
        photos_timeout = DEFAULTS["photos"]["photos_timeout_sec"]

    photos_long_timeout = get_section("photos", "photos_long_timeout_sec", DEFAULTS["photos"]["photos_long_timeout_sec"])
    try:
        photos_long_timeout = float(photos_long_timeout)
        if photos_long_timeout <= 0:
            photos_long_timeout = DEFAULTS["photos"]["photos_long_timeout_sec"]
    except (TypeError, ValueError):
        photos_long_timeout = DEFAULTS["photos"]["photos_long_timeout_sec"]

    photos_timeout_ids = _parse_id_list(get_section("photos", "photos_timeout_ids", []))

    # Истории
    stories_timeout = get_section("stories", "stories_timeout_sec", DEFAULTS["stories"]["stories_timeout_sec"])
    try:
        stories_timeout = float(stories_timeout)
        if stories_timeout <= 0:
            stories_timeout = DEFAULTS["stories"]["stories_timeout_sec"]
    except (TypeError, ValueError):
        stories_timeout = DEFAULTS["stories"]["stories_timeout_sec"]

    stories_long_timeout = get_section("stories", "stories_long_timeout_sec", DEFAULTS["stories"]["stories_long_timeout_sec"])
    try:
        stories_long_timeout = float(stories_long_timeout)
        if stories_long_timeout <= 0:
            stories_long_timeout = DEFAULTS["stories"]["stories_long_timeout_sec"]
    except (TypeError, ValueError):
        stories_long_timeout = DEFAULTS["stories"]["stories_long_timeout_sec"]

    stories_timeout_ids = _parse_id_list(get_section("stories", "stories_timeout_ids", []))

    # Отписка
    unsubscribe_ids = _parse_id_list(get_section("unsubscribe", "unsubscribe_ids", []))

    # Режим работы (ключ в корне config.json)
    work_mode = raw.get("work_mode", DEFAULT_WORK_MODE)
    if isinstance(work_mode, str):
        work_mode = work_mode.strip().lower()
    if work_mode not in VALID_WORK_MODES:
        work_mode = DEFAULT_WORK_MODE
        if logger:
            logger.warning(
                "Некорректный work_mode в config.json. Допустимые: %s. Используется: %s",
                ", ".join(VALID_WORK_MODES),
                work_mode,
            )

    return {
        "concurrency": concurrency,
        "request_timeout": request_timeout,
        "channel_timeout": channel_timeout,
        "private_timeout": private_timeout,
        "private_timeout_ids": private_timeout_ids,
        "private_text_timeout": private_text_timeout,
        "private_text_timeout_ids": private_text_timeout_ids,
        "delete_private_chat_ids": delete_private_chat_ids,
        "photos_timeout": photos_timeout,
        "photos_long_timeout": photos_long_timeout,
        "photos_timeout_ids": photos_timeout_ids,
        "stories_timeout": stories_timeout,
        "stories_long_timeout": stories_long_timeout,
        "stories_timeout_ids": stories_timeout_ids,
        "unsubscribe_ids": unsubscribe_ids,
        "work_mode": work_mode,
    }
