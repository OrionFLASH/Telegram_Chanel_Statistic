"""
Основной модуль программы для сканирования каналов Telegram.

Программа сканирует все каналы, группы и супергруппы, в которых
пользователь является участником, и сохраняет информацию о них.
"""

import asyncio
import os
import sys
from pathlib import Path
from typing import Optional, Tuple
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from logger_config import setup_logger
from channel_scanner import ChannelScanner


def load_config() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Загружает конфигурацию из файла .env.
    
    Returns:
        Кортеж с API_ID, API_HASH и PHONE из конфигурации
    """
    # Загружаем переменные окружения из .env файла
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(env_path)
    
    api_id = os.getenv('TELEGRAM_API_ID')
    api_hash = os.getenv('TELEGRAM_API_HASH')
    phone = os.getenv('TELEGRAM_PHONE')
    
    return api_id, api_hash, phone


def load_scan_concurrency(default_value: int = 32) -> int:
    """
    Загружает параметр параллелизма сканирования из переменных окружения.
    
    Args:
        default_value: Значение по умолчанию, если переменная не задана
    
    Returns:
        Количество одновременных задач для сканирования
    """
    logger = setup_logger("main")
    concurrency_raw = os.getenv("TELEGRAM_CONCURRENCY", "").strip()
    if not concurrency_raw:
        return default_value
    try:
        concurrency_value = int(concurrency_raw)
        if concurrency_value <= 0:
            raise ValueError("Параллелизм должен быть положительным числом")
        return concurrency_value
    except ValueError as exc:
        logger.warning(
            f"Некорректное значение TELEGRAM_CONCURRENCY '{concurrency_raw}', "
            f"используется значение по умолчанию {default_value}"
        )
        logger.debug(f"Детали ошибки при разборе TELEGRAM_CONCURRENCY: {exc}")
        return default_value


def load_request_timeout(default_value: int = 60) -> int:
    """
    Загружает таймаут запросов из переменных окружения.
    
    Args:
        default_value: Значение по умолчанию в секундах
    
    Returns:
        Таймаут запросов в секундах
    """
    logger = setup_logger("main")
    timeout_raw = os.getenv("TELEGRAM_REQUEST_TIMEOUT", "").strip()
    if not timeout_raw:
        return default_value
    try:
        timeout_value = int(timeout_raw)
        if timeout_value <= 0:
            raise ValueError("Таймаут должен быть положительным числом")
        return timeout_value
    except ValueError as exc:
        logger.warning(
            f"Некорректное значение TELEGRAM_REQUEST_TIMEOUT '{timeout_raw}', "
            f"используется значение по умолчанию {default_value}"
        )
        logger.debug(f"Детали ошибки при разборе TELEGRAM_REQUEST_TIMEOUT: {exc}")
        return default_value


def load_unsubscribe_ids() -> set:
    """
    Загружает список ID каналов/групп для авто-отписки из .env.
    
    Returns:
        Набор ID каналов/групп для отписки
    """
    logger = setup_logger("main")
    raw_value = os.getenv("TELEGRAM_UNSUBSCRIBE_IDS", "").strip()
    if not raw_value:
        return set()
    ids: set = set()
    for item in raw_value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ids.add(int(item))
        except ValueError as exc:
            logger.warning(
                f"Некорректный ID в TELEGRAM_UNSUBSCRIBE_IDS: '{item}', пропуск"
            )
            logger.debug(f"Детали ошибки разбора ID: {exc}")
    return ids


def load_private_timeout_ids() -> set:
    """
    Загружает список ID личных чатов для отдельного таймаута из .env.
    
    Returns:
        Набор ID личных чатов
    """
    logger = setup_logger("main")
    raw_value = os.getenv("TELEGRAM_PRIVATE_TIMEOUT_IDS", "").strip()
    if not raw_value:
        return set()
    ids: set = set()
    for item in raw_value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ids.add(int(item))
        except ValueError as exc:
            logger.warning(
                f"Некорректный ID в TELEGRAM_PRIVATE_TIMEOUT_IDS: '{item}', пропуск"
            )
            logger.debug(f"Детали ошибки разбора ID: {exc}")
    return ids


def load_channel_timeout(default_value: float = 100.0) -> float:
    """
    Загружает таймаут обработки каналов/групп из переменных окружения.
    
    Args:
        default_value: Значение по умолчанию в секундах
    
    Returns:
        Таймаут обработки каналов в секундах
    """
    logger = setup_logger("main")
    timeout_raw = os.getenv("TELEGRAM_CHANNEL_TIMEOUT", "").strip()
    if not timeout_raw:
        return default_value
    try:
        timeout_value = float(timeout_raw)
        if timeout_value <= 0:
            raise ValueError("Таймаут должен быть положительным числом")
        return timeout_value
    except ValueError as exc:
        logger.warning(
            f"Некорректное значение TELEGRAM_CHANNEL_TIMEOUT '{timeout_raw}', "
            f"используется значение по умолчанию {default_value}"
        )
        logger.debug(f"Детали ошибки при разборе TELEGRAM_CHANNEL_TIMEOUT: {exc}")
        return default_value


def load_private_timeout_value(default_value: int = 600) -> int:
    """
    Загружает отдельный таймаут для личных чатов из переменных окружения.
    
    Args:
        default_value: Значение по умолчанию в секундах
    
    Returns:
        Таймаут запросов для личных чатов в секундах
    """
    logger = setup_logger("main")
    timeout_raw = os.getenv("TELEGRAM_PRIVATE_TIMEOUT", "").strip()
    if not timeout_raw:
        return default_value
    try:
        timeout_value = int(timeout_raw)
        if timeout_value <= 0:
            raise ValueError("Таймаут должен быть положительным числом")
        return timeout_value
    except ValueError as exc:
        logger.warning(
            f"Некорректное значение TELEGRAM_PRIVATE_TIMEOUT '{timeout_raw}', "
            f"используется значение по умолчанию {default_value}"
        )
        logger.debug(f"Детали ошибки при разборе TELEGRAM_PRIVATE_TIMEOUT: {exc}")
        return default_value


def load_private_text_timeout_ids() -> set:
    """
    Загружает список ID личных чатов для расширенной статистики текста из .env.
    
    Returns:
        Набор ID личных чатов
    """
    logger = setup_logger("main")
    raw_value = os.getenv("TELEGRAM_PRIVATE_TEXT_TIMEOUT_IDS", "").strip()
    if not raw_value:
        return set()
    ids: set = set()
    for item in raw_value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ids.add(int(item))
        except ValueError as exc:
            logger.warning(
                f"Некорректный ID в TELEGRAM_PRIVATE_TEXT_TIMEOUT_IDS: '{item}', пропуск"
            )
            logger.debug(f"Детали ошибки разбора ID: {exc}")
    return ids


def load_delete_private_chat_ids() -> set:
    """
    Загружает список ID личных чатов для удаления из .env.
    
    Returns:
        Набор ID личных чатов для удаления
    """
    logger = setup_logger("main")
    raw_value = os.getenv("TELEGRAM_DELETE_PRIVATE_CHAT_IDS", "").strip()
    if not raw_value:
        return set()
    ids: set = set()
    for item in raw_value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ids.add(int(item))
        except ValueError as exc:
            logger.warning(
                f"Некорректный ID в TELEGRAM_DELETE_PRIVATE_CHAT_IDS: '{item}', пропуск"
            )
            logger.debug(f"Детали ошибки разбора ID: {exc}")
    return ids


def load_private_text_timeout_value(default_value: int = 2000) -> int:
    """
    Загружает отдельный таймаут для расширенной статистики текста.
    
    Args:
        default_value: Значение по умолчанию в секундах
    
    Returns:
        Таймаут запросов для личных чатов в секундах
    """
    logger = setup_logger("main")
    timeout_raw = os.getenv("TELEGRAM_PRIVATE_TEXT_TIMEOUT", "").strip()
    if not timeout_raw:
        return default_value
    try:
        timeout_value = int(timeout_raw)
        if timeout_value <= 0:
            raise ValueError("Таймаут должен быть положительным числом")
        return timeout_value
    except ValueError as exc:
        logger.warning(
            f"Некорректное значение TELEGRAM_PRIVATE_TEXT_TIMEOUT '{timeout_raw}', "
            f"используется значение по умолчанию {default_value}"
        )
        logger.debug(f"Детали ошибки при разборе TELEGRAM_PRIVATE_TEXT_TIMEOUT: {exc}")
        return default_value


def load_photos_timeout_value(default_value: float = 100.0) -> float:
    """
    Загружает таймаут для скачивания фотографий профиля.
    
    Args:
        default_value: Значение по умолчанию в секундах
    
    Returns:
        Таймаут для скачивания фотографий в секундах
    """
    logger = setup_logger("main")
    timeout_raw = os.getenv("TELEGRAM_PHOTOS_TIMEOUT", "").strip()
    if not timeout_raw:
        return default_value
    try:
        timeout_value = float(timeout_raw)
        if timeout_value <= 0:
            raise ValueError("Таймаут должен быть положительным числом")
        return timeout_value
    except ValueError as exc:
        logger.warning(
            f"Некорректное значение TELEGRAM_PHOTOS_TIMEOUT '{timeout_raw}', "
            f"используется значение по умолчанию {default_value}"
        )
        logger.debug(f"Детали ошибки при разборе TELEGRAM_PHOTOS_TIMEOUT: {exc}")
        return default_value


def load_photos_timeout_ids() -> set:
    """
    Загружает список ID личных чатов для отдельного таймаута при скачивании фотографий из .env.
    
    Returns:
        Набор ID личных чатов
    """
    logger = setup_logger("main")
    raw_value = os.getenv("TELEGRAM_PHOTOS_TIMEOUT_IDS", "").strip()
    if not raw_value:
        return set()
    ids: set = set()
    for item in raw_value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ids.add(int(item))
        except ValueError as exc:
            logger.warning(
                f"Некорректный ID в TELEGRAM_PHOTOS_TIMEOUT_IDS: '{item}', пропуск"
            )
            logger.debug(f"Детали ошибки разбора ID: {exc}")
    return ids


def load_photos_long_timeout_value(default_value: float = 300.0) -> float:
    """
    Загружает большой таймаут для скачивания фотографий профиля (для пользователей с большим количеством фото).
    
    Args:
        default_value: Значение по умолчанию в секундах
    
    Returns:
        Большой таймаут для скачивания фотографий в секундах
    """
    logger = setup_logger("main")
    timeout_raw = os.getenv("TELEGRAM_PHOTOS_LONG_TIMEOUT", "").strip()
    if not timeout_raw:
        return default_value
    try:
        timeout_value = float(timeout_raw)
        if timeout_value <= 0:
            raise ValueError("Таймаут должен быть положительным числом")
        return timeout_value
    except ValueError as exc:
        logger.warning(
            f"Некорректное значение TELEGRAM_PHOTOS_LONG_TIMEOUT '{timeout_raw}', "
            f"используется значение по умолчанию {default_value}"
        )
        logger.debug(f"Детали ошибки при разборе TELEGRAM_PHOTOS_LONG_TIMEOUT: {exc}")
        return default_value


def load_stories_timeout_value(default_value: float = 100.0) -> float:
    """
    Загружает таймаут для скачивания историй (stories).
    
    Args:
        default_value: Значение по умолчанию в секундах
    
    Returns:
        Таймаут для скачивания историй в секундах
    """
    logger = setup_logger("main")
    timeout_raw = os.getenv("TELEGRAM_STORIES_TIMEOUT", "").strip()
    if not timeout_raw:
        return default_value
    try:
        timeout_value = float(timeout_raw)
        if timeout_value <= 0:
            raise ValueError("Таймаут должен быть положительным числом")
        return timeout_value
    except ValueError as exc:
        logger.warning(
            f"Некорректное значение TELEGRAM_STORIES_TIMEOUT '{timeout_raw}', "
            f"используется значение по умолчанию {default_value}"
        )
        logger.debug(f"Детали ошибки при разборе TELEGRAM_STORIES_TIMEOUT: {exc}")
        return default_value


def load_stories_timeout_ids() -> set:
    """
    Загружает список ID личных чатов для отдельного таймаута при скачивании историй из .env.
    
    Returns:
        Набор ID личных чатов
    """
    logger = setup_logger("main")
    raw_value = os.getenv("TELEGRAM_STORIES_TIMEOUT_IDS", "").strip()
    if not raw_value:
        return set()
    ids: set = set()
    for item in raw_value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ids.add(int(item))
        except ValueError as exc:
            logger.warning(
                f"Некорректный ID в TELEGRAM_STORIES_TIMEOUT_IDS: '{item}', пропуск"
            )
            logger.debug(f"Детали ошибки разбора ID: {exc}")
    return ids


def load_stories_long_timeout_value(default_value: float = 300.0) -> float:
    """
    Загружает большой таймаут для скачивания историй (для пользователей с большим количеством историй).
    
    Args:
        default_value: Значение по умолчанию в секундах
    
    Returns:
        Большой таймаут для скачивания историй в секундах
    """
    logger = setup_logger("main")
    timeout_raw = os.getenv("TELEGRAM_STORIES_LONG_TIMEOUT", "").strip()
    if not timeout_raw:
        return default_value
    try:
        timeout_value = float(timeout_raw)
        if timeout_value <= 0:
            raise ValueError("Таймаут должен быть положительным числом")
        return timeout_value
    except ValueError as exc:
        logger.warning(
            f"Некорректное значение TELEGRAM_STORIES_LONG_TIMEOUT '{timeout_raw}', "
            f"используется значение по умолчанию {default_value}"
        )
        logger.debug(f"Детали ошибки при разборе TELEGRAM_STORIES_LONG_TIMEOUT: {exc}")
        return default_value


def load_work_mode(default_value: str = "full") -> str:
    """
    Загружает режим работы программы из переменных окружения.
    
    Возможные значения:
    - "full" - полная обработка (сканирование каналов и чатов, сохранение в Excel, скачивание фото и историй)
    - "stats_only" - только статистика (сканирование каналов и чатов, сохранение в Excel, без скачивания медиа)
    - "photos_only" - только фотографии (минимальное сканирование личных чатов для получения списка пользователей, скачивание фотографий профиля)
    - "stories_only" - только истории (минимальное сканирование личных чатов для получения списка пользователей, скачивание историй)
    - "unsubscribe_only" - только очистка (отписка от каналов из списка TELEGRAM_UNSUBSCRIBE_IDS, без сканирования и сохранения)
    
    Args:
        default_value: Значение по умолчанию
    
    Returns:
        Режим работы программы
    """
    logger = setup_logger("main")
    mode_raw = os.getenv("TELEGRAM_WORK_MODE", "").strip().lower()
    if not mode_raw:
        return default_value
    
    valid_modes = ["full", "stats_only", "photos_only", "stories_only", "unsubscribe_only"]
    if mode_raw not in valid_modes:
        logger.warning(
            f"Некорректное значение TELEGRAM_WORK_MODE '{mode_raw}', "
            f"допустимые значения: {', '.join(valid_modes)}, "
            f"используется значение по умолчанию '{default_value}'"
        )
        return default_value
    
    return mode_raw


async def authenticate_client(client: TelegramClient, phone: str) -> None:
    """
    Выполняет аутентификацию клиента Telegram.
    
    Args:
        client: Экземпляр TelegramClient
        phone: Номер телефона для входа
    """
    logger = setup_logger("main")
    
    await client.connect()
    
    if not await client.is_user_authorized():
        logger.info("Клиент не авторизован. Начинаем процесс авторизации")
        await client.send_code_request(phone)
        
        code = input('Введите код, который пришел в Telegram: ')
        
        try:
            await client.sign_in(phone, code)
            logger.info("Успешная авторизация по коду")
        except SessionPasswordNeededError:
            logger.info("Требуется пароль двухфакторной аутентификации")
            password = input('Введите пароль двухфакторной аутентификации: ')
            await client.sign_in(password=password)
            logger.info("Успешная авторизация с паролем")
    else:
        logger.info("Клиент уже авторизован")


async def main() -> None:
    """
    Основная функция программы.
    
    Выполняет сканирование всех каналов и сохранение результатов.
    """
    # Настраиваем логирование
    logger = setup_logger("telegram_scanner")
    logger.info("=" * 80)
    logger.info("Запуск программы сканирования каналов Telegram")
    logger.info("=" * 80)
    
    try:
        # Загружаем конфигурацию
        logger.info("Загрузка конфигурации из .env файла")
        api_id, api_hash, phone = load_config()
        
        # Проверяем наличие всех необходимых параметров
        if not api_id or not api_hash or not phone:
            logger.error("Ошибка: не все параметры конфигурации заданы в .env файле")
            logger.error("Необходимо указать: TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE")
            sys.exit(1)
        
        # Преобразуем API_ID в число
        try:
            api_id_int = int(api_id)
        except ValueError:
            logger.error("Ошибка: TELEGRAM_API_ID должен быть числом")
            sys.exit(1)
        
        # Загружаем режим работы
        work_mode = load_work_mode()
        logger.info(f"Режим работы: {work_mode}")
        if work_mode == "full":
            logger.info("  → Полная обработка: сканирование, статистика, фото и истории")
        elif work_mode == "stats_only":
            logger.info("  → Только статистика: сканирование и сохранение в Excel, без медиа")
        elif work_mode == "photos_only":
            logger.info("  → Только фотографии: минимальное сканирование личных чатов, скачивание фото профиля")
        elif work_mode == "stories_only":
            logger.info("  → Только истории: минимальное сканирование личных чатов, скачивание историй")
        elif work_mode == "unsubscribe_only":
            logger.info("  → Только очистка: отписка от каналов из списка TELEGRAM_UNSUBSCRIBE_IDS")
        
        logger.info("Конфигурация успешно загружена")
        logger.debug(f"API ID: {api_id_int}, Phone: {phone}")
        
        # Создаем клиент Telegram
        session_name = 'telegram_session'
        logger.info(f"Создание клиента Telegram (сессия: {session_name})")
        client = TelegramClient(session_name, api_id_int, api_hash)
        
        # Выполняем аутентификацию
        await authenticate_client(client, phone)
        
        # Создаем сканер каналов
        logger.info("Инициализация сканера каналов")
        concurrency = load_scan_concurrency()
        request_timeout = load_request_timeout()
        channel_timeout = load_channel_timeout()
        private_timeout = load_private_timeout_value()
        private_timeout_ids = load_private_timeout_ids()
        private_text_timeout = load_private_text_timeout_value()
        private_text_timeout_ids = load_private_text_timeout_ids()
        delete_private_chat_ids = load_delete_private_chat_ids()
        photos_timeout = load_photos_timeout_value()
        photos_timeout_ids = load_photos_timeout_ids()
        photos_long_timeout = load_photos_long_timeout_value()
        stories_timeout = load_stories_timeout_value()
        stories_timeout_ids = load_stories_timeout_ids()
        stories_long_timeout = load_stories_long_timeout_value()
        unsubscribe_ids = load_unsubscribe_ids()
        logger.info(f"Параллелизм сканирования: {concurrency}")
        logger.info(f"Таймаут запроса: {request_timeout} сек")
        logger.info(f"Таймаут обработки каналов: {channel_timeout} сек")
        if private_timeout_ids:
            logger.info(
                f"Отдельный таймаут для личных чатов: {private_timeout} сек "
                f"(ID: {len(private_timeout_ids)})"
            )
        if delete_private_chat_ids:
            logger.info(
                f"Активна авто-удаление личных чатов, ID в списке: {len(delete_private_chat_ids)}"
            )
        if private_text_timeout_ids:
            logger.info(
                f"Таймаут для расширенной статистики текста: {private_text_timeout} сек "
                f"(ID: {len(private_text_timeout_ids)})"
            )
        logger.info(f"Таймаут для скачивания фотографий профиля: {photos_timeout} сек")
        if photos_timeout_ids:
            logger.info(
                f"Большой таймаут для скачивания фотографий: {photos_long_timeout} сек "
                f"(ID: {len(photos_timeout_ids)})"
            )
        logger.info(f"Таймаут для скачивания историй: {stories_timeout} сек")
        if stories_timeout_ids:
            logger.info(
                f"Большой таймаут для скачивания историй: {stories_long_timeout} сек "
                f"(ID: {len(stories_timeout_ids)})"
            )
        if unsubscribe_ids:
            logger.info(f"Активна авто-отписка, ID в списке: {len(unsubscribe_ids)}")
        scanner = ChannelScanner(
            client,
            concurrency=concurrency,
            unsubscribe_ids=unsubscribe_ids,
            request_timeout=float(request_timeout),
            channel_timeout=float(channel_timeout),
            private_timeout=float(private_timeout),
            private_timeout_ids=private_timeout_ids,
            private_text_timeout=float(private_text_timeout),
            private_text_timeout_ids=private_text_timeout_ids,
            delete_private_chat_ids=delete_private_chat_ids,
            photos_timeout=float(photos_timeout),
            photos_timeout_ids=photos_timeout_ids,
            photos_long_timeout=float(photos_long_timeout),
            stories_timeout=float(stories_timeout),
            stories_timeout_ids=stories_timeout_ids,
            stories_long_timeout=float(stories_long_timeout),
        )
        
        # Инициализируем переменные для результатов
        channels_data = []
        private_chats_data = []
        photos_stats = {}
        stories_stats = {}
        output_file = None
        output_filename = None
        
        # Выполняем действия в зависимости от режима работы
        if work_mode == "full":
            # Полная обработка: сканирование каналов и чатов
            logger.info("Начало процесса сканирования")
            channels_data = await scanner.scan_all_channels()
            logger.info("Сканирование каналов завершено, старт сканирования личных чатов")
            private_chats_data = await scanner.scan_private_chats()
            logger.info(f"Сканирование личных чатов завершено: {len(private_chats_data)}")
            
            # Скачиваем фотографии профиля
            logger.info("Начало скачивания фотографий профиля")
            photos_stats = await scanner.download_profile_photos()
            logger.info(
                f"Скачивание фотографий завершено: найдено {photos_stats.get('total_photos', 0)}, "
                f"скачано {photos_stats.get('downloaded_photos', 0)}, ошибок {photos_stats.get('failed_photos', 0)}"
            )
            
            # Скачиваем истории
            logger.info("Начало скачивания историй")
            stories_stats = await scanner.download_stories()
            logger.info(
                f"Скачивание историй завершено: найдено {stories_stats.get('total_stories', 0)}, "
                f"скачано {stories_stats.get('downloaded', 0)}, ошибок {stories_stats.get('failed', 0)}"
            )
            
            # Сохраняем результаты
            logger.info("Сохранение результатов сканирования")
            output_file = scanner.save_to_xlsx("channels_data.xlsx")
            output_filename = output_file.split("/")[-1] if "/" in output_file else output_file.split("\\")[-1]
            
        elif work_mode == "stats_only":
            # Только статистика: сканирование каналов и чатов, сохранение в Excel
            logger.info("Начало процесса сканирования (режим: только статистика)")
            channels_data = await scanner.scan_all_channels()
            logger.info("Сканирование каналов завершено, старт сканирования личных чатов")
            private_chats_data = await scanner.scan_private_chats()
            logger.info(f"Сканирование личных чатов завершено: {len(private_chats_data)}")
            
            # Сохраняем результаты
            logger.info("Сохранение результатов сканирования")
            output_file = scanner.save_to_xlsx("channels_data.xlsx")
            output_filename = output_file.split("/")[-1] if "/" in output_file else output_file.split("\\")[-1]
            
        elif work_mode == "photos_only":
            # Только фотографии: минимальное сканирование личных чатов для получения списка пользователей
            logger.info("Начало процесса сканирования личных чатов (режим: только фотографии)")
            logger.info("Сканирование каналов пропущено (режим: только фотографии)")
            private_chats_data = await scanner.scan_private_chats()
            logger.info(f"Сканирование личных чатов завершено: {len(private_chats_data)}")
            
            # Скачиваем только фотографии профиля
            logger.info("Начало скачивания фотографий профиля")
            photos_stats = await scanner.download_profile_photos()
            logger.info(
                f"Скачивание фотографий завершено: найдено {photos_stats.get('total_photos', 0)}, "
                f"скачано {photos_stats.get('downloaded_photos', 0)}, ошибок {photos_stats.get('failed_photos', 0)}"
            )
        
        elif work_mode == "stories_only":
            # Только истории: получаем список пользователей напрямую из get_dialogs() без полного сканирования
            logger.info("Режим: только истории - пропускаем полное сканирование")
            logger.info("Сканирование каналов пропущено (режим: только истории)")
            logger.info("Получение списка пользователей для скачивания историй...")
            
            # Скачиваем только истории (список пользователей получается внутри download_stories)
            logger.info("Начало скачивания историй")
            stories_stats = await scanner.download_stories()
            logger.info(
                f"Скачивание историй завершено: найдено {stories_stats.get('total_stories', 0)}, "
                f"скачано {stories_stats.get('downloaded', 0)}, ошибок {stories_stats.get('failed', 0)}"
            )
        
        elif work_mode == "unsubscribe_only":
            # Только очистка: отписка от каналов из списка TELEGRAM_UNSUBSCRIBE_IDS
            if not unsubscribe_ids:
                logger.warning("Режим 'unsubscribe_only' выбран, но список TELEGRAM_UNSUBSCRIBE_IDS пуст")
                logger.info("Очистка не будет выполнена")
            else:
                logger.info(f"Начало отписки от каналов (ID в списке: {len(unsubscribe_ids)})")
                unsubscribe_stats = await scanner.unsubscribe_only_channels()
                logger.info(
                    f"Отписка завершена: обработано {unsubscribe_stats.get('total', 0)}, "
                    f"успешно отписано {unsubscribe_stats.get('unsubscribed', 0)}, "
                    f"ошибок {unsubscribe_stats.get('failed', 0)}, "
                    f"не найдено {unsubscribe_stats.get('not_found', 0)}"
                )
        
        # Выводим статистику в зависимости от режима работы
        logger.info("=" * 80)
        logger.info("ОБРАБОТКА ЗАВЕРШЕНА")
        logger.info("=" * 80)
        
        if work_mode in ["full", "stats_only"]:
            # Статистика по каналам (только для режимов с полным сканированием)
            logger.info(f"Всего найдено каналов и групп: {len(channels_data)}")
            
            if channels_data:
                # Подсчитываем статистику по каналам
                channels_count = sum(1 for ch in channels_data if ch['is_broadcast'])
                groups_count = sum(1 for ch in channels_data if not ch['is_broadcast'])
                public_count = sum(1 for ch in channels_data if ch['is_public'])
                private_count = len(channels_data) - public_count
                
                logger.info(f"  - Каналов: {channels_count}")
                logger.info(f"  - Групп: {groups_count}")
                logger.info(f"  - Публичных: {public_count}")
                logger.info(f"  - Приватных: {private_count}")
            
            # Подсчитываем статистику по личным чатам
            logger.info("=" * 80)
            logger.info(f"Всего найдено личных чатов: {len(private_chats_data)}")
            
            if private_chats_data:
                # Статистика по личным чатам
                total_messages = sum(chat.get("messages_total", 0) or 0 for chat in private_chats_data)
                total_messages_365 = sum(chat.get("messages_365", 0) or 0 for chat in private_chats_data)
                total_messages_30 = sum(chat.get("messages_30", 0) or 0 for chat in private_chats_data)
                total_messages_from_me = sum(chat.get("messages_from_me", 0) or 0 for chat in private_chats_data)
                total_messages_from_other = sum(chat.get("messages_from_other", 0) or 0 for chat in private_chats_data)
                
                bots_count = sum(1 for chat in private_chats_data if chat.get("is_bot") == "Да")
                verified_count = sum(1 for chat in private_chats_data if chat.get("is_verified") == "Да")
                premium_count = sum(1 for chat in private_chats_data if chat.get("is_premium") == "Да")
                deleted_count = sum(1 for chat in private_chats_data if chat.get("deleted_status") == "Да")
                
                logger.info(f"  - Всего сообщений: {total_messages:,}")
                logger.info(f"  - Сообщений за последние 365 дней: {total_messages_365:,}")
                logger.info(f"  - Сообщений за последние 30 дней: {total_messages_30:,}")
                logger.info(f"  - Сообщений от вас: {total_messages_from_me:,}")
                logger.info(f"  - Сообщений от собеседников: {total_messages_from_other:,}")
                logger.info(f"  - Ботов: {bots_count}")
                logger.info(f"  - Верифицированных: {verified_count}")
                logger.info(f"  - Premium: {premium_count}")
                if deleted_count > 0:
                    logger.info(f"  - Удалено чатов: {deleted_count}")
            
            if output_filename:
                logger.info("=" * 80)
                logger.info("Результаты сохранены в файлы:")
                logger.info(f"  - {output_filename} (Excel формат)")
        
        elif work_mode == "photos_only":
            # Статистика только по фотографиям
            logger.info(f"Всего обработано личных чатов: {len(private_chats_data)}")
            if photos_stats:
                logger.info("=" * 80)
                logger.info("Статистика скачивания фотографий:")
                logger.info(f"  - Всего фотографий найдено: {photos_stats.get('total_photos', 0)}")
                logger.info(f"  - Успешно скачано: {photos_stats.get('downloaded_photos', 0)}")
                logger.info(f"  - Ошибок при скачивании: {photos_stats.get('failed_photos', 0)}")
                logger.info(f"  - Пользователей с фотографиями: {photos_stats.get('users_with_photos', 0)}")
                logger.info(f"  - Пользователей без фотографий: {photos_stats.get('users_without_photos', 0)}")
        
        elif work_mode == "stories_only":
            # Статистика только по историям
            logger.info(f"Всего обработано личных чатов: {len(private_chats_data)}")
            if stories_stats:
                logger.info("=" * 80)
                logger.info("Статистика скачивания историй:")
                logger.info(f"  - Всего историй найдено: {stories_stats.get('total_stories', 0)}")
                logger.info(f"  - Успешно скачано: {stories_stats.get('downloaded', 0)}")
                logger.info(f"  - Ошибок при скачивании: {stories_stats.get('failed', 0)}")
                logger.info(f"  - Пользователей с историями: {stories_stats.get('users_with_stories', 0)}")
                logger.info(f"  - Пользователей без историй: {stories_stats.get('users_without_stories', 0)}")
        
        logger.info("=" * 80)
        
        # Закрываем клиент
        await client.disconnect()
        logger.info("Работа программы завершена успешно")
        
    except KeyboardInterrupt:
        logger.warning("Программа прервана пользователем")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # Запускаем асинхронную функцию main
    asyncio.run(main())
