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
        unsubscribe_ids = load_unsubscribe_ids()
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
        )
        
        # Выполняем сканирование
        logger.info("Начало процесса сканирования")
        channels_data = await scanner.scan_all_channels()
        logger.info("Сканирование каналов завершено, старт сканирования личных чатов")
        private_chats_data = await scanner.scan_private_chats()
        logger.info(f"Сканирование личных чатов завершено: {len(private_chats_data)}")
        
        # Сохраняем результаты
        logger.info("Сохранение результатов сканирования")
        scanner.save_to_xlsx("channels_data.xlsx")
        
        # Выводим статистику
        logger.info("=" * 80)
        logger.info("СКАНИРОВАНИЕ ЗАВЕРШЕНО")
        logger.info("=" * 80)
        logger.info(f"Всего найдено каналов и групп: {len(channels_data)}")
        
        # Подсчитываем статистику
        channels_count = sum(1 for ch in channels_data if ch['is_broadcast'])
        groups_count = sum(1 for ch in channels_data if not ch['is_broadcast'])
        public_count = sum(1 for ch in channels_data if ch['is_public'])
        private_count = len(channels_data) - public_count
        
        logger.info(f"  - Каналов: {channels_count}")
        logger.info(f"  - Групп: {groups_count}")
        logger.info(f"  - Публичных: {public_count}")
        logger.info(f"  - Приватных: {private_count}")
        logger.info("=" * 80)
        logger.info("Результаты сохранены в файлы:")
        logger.info("  - channels_data.xlsx (Excel формат)")
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
