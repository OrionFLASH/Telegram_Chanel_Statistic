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
        scanner = ChannelScanner(client)
        
        # Выполняем сканирование
        logger.info("Начало процесса сканирования")
        channels_data = await scanner.scan_all_channels()
        
        # Сохраняем результаты
        logger.info("Сохранение результатов сканирования")
        scanner.save_to_json("channels_data.json")
        scanner.save_to_text("channels_list.txt")
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
        logger.info("  - channels_data.json (JSON формат)")
        logger.info("  - channels_list.txt (Текстовый формат)")
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
