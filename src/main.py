"""
Основной модуль программы для сканирования каналов Telegram.

Программа сканирует все каналы, группы и супергруппы, в которых
пользователь является участником, и сохраняет информацию о них.

Учётные данные (API ID, API Hash, телефон) загружаются из .env.
Остальные настройки — из config.json.
"""

import asyncio
import sys
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from logger_config import setup_logger
from channel_scanner import ChannelScanner
from config_loader import load_env_credentials, load_app_config


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
        # Учётные данные только из .env (файл в .gitignore)
        logger.info("Загрузка учётных данных из .env")
        api_id, api_hash, phone = load_env_credentials()
        if not api_id or not api_hash or not phone:
            logger.error("Ошибка: не все параметры заданы в .env")
            logger.error("Необходимо указать: TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE")
            sys.exit(1)
        try:
            api_id_int = int(api_id)
        except ValueError:
            logger.error("Ошибка: TELEGRAM_API_ID должен быть числом")
            sys.exit(1)

        # Настройки работы из config.json
        logger.info("Загрузка настроек из config.json")
        cfg = load_app_config(logger)
        work_mode = cfg["work_mode"]
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
        
        # Создаем сканер каналов (параметры из config.json)
        logger.info("Инициализация сканера каналов")
        concurrency = cfg["concurrency"]
        request_timeout = cfg["request_timeout"]
        channel_timeout = cfg["channel_timeout"]
        private_timeout = cfg["private_timeout"]
        private_timeout_ids = cfg["private_timeout_ids"]
        private_text_timeout = cfg["private_text_timeout"]
        private_text_timeout_ids = cfg["private_text_timeout_ids"]
        delete_private_chat_ids = cfg["delete_private_chat_ids"]
        photos_timeout = cfg["photos_timeout"]
        photos_timeout_ids = cfg["photos_timeout_ids"]
        photos_long_timeout = cfg["photos_long_timeout"]
        stories_timeout = cfg["stories_timeout"]
        stories_timeout_ids = cfg["stories_timeout_ids"]
        stories_long_timeout = cfg["stories_long_timeout"]
        unsubscribe_ids = cfg["unsubscribe_ids"]
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
            # Только очистка: отписка от каналов из списка config.json → unsubscribe.unsubscribe_ids
            if not unsubscribe_ids:
                logger.warning("Режим 'unsubscribe_only' выбран, но список unsubscribe_ids в config.json пуст")
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
