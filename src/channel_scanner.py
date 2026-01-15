"""
Модуль для сканирования каналов Telegram.

Содержит класс ChannelScanner для получения информации о всех каналах,
на которые подписан пользователь или в которых он является участником.
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from time import monotonic
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import xlsxwriter
from telethon import TelegramClient
from telethon.tl import functions
from telethon.errors import ChatAdminRequiredError, FloodWaitError
from telethon.tl.types import Channel, Chat, User

from logger_config import get_logger


class ChannelScanner:
    """
    Класс для сканирования и сбора информации о каналах Telegram.
    
    Позволяет получить полный список всех каналов, групп и супергрупп,
    в которых пользователь является участником, с детальной информацией.
    """
    
    def __init__(
        self,
        client: TelegramClient,
        concurrency: int = 16,
        request_delay: float = 0.2,
        unsubscribe_ids: Optional[Set[int]] = None,
        request_timeout: float = 60.0,
        channel_timeout: float = 100.0,
        private_timeout: float = 600.0,
        private_timeout_ids: Optional[Set[int]] = None,
        private_text_timeout: float = 2000.0,
        private_text_timeout_ids: Optional[Set[int]] = None,
    ) -> None:
        """
        Инициализация сканера каналов.
        
        Args:
            client: Экземпляр TelegramClient для работы с API
            concurrency: Максимальное количество одновременных запросов
            request_delay: Небольшая задержка между запросами для снижения нагрузки
            unsubscribe_ids: Набор ID каналов/групп для авто-отписки
            request_timeout: Таймаут запроса в секундах для долгих операций
            channel_timeout: Таймаут обработки канала/группы в секундах (по умолчанию 100)
            private_timeout: Отдельный таймаут для личных чатов из списка
            private_timeout_ids: Набор ID личных чатов для отдельного таймаута
            private_text_timeout: Таймаут для расширенной статистики текста
            private_text_timeout_ids: Набор ID для расширенной статистики текста
        """
        self.client = client
        self.logger = get_logger("channel_scanner")
        self.channels_data: List[Dict[str, Any]] = []
        self.private_chats_data: List[Dict[str, Any]] = []
        self.concurrency = max(1, concurrency)
        # Уменьшаем задержку, так как Semaphore уже контролирует параллелизм
        # Задержка нужна только для предотвращения FloodWaitError при очень высокой нагрузке
        self.request_delay = max(0.0, min(request_delay, 0.05))  # Максимум 50мс вместо 200мс
        self.output_dir = Path(__file__).parent.parent / "OUT"
        self.output_dir.mkdir(exist_ok=True)
        self.unsubscribe_ids = unsubscribe_ids or set()
        self.request_timeout = max(1.0, request_timeout)
        self.channel_timeout = max(1.0, channel_timeout)
        self.private_timeout = max(1.0, private_timeout)
        self.private_timeout_ids = private_timeout_ids or set()
        self.private_text_timeout = max(1.0, private_text_timeout)
        self.private_text_timeout_ids = private_text_timeout_ids or set()

    def _build_basic_channel_info(
        self,
        entity: Channel,
        last_message_date: Optional[str],
        status: str,
    ) -> Dict[str, Any]:
        """
        Создает базовую запись о канале для случаев таймаута/ошибки.
        
        Args:
            entity: Сущность канала/группы
            last_message_date: Дата последнего сообщения
            status: Статус обработки
        
        Returns:
            Базовый словарь с данными канала
        """
        is_public = entity.username is not None
        if entity.username:
            link = f"https://t.me/{entity.username}"
        else:
            link = f"tg://resolve?domain={entity.id}"
        return {
            "id": entity.id,
            "title": entity.title or "Без названия",
            "username": entity.username or "Нет username",
            "is_broadcast": entity.broadcast,
            "is_megagroup": entity.megagroup,
            "is_gigagroup": getattr(entity, "gigagroup", False),
            "access_hash": str(entity.access_hash) if hasattr(entity, "access_hash") else None,
            "scanned_at": datetime.now().isoformat(),
            "participants_count": None,
            "about": "Нет описания",
            "created_date": None,
            "is_public": is_public,
            "link": link,
            "linked_chat_id": None,
            "linked_chat_title": None,
            "linked_chat_username": None,
            "linked_chat_link": None,
            "forum_topics": [],
            "forum_topics_count": 0,
            "last_message_date": last_message_date,
            "unsubscribed_status": "Нет",
            "processing_status": status,
        }

    def _format_participants_count(self, participants_count: Any) -> str:
        """
        Приводит количество участников к строке для логов и вывода.
        
        Args:
            participants_count: Значение количества участников
        
        Returns:
            Строковое представление количества участников
        """
        if participants_count is None:
            return "Неизвестно"
        return str(participants_count)
    
    async def _fetch_participants_count(
        self,
        entity: Channel,
        full_channel_info: Optional[Any] = None,
        full_chat_info: Optional[Any] = None,
    ) -> Optional[Any]:
        """
        Пытается получить количество участников разными способами.
        
        Args:
            entity: Объект канала/группы из Telegram API
            full_channel_info: Полная информация о канале (если уже получена)
            full_chat_info: Полная информация о чате (если уже получена)
        
        Returns:
            Количество участников или None
        """
        if hasattr(entity, "participants_count") and entity.participants_count:
            return entity.participants_count
        try:
            if full_channel_info and hasattr(full_channel_info, "full_chat"):
                if hasattr(full_channel_info.full_chat, "participants_count"):
                    return full_channel_info.full_chat.participants_count
            if full_chat_info and hasattr(full_chat_info, "full_chat"):
                if hasattr(full_chat_info.full_chat, "participants_count"):
                    return full_chat_info.full_chat.participants_count
            if isinstance(entity, Channel):
                full_info = await self.client(functions.channels.GetFullChannelRequest(channel=entity))
                if hasattr(full_info, "full_chat") and hasattr(full_info.full_chat, "participants_count"):
                    return full_info.full_chat.participants_count
            if isinstance(entity, Chat):
                full_info = await self.client(functions.messages.GetFullChatRequest(chat_id=entity.id))
                if hasattr(full_info, "full_chat") and hasattr(full_info.full_chat, "participants_count"):
                    return full_info.full_chat.participants_count
        except ChatAdminRequiredError:
            return "Требуются права администратора"
        except Exception as e:
            self.logger.debug(f"Ошибка при получении количества участников через Full* методы: {e}")
        return None

    async def _fetch_linked_channel_info(self, linked_chat_id: int) -> Dict[str, Optional[Any]]:
        """
        Получает информацию о связанном канале, если он доступен.
        
        Args:
            linked_chat_id: ID связанного канала
        
        Returns:
            Словарь с данными связанного канала
        """
        linked_data: Dict[str, Optional[Any]] = {
            "linked_chat_id": str(linked_chat_id),
            "linked_chat_title": None,
            "linked_chat_username": None,
            "linked_chat_link": None,
            "_linked_entity": None,
        }
        try:
            linked_entity = await self.client.get_entity(linked_chat_id)
            linked_data["_linked_entity"] = linked_entity
            linked_data["linked_chat_title"] = getattr(linked_entity, "title", None)
            linked_data["linked_chat_username"] = getattr(linked_entity, "username", None)
            if linked_data["linked_chat_username"]:
                linked_data["linked_chat_link"] = f"https://t.me/{linked_data['linked_chat_username']}"
        except Exception as e:
            self.logger.debug(f"Не удалось получить данные связанного канала {linked_chat_id}: {e}")
        return linked_data

    async def _fetch_forum_topics(self, entity: Channel, limit: int = 100) -> List[str]:
        """
        Получает список тем форума для супергруппы с включенными темами.
        
        Args:
            entity: Сущность супергруппы
            limit: Максимальное количество тем для выборки
        
        Returns:
            Список названий тем форума
        """
        topics: List[str] = []
        seen_titles: Set[str] = set()
        try:
            # Попытка получить темы через GetForumTopicsRequest
            offset_id = 0
            offset_topic = 0
            remaining = limit
            iteration = 0
            max_iterations = 10  # Ограничение количества итераций для предотвращения бесконечного цикла
            
            while remaining > 0 and iteration < max_iterations:
                iteration += 1
                result = await self.client(
                    functions.channels.GetForumTopicsRequest(
                        channel=entity,
                        q="",
                        offset_date=0,
                        offset_id=offset_id,
                        offset_topic=offset_topic,
                        limit=min(remaining, 100),
                    )
                )
                
                # Проверяем структуру результата
                if not hasattr(result, "topics"):
                    self.logger.debug(f"Результат GetForumTopicsRequest для {entity.id} не содержит 'topics'")
                    break
                
                result_topics = getattr(result, "topics", [])
                if not result_topics:
                    self.logger.debug(f"Нет тем в результате для {entity.id} (итерация {iteration})")
                    break
                
                self.logger.debug(f"Получено {len(result_topics)} тем для {entity.id} (итерация {iteration})")
                
                for topic in result_topics:
                    title = getattr(topic, "title", None)
                    if title and title not in seen_titles:
                        seen_titles.add(title)
                        topics.append(title)
                
                # Проверяем, есть ли еще темы для загрузки
                if len(result_topics) < min(remaining, 100):
                    break
                
                # Обновляем offset для следующей итерации
                last_topic = result_topics[-1]
                new_offset_topic = getattr(last_topic, "id", 0) or 0
                new_offset_id = getattr(last_topic, "top_message", 0) or 0
                
                # Проверяем, что offset изменился, иначе выходим из цикла
                if new_offset_topic == offset_topic and new_offset_id == offset_id:
                    self.logger.debug(f"Offset не изменился для {entity.id}, завершение")
                    break
                
                offset_topic = new_offset_topic
                offset_id = new_offset_id
                remaining = limit - len(topics)
        except ChatAdminRequiredError:
            self.logger.debug(f"Требуются права администратора для получения тем форума {entity.id}")
        except Exception as e:
            error_msg = str(e)
            error_type = type(e).__name__
            self.logger.debug(
                f"Не удалось получить темы форума для {entity.id}: {error_type}: {error_msg}"
            )
        return topics

    async def _fetch_last_message_date(self, entity: Channel) -> Optional[str]:
        """
        Получает дату последнего сообщения в канале/чате/группе.
        
        Args:
            entity: Сущность канала/группы
        
        Returns:
            Дата последнего сообщения в ISO формате или None
        """
        try:
            messages = await self.client.get_messages(entity, limit=1)
            if messages and messages[0] and messages[0].date:
                return messages[0].date.isoformat()
        except Exception as e:
            self.logger.debug(f"Не удалось получить дату последнего сообщения для {entity.id}: {e}")
        return None

    async def _leave_channel_or_chat(self, entity: Channel) -> bool:
        """
        Выполняет отписку от канала или выход из группы.
        
        Args:
            entity: Сущность канала/группы
        
        Returns:
            True, если отписка выполнена успешно
        """
        try:
            if isinstance(entity, Channel):
                await self.client(functions.channels.LeaveChannelRequest(channel=entity))
            elif isinstance(entity, Chat):
                await self.client(functions.messages.DeleteChatUser(chat_id=entity.id, user_id="me"))
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при попытке отписки от {entity.id}: {e}")
            return False

    async def get_channel_info(
        self,
        entity: Channel,
        last_message_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Получает детальную информацию о канале.
        
        Args:
            entity: Объект канала из Telegram API
            last_message_date: Дата последнего сообщения (если уже получена)
        
        Returns:
            Словарь с информацией о канале или None в случае ошибки
        """
        try:
            self.logger.debug(f"Получение информации о канале: {entity.title}")
            
            # Получаем базовую информацию о канале
            await self.client.get_entity(entity)
            
            # Базовые данные канала
            channel_data: Dict[str, Any] = {
                "id": entity.id,
                "title": entity.title or "Без названия",
                "username": entity.username or "Нет username",
                "is_broadcast": entity.broadcast,  # True для каналов, False для групп
                "is_megagroup": entity.megagroup,  # True для супергрупп
                "is_gigagroup": getattr(entity, 'gigagroup', False),
                "access_hash": str(entity.access_hash) if hasattr(entity, 'access_hash') else None,
                "scanned_at": datetime.now().isoformat(),
                "processing_status": "Ок",
            }
            
            # Флаги канала (быстрая проверка, не требует дополнительных запросов)
            channel_data["is_verified"] = "Да" if getattr(entity, 'verified', False) else "Нет"
            channel_data["is_scam"] = "Да" if getattr(entity, 'scam', False) else "Нет"
            channel_data["is_fake"] = "Да" if getattr(entity, 'fake', False) else "Нет"
            channel_data["is_restricted"] = "Да" if getattr(entity, 'restricted', False) else "Нет"
            channel_data["is_min"] = "Да" if getattr(entity, 'min', False) else "Нет"
            
            # Пытаемся получить полную информацию для расширенных данных
            full_channel_info = None
            full_chat_info = None
            try:
                if isinstance(entity, Channel):
                    full_channel_info = await self.client(
                        functions.channels.GetFullChannelRequest(channel=entity)
                    )
                elif isinstance(entity, Chat):
                    full_chat_info = await self.client(
                        functions.messages.GetFullChatRequest(chat_id=entity.id)
                    )
            except ChatAdminRequiredError:
                self.logger.debug("Требуются права администратора для получения полной информации")
            except Exception as e:
                self.logger.debug(f"Не удалось получить полную информацию: {e}")
            
            # Пытаемся получить количество участников
            participants_count = await self._fetch_participants_count(
                entity,
                full_channel_info=full_channel_info,
                full_chat_info=full_chat_info,
            )
            if participants_count is None:
                # Дополнительная попытка через подсчет участников (только для групп)
                if not entity.broadcast:
                    try:
                        count = 0
                        async for _ in self.client.iter_participants(entity):
                            count += 1
                            if count >= 10000:  # Ограничение для производительности
                                break
                        participants_count = count if count < 10000 else ">10000"
                    except Exception as e:
                        self.logger.debug(
                            f"Не удалось получить количество участников через iter_participants: {e}"
                        )
            # Связанный канал (если настроен)
            linked_chat_id = None
            if full_channel_info and hasattr(full_channel_info, "full_chat"):
                linked_chat_id = getattr(full_channel_info.full_chat, "linked_chat_id", None)
            linked_entity = None
            if linked_chat_id:
                linked_data = await self._fetch_linked_channel_info(linked_chat_id)
                linked_entity = linked_data.pop("_linked_entity", None)
                channel_data.update(linked_data)
            else:
                channel_data.update(
                    {
                        "linked_chat_id": None,
                        "linked_chat_title": None,
                        "linked_chat_username": None,
                        "linked_chat_link": None,
                    }
                )

            # Темы форума (если супергруппа с включенными темами)
            forum_topics: List[str] = []
            is_forum = bool(getattr(entity, "forum", False))
            if full_channel_info and hasattr(full_channel_info, "full_chat"):
                is_forum = is_forum or bool(getattr(full_channel_info.full_chat, "forum", False))
            
            # Пробуем получить темы форума для всех супергрупп, даже если forum=False
            # так как иногда флаг может быть не установлен, но темы есть
            if isinstance(entity, Channel) and entity.megagroup:
                self.logger.debug(
                    f"Проверка тем форума для {entity.id} (is_forum={is_forum}, megagroup={entity.megagroup})"
                )
                forum_topics = await self._fetch_forum_topics(entity)
                if forum_topics:
                    self.logger.debug(
                        f"Найдено тем форума для {entity.id}: {len(forum_topics)}"
                    )
                else:
                    self.logger.debug(
                        f"Темы форума для {entity.id} не найдены или недоступны"
                    )
            
            # Пробуем получить темы из связанного чата, если в основном не нашли
            if not forum_topics and linked_entity and isinstance(linked_entity, Channel):
                if getattr(linked_entity, "megagroup", False):
                    self.logger.debug(
                        f"Попытка получения тем форума для связанного чата {linked_entity.id}"
                    )
                    forum_topics = await self._fetch_forum_topics(linked_entity)
                    if forum_topics:
                        self.logger.debug(
                            f"Найдено тем форума для связанного чата {linked_entity.id}: {len(forum_topics)}"
                        )
                    else:
                        self.logger.debug(
                            f"Темы форума для связанного чата {linked_entity.id} не найдены"
                        )
            channel_data["forum_topics"] = forum_topics
            channel_data["forum_topics_count"] = len(forum_topics)

            # Дата последнего сообщения
            if last_message_date:
                channel_data["last_message_date"] = last_message_date
            else:
                channel_data["last_message_date"] = await self._fetch_last_message_date(entity)
            
            # Дополнительная информация
            if hasattr(entity, 'about'):
                channel_data["about"] = entity.about or "Нет описания"
            else:
                channel_data["about"] = "Нет описания"
            
            # Дата создания (если доступна)
            if hasattr(entity, 'date'):
                channel_data["created_date"] = entity.date.isoformat() if entity.date else None
            else:
                channel_data["created_date"] = None
            
            # Проверяем, является ли канал публичным
            channel_data["is_public"] = entity.username is not None
            
            # Ссылка на канал
            if entity.username:
                channel_data["link"] = f"https://t.me/{entity.username}"
            else:
                channel_data["link"] = f"tg://resolve?domain={entity.id}"
            
            # Дополнительная статистика из full_channel_info (быстрые данные, не требуют долгих запросов)
            if full_channel_info and hasattr(full_channel_info, "full_chat"):
                full_chat = full_channel_info.full_chat
                
                # Режим медленной отправки
                channel_data["slowmode_seconds"] = getattr(full_chat, "slowmode_seconds", None) or ""
                
                # Количество онлайн (если доступно)
                channel_data["online_count"] = getattr(full_chat, "online_count", None) or ""
                
                # Количество непрочитанных сообщений
                channel_data["unread_count"] = getattr(full_chat, "unread_count", None) or ""
                
                # ID закрепленного сообщения
                channel_data["pinned_msg_id"] = getattr(full_chat, "pinned_msg_id", None) or ""
                
                # ID папки
                channel_data["folder_id"] = getattr(full_chat, "folder_id", None) or ""
                
                # Геолокация (если установлена)
                location = getattr(full_chat, "location", None)
                if location:
                    if hasattr(location, "geo_point"):
                        geo = location.geo_point
                        if hasattr(geo, "lat") and hasattr(geo, "long"):
                            channel_data["location"] = f"{geo.lat}, {geo.long}"
                        else:
                            channel_data["location"] = "Установлена"
                    else:
                        channel_data["location"] = "Установлена"
                else:
                    channel_data["location"] = ""
                
                # Информация о миграции (если группа была мигрирована)
                channel_data["migrated_from_chat_id"] = getattr(full_chat, "migrated_from_chat_id", None) or ""
                channel_data["migrated_from_max_id"] = getattr(full_chat, "migrated_from_max_id", None) or ""
                
                # Права пользователя (только самые важные)
                channel_data["can_view_participants"] = "Да" if getattr(full_chat, "can_view_participants", False) else "Нет"
                channel_data["can_set_username"] = "Да" if getattr(full_chat, "can_set_username", False) else "Нет"
            else:
                # Значения по умолчанию, если full_channel_info недоступен
                channel_data["slowmode_seconds"] = ""
                channel_data["online_count"] = ""
                channel_data["unread_count"] = ""
                channel_data["pinned_msg_id"] = ""
                channel_data["folder_id"] = ""
                channel_data["location"] = ""
                channel_data["migrated_from_chat_id"] = ""
                channel_data["migrated_from_max_id"] = ""
                channel_data["can_view_participants"] = ""
                channel_data["can_set_username"] = ""
            
            participants_text = self._format_participants_count(channel_data.get("participants_count"))
            self.logger.info(
                f"Успешно получена информация о канале: {channel_data['title']} "
                f"(подписчиков: {participants_text})"
            )
            return channel_data
            
        except FloodWaitError as e:
            self.logger.warning(f"Превышен лимит запросов. Ожидание {e.seconds} секунд")
            await asyncio.sleep(e.seconds)
            return await self.get_channel_info(entity)
        except Exception as e:
            self.logger.error(f"Ошибка при получении информации о канале {entity.title}: {e}")
            return None
    
    async def scan_all_channels(self) -> List[Dict[str, Any]]:
        """
        Сканирует все каналы, группы и супергруппы пользователя.
        
        Returns:
            Список словарей с информацией о каждом канале
        """
        self.logger.info("Начало сканирования каналов")
        self.channels_data = []
        
        try:
            # Получаем все диалоги (чаты, каналы, группы)
            self.logger.debug("Получение списка всех диалогов")
            dialogs = await self.client.get_dialogs()
            
            self.logger.info(f"Найдено диалогов: {len(dialogs)}")
            
            # Фильтруем только каналы и группы
            channels_and_groups = []
            last_message_map: Dict[int, Optional[str]] = {}
            for dialog in dialogs:
                entity = dialog.entity
                # Проверяем, является ли это каналом или группой
                if isinstance(entity, Channel):
                    channels_and_groups.append(entity)
                    message_date = None
                    if getattr(dialog, "message", None) and getattr(dialog.message, "date", None):
                        message_date = dialog.message.date.isoformat()
                    last_message_map[entity.id] = message_date
                    self.logger.debug(f"Найден канал/группа: {entity.title}")
            
            self.logger.info(f"Найдено каналов и групп: {len(channels_and_groups)}")
            
            # Получаем информацию о каждом канале с ограничением параллелизма
            semaphore = asyncio.Semaphore(self.concurrency)

            async def process_channel(index: int, entity: Channel) -> Optional[Dict[str, Any]]:
                """
                Обрабатывает один канал с ограничением параллелизма.
                
                Args:
                    index: Порядковый номер канала
                    entity: Сущность канала/группы
                
                Returns:
                    Словарь с информацией о канале или None
                """
                async with semaphore:
                    self.logger.info(
                        f"Обработка канала {index}/{len(channels_and_groups)}: {entity.title}"
                    )
                    start_time = monotonic()
                    try:
                        channel_info = await asyncio.wait_for(
                            self.get_channel_info(
                                entity,
                                last_message_date=last_message_map.get(entity.id),
                            ),
                            timeout=self.channel_timeout,
                        )
                    except asyncio.TimeoutError:
                        self.logger.warning(
                            f"Таймаут обработки канала {entity.id} "
                            f"(>{self.channel_timeout:.0f} сек)"
                        )
                        channel_info = self._build_basic_channel_info(
                            entity,
                            last_message_date=last_message_map.get(entity.id),
                            status="Таймаут",
                        )
                    duration = monotonic() - start_time
                    if duration > 10:
                        self.logger.warning(
                            f"Долгая обработка канала {entity.id}: {duration:.1f} сек"
                        )
                if channel_info:
                    participants_text = self._format_participants_count(
                        channel_info.get("participants_count")
                    )
                    self.logger.info(
                        f"Канал обработан: {channel_info.get('title', '')} "
                        f"(подписчиков: {participants_text})"
                    )
                    if channel_info.get("id") in self.unsubscribe_ids:
                        self.logger.info(
                            f"Отписка по списку: {channel_info.get('title', '')} "
                            f"(id: {channel_info.get('id')})"
                        )
                        is_unsubscribed = await self._leave_channel_or_chat(entity)
                        if is_unsubscribed:
                            channel_info["unsubscribed_status"] = "Да"
                        else:
                            channel_info["unsubscribed_status"] = "Ошибка отписки"
                    else:
                        channel_info["unsubscribed_status"] = "Нет"
                else:
                    channel_info = self._build_basic_channel_info(
                        entity,
                        last_message_date=last_message_map.get(entity.id),
                        status="Ошибка",
                    )
                # Убрали задержку - Semaphore уже контролирует параллелизм
                # Задержка нужна только при очень высокой нагрузке для предотвращения FloodWaitError
                return channel_info

            tasks = [
                process_channel(index, channel)
                for index, channel in enumerate(channels_and_groups, 1)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, dict):
                    self.channels_data.append(result)
                elif isinstance(result, Exception):
                    self.logger.error(f"Ошибка при обработке канала: {result}")
            
            self.logger.info(f"Сканирование завершено. Обработано каналов: {len(self.channels_data)}")
            return self.channels_data
            
        except Exception as e:
            self.logger.error(f"Критическая ошибка при сканировании каналов: {e}")
            raise
    
    def save_to_json(self, filename: str = "channels_data.json") -> None:
        """
        Сохраняет данные о каналах в JSON файл.
        
        Args:
            filename: Имя файла для сохранения
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            output_path = self.output_dir / self._append_timestamp(filename, timestamp)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(self.channels_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Данные сохранены в файл: {output_path}")
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении данных: {e}")
            raise
    
    def save_to_text(self, filename: str = "channels_list.txt") -> None:
        """
        Сохраняет данные о каналах в текстовый файл для удобного чтения.
        
        Args:
            filename: Имя файла для сохранения
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            output_path = self.output_dir / self._append_timestamp(filename, timestamp)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("СПИСОК КАНАЛОВ И ГРУПП TELEGRAM\n")
                f.write(f"Дата сканирования: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Всего найдено: {len(self.channels_data)}\n")
                f.write("=" * 80 + "\n\n")
                
                for i, channel in enumerate(self.channels_data, 1):
                    f.write(f"\n{'=' * 80}\n")
                    f.write(f"Канал #{i}\n")
                    f.write(f"{'=' * 80}\n")
                    f.write(f"Название: {channel['title']}\n")
                    f.write(f"ID: {channel['id']}\n")
                    f.write(f"Username: {channel['username']}\n")
                    f.write(f"Тип: ")
                    if channel['is_broadcast']:
                        f.write("Канал (Broadcast)\n")
                    elif channel['is_megagroup']:
                        f.write("Супергруппа (Megagroup)\n")
                    elif channel['is_gigagroup']:
                        f.write("Гигагруппа (Gigagroup)\n")
                    else:
                        f.write("Группа\n")
                    f.write(f"Публичный: {'Да' if channel['is_public'] else 'Нет'}\n")
                    f.write(f"Количество участников: {channel.get('participants_count', 'Неизвестно')}\n")
                    f.write(f"Описание: {channel.get('about', 'Нет описания')}\n")
                    f.write(f"Ссылка: {channel['link']}\n")
                    if channel.get('created_date'):
                        f.write(f"Дата создания: {channel['created_date']}\n")
                    f.write(f"Дата сканирования: {channel['scanned_at']}\n")
                
            self.logger.info(f"Текстовый отчет сохранен в файл: {output_path}")
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении текстового файла: {e}")
            raise

    def _build_xlsx_rows(self) -> Tuple[List[str], List[List[str]]]:
        """
        Подготавливает заголовки и строки для выгрузки в XLSX.
        
        Returns:
            Заголовки и строки в виде списков строк
        """
        headers = [
            "ID",
            "Название",
            "Username",
            "Тип",
            "Публичный",
            "Участников",
            "Описание",
            "Ссылка",
            "Верифицирован",
            "Мошеннический",
            "Фейковый",
            "Ограниченный",
            "Скрытый",
            "Связанный канал ID",
            "Связанный канал",
            "Связанный канал ссылка",
            "Режим медленной отправки (сек)",
            "Онлайн",
            "Непрочитанных",
            "ID закрепленного сообщения",
            "ID папки",
            "Геолокация",
            "Миграция из чата ID",
            "Можно просматривать участников",
            "Можно менять username",
            "Удален по списку",
            "Статус обработки",
            "Темы форума (кол-во)",
            "Темы форума",
            "Дата создания",
            "Дата последнего сообщения",
            "Дата сканирования",
        ]
        rows: List[List[Any]] = []
        sorted_channels = sorted(
            self.channels_data,
            key=self._participants_sort_key,
            reverse=True,
        )
        for channel in sorted_channels:
            if channel.get("is_broadcast"):
                channel_type = "Канал"
            elif channel.get("is_megagroup"):
                channel_type = "Супергруппа"
            elif channel.get("is_gigagroup"):
                channel_type = "Гигагруппа"
            else:
                channel_type = "Группа"
            participants_value = channel.get("participants_count")
            if isinstance(participants_value, int):
                participants_cell: Any = participants_value
            elif isinstance(participants_value, str) and participants_value.isdigit():
                participants_cell = int(participants_value)
            elif participants_value is None:
                participants_cell = "Неизвестно"
            else:
                participants_cell = str(participants_value)
            forum_topics = channel.get("forum_topics") or []
            forum_topics_text = "; ".join(str(item) for item in forum_topics) if forum_topics else ""
            rows.append(
                [
                    str(channel.get("id", "")),
                    str(channel.get("title", "")),
                    str(channel.get("username", "")),
                    channel_type,
                    "Да" if channel.get("is_public") else "Нет",
                    participants_cell,
                    str(channel.get("about", "")),
                    str(channel.get("link", "")),
                    str(channel.get("is_verified", "")),
                    str(channel.get("is_scam", "")),
                    str(channel.get("is_fake", "")),
                    str(channel.get("is_restricted", "")),
                    str(channel.get("is_min", "")),
                    str(channel.get("linked_chat_id", "")) if channel.get("linked_chat_id") else "",
                    str(channel.get("linked_chat_title", "")) if channel.get("linked_chat_title") else "",
                    str(channel.get("linked_chat_link", "")) if channel.get("linked_chat_link") else "",
                    channel.get("slowmode_seconds") if channel.get("slowmode_seconds") not in (None, "") else "",
                    channel.get("online_count") if channel.get("online_count") not in (None, "") else "",
                    channel.get("unread_count") if channel.get("unread_count") not in (None, "") else "",
                    channel.get("pinned_msg_id") if channel.get("pinned_msg_id") not in (None, "") else "",
                    channel.get("folder_id") if channel.get("folder_id") not in (None, "") else "",
                    str(channel.get("location", "")),
                    channel.get("migrated_from_chat_id") if channel.get("migrated_from_chat_id") not in (None, "") else "",
                    str(channel.get("can_view_participants", "")),
                    str(channel.get("can_set_username", "")),
                    str(channel.get("unsubscribed_status", "")),
                    str(channel.get("processing_status", "")),
                    int(channel.get("forum_topics_count", 0) or 0),
                    forum_topics_text,
                    str(channel.get("created_date", "")) if channel.get("created_date") else "",
                    str(channel.get("last_message_date", "")) if channel.get("last_message_date") else "",
                    str(channel.get("scanned_at", "")),
                ]
            )
        return headers, rows

    def _participants_sort_key(self, channel: Dict[str, Any]) -> int:
        """
        Возвращает числовой ключ для сортировки по количеству участников.
        
        Args:
            channel: Данные канала
        
        Returns:
            Число участников, если доступно, иначе 0
        """
        participants_value = channel.get("participants_count")
        if isinstance(participants_value, int):
            return participants_value
        if isinstance(participants_value, str) and participants_value.isdigit():
            return int(participants_value)
        return 0

    def _write_xlsx_sheet(
        self,
        workbook: "xlsxwriter.Workbook",
        sheet_name: str,
        headers: List[str],
        rows: List[List[Any]],
        numeric_formats: Optional[Dict[str, str]] = None,
        date_columns: Optional[Set[str]] = None,
    ) -> None:
        """
        Записывает данные и оформление листа XLSX.
        
        Args:
            workbook: Экземпляр Workbook
            sheet_name: Имя листа
            headers: Заголовки таблицы
            rows: Данные строк
            numeric_formats: Форматы числовых колонок по названию
        """
        worksheet = workbook.add_worksheet(sheet_name)
        header_format = workbook.add_format(
            {
                "bold": True,
                "font_color": "#FFFFFF",
                "bg_color": "#2F75B5",
                "align": "center",
                "valign": "vcenter",
                "border": 1,
            }
        )
        default_row_format = workbook.add_format({"bg_color": "#FFFFFF"})
        worksheet.set_default_row(None, default_row_format)
        data_format = workbook.add_format(
            {"text_wrap": True, "valign": "top", "border": 1, "bg_color": "#FFFFFF"}
        )
        zebra_format = workbook.add_format(
            {"text_wrap": True, "valign": "top", "border": 1, "bg_color": "#F3F6FA"}
        )
        numeric_formats = numeric_formats or {}
        date_columns = date_columns or set()
        numeric_format_map: Dict[int, str] = {
            headers.index(name): fmt
            for name, fmt in numeric_formats.items()
            if name in headers
        }
        date_cols = {headers.index(name) for name in date_columns if name in headers}
        numeric_format_cache: Dict[Tuple[str, bool], Any] = {}
        date_format_cache: Dict[bool, Any] = {}

        for col_idx, header in enumerate(headers):
            worksheet.write(0, col_idx, header, header_format)

        for row_idx, row in enumerate(rows, start=1):
            is_zebra = row_idx % 2 == 0
            for col_idx, value in enumerate(row):
                # Пропускаем пустые значения (None или пустая строка)
                if value is None or value == "":
                    worksheet.write_blank(row_idx, col_idx, None, zebra_format if is_zebra else data_format)
                    continue
                
                fmt = zebra_format if is_zebra else data_format
                if col_idx in date_cols and value:
                    try:
                        parsed = datetime.fromisoformat(str(value))
                        if parsed.tzinfo:
                            parsed = parsed.replace(tzinfo=None)
                        if is_zebra not in date_format_cache:
                            date_format_cache[is_zebra] = workbook.add_format(
                                {
                                    "num_format": "yyyy-mm-dd hh:mm",
                                    "valign": "top",
                                    "border": 1,
                                    "bg_color": "#F3F6FA" if is_zebra else "#FFFFFF",
                                }
                            )
                        worksheet.write_datetime(row_idx, col_idx, parsed, date_format_cache[is_zebra])
                        continue
                    except (ValueError, TypeError):
                        pass
                if col_idx in numeric_format_map and isinstance(value, (int, float)):
                    format_key = (numeric_format_map[col_idx], is_zebra)
                    if format_key not in numeric_format_cache:
                        format_payload = {
                            "align": "right",
                            "valign": "top",
                            "border": 1,
                            "num_format": numeric_format_map[col_idx],
                        }
                        format_payload["bg_color"] = "#F3F6FA" if is_zebra else "#FFFFFF"
                        numeric_format_cache[format_key] = workbook.add_format(format_payload)
                    fmt = numeric_format_cache[format_key]
                worksheet.write(row_idx, col_idx, value, fmt)

        worksheet.freeze_panes(1, 0)
        if rows:
            worksheet.autofilter(0, 0, len(rows), len(headers) - 1)
        else:
            worksheet.autofilter(0, 0, 0, len(headers) - 1)

        for col_idx, header in enumerate(headers):
            if header == "Темы форума":
                worksheet.set_column(col_idx, col_idx, 80)
                continue
            max_len = len(header)
            for row in rows:
                value = row[col_idx]
                if value:
                    max_len = max(max_len, len(str(value)))
            adjusted = min(max(max_len + 2, 12), 60)
            worksheet.set_column(col_idx, col_idx, adjusted)

    def save_to_xlsx(self, filename: str = "channels_data.xlsx") -> None:
        """
        Сохраняет данные о каналах в XLSX файл с удобным форматированием.
        
        Args:
            filename: Имя файла для сохранения
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            output_path = self.output_dir / self._append_timestamp(filename, timestamp)
            workbook = xlsxwriter.Workbook(str(output_path))

            headers, rows = self._build_xlsx_rows()
            self._write_xlsx_sheet(
                workbook,
                "Каналы",
                headers,
                rows,
                numeric_formats={
                    "Участников": "#,##0",
                    "Темы форума (кол-во)": "#,##0",
                    "Режим медленной отправки (сек)": "#,##0",
                    "Онлайн": "#,##0",
                    "Непрочитанных": "#,##0",
                    "ID закрепленного сообщения": "#,##0",
                    "ID папки": "#,##0",
                    "Миграция из чата ID": "#,##0",
                },
                date_columns={
                    "Дата создания",
                    "Дата последнего сообщения",
                    "Дата сканирования",
                },
            )

            private_headers, private_rows = self._build_private_xlsx_rows()
            
            # Формируем numeric_formats в зависимости от наличия ID для подсчета текста
            private_numeric_formats = {
                "Сообщений за 90 дней": "#,##0",
                "Среднее в день": "#,##0.00",
                "Среднее в неделю": "#,##0.00",
                "Среднее в месяц": "#,##0.00",
                "Сообщений за год": "#,##0",
                "Сообщений всего": "#,##0",
                "Сообщений от вас": "#,##0",
                "Сообщений от собеседника": "#,##0",
                "Общих чатов": "#,##0",
                "Время обработки (сек)": "0.00",
            }
            
            # Добавляем форматы для колонок со словами и буквами только если есть ID в списке
            if self.private_text_timeout_ids:
                private_numeric_formats.update({
                    "Слов от вас": "#,##0",
                    "Слов от собеседника": "#,##0",
                    "Слов всего": "#,##0",
                    "Букв от вас": "#,##0",
                    "Букв от собеседника": "#,##0",
                    "Букв всего": "#,##0",
                })
            
            self._write_xlsx_sheet(
                workbook,
                "Личные чаты",
                private_headers,
                private_rows,
                numeric_formats=private_numeric_formats,
                date_columns={"Дата последнего сообщения"},
            )
            workbook.close()
            self.logger.info(f"XLSX отчет сохранен в файл: {output_path}")
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении XLSX файла: {e}")
            raise

    def _append_timestamp(self, filename: str, timestamp: str) -> str:
        """
        Добавляет таймштамп к имени файла перед расширением.
        
        Args:
            filename: Имя файла
            timestamp: Таймштамп в формате YYYYMMDD_HHMM
        
        Returns:
            Имя файла с добавленным таймштампом
        """
        if "." in filename:
            name, ext = filename.rsplit(".", 1)
            return f"{name}_{timestamp}.{ext}"
        return f"{filename}_{timestamp}"

    async def scan_private_chats(self) -> List[Dict[str, Any]]:
        """
        Сканирует личные чаты и собирает статистику сообщений.
        
        Returns:
            Список словарей с информацией о личных чатах
        """
        self.logger.info("Начало сканирования личных чатов")
        self.private_chats_data = []
        try:
            dialogs = await self.client.get_dialogs()
            private_dialogs = []
            last_message_map: Dict[int, Optional[str]] = {}
            for dialog in dialogs:
                entity = dialog.entity
                if isinstance(entity, User) and not getattr(entity, "bot", False):
                    if getattr(entity, "is_self", False):
                        continue
                    private_dialogs.append(entity)
                    message_date = None
                    if getattr(dialog, "message", None) and getattr(dialog.message, "date", None):
                        message_date = dialog.message.date.isoformat()
                    last_message_map[entity.id] = message_date

            self.logger.info(f"Найдено личных чатов: {len(private_dialogs)}")
            self.logger.info("Старт параллельной обработки личных чатов")
            semaphore = asyncio.Semaphore(self.concurrency)

            async def process_private_chat(index: int, entity: User) -> Optional[Dict[str, Any]]:
                """
                Обрабатывает личный чат с ограничением параллелизма.
                
                Args:
                    index: Порядковый номер чата
                    entity: Пользователь чата
                
                Returns:
                    Словарь с данными личного чата или None
                """
                async with semaphore:
                    display_name = " ".join(
                        part for part in [
                            getattr(entity, "first_name", ""),
                            getattr(entity, "last_name", "")
                        ] if part
                    ).strip() or "Без имени"
                    username = getattr(entity, "username", None)
                    name_with_username = f"{display_name} (@{username})" if username else display_name
                    self.logger.info(
                        f"Обработка личного чата {index}/{len(private_dialogs)}: {name_with_username}"
                    )
                    start_time = monotonic()
                    try:
                        use_text_stats = entity.id in self.private_text_timeout_ids
                        if entity.id in self.private_text_timeout_ids:
                            timeout_value = self.private_text_timeout
                        elif entity.id in self.private_timeout_ids:
                            timeout_value = self.private_timeout
                        else:
                            timeout_value = self.request_timeout
                        if entity.id in self.private_text_timeout_ids:
                            self.logger.debug(
                                f"Личный чат {entity.id} ({name_with_username}): таймаут для текста {timeout_value} сек"
                            )
                        elif entity.id in self.private_timeout_ids:
                            self.logger.debug(
                                f"Личный чат {entity.id} ({name_with_username}): применен отдельный таймаут {timeout_value} сек"
                            )
                        chat_info = await asyncio.wait_for(
                            self._collect_private_chat_info(
                                entity,
                                last_message_map.get(entity.id),
                                use_text_stats,
                            ),
                            timeout=timeout_value,
                        )
                    except asyncio.TimeoutError:
                        self.logger.warning(
                            f"Таймаут обработки личного чата {entity.id} ({name_with_username}) "
                            f"(>{timeout_value:.0f} сек)"
                        )
                        chat_info = await self._build_basic_private_chat_info(
                            entity,
                            last_message_map.get(entity.id),
                            status="Таймаут",
                        )
                    duration = monotonic() - start_time
                    if chat_info:
                        chat_info["processing_time"] = round(duration, 2)
                    if duration > 10:
                        self.logger.warning(
                            f"Долгая обработка личного чата {entity.id} ({name_with_username}): {duration:.1f} сек"
                        )
                    # Убрали задержку - Semaphore уже контролирует параллелизм
                    return chat_info

            tasks = [
                process_private_chat(index, entity)
                for index, entity in enumerate(private_dialogs, 1)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            completed = 0
            for result in results:
                if isinstance(result, dict):
                    self.private_chats_data.append(result)
                    completed += 1
                    if completed % 5 == 0:
                        self.logger.info(
                            f"Прогресс личных чатов: {completed}/{len(private_dialogs)}"
                        )
                elif isinstance(result, Exception):
                    self.logger.error(f"Ошибка при обработке личного чата: {result}")
            self.logger.info(
                f"Обработка личных чатов завершена: {len(self.private_chats_data)}"
            )
            return self.private_chats_data
        except Exception as e:
            self.logger.error(f"Критическая ошибка при сканировании личных чатов: {e}")
            raise

    async def _collect_private_chat_info(
        self,
        entity: User,
        last_message_date: Optional[str],
        use_text_stats: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Собирает информацию и статистику по личному чату.
        
        Args:
            entity: Пользователь
            last_message_date: Дата последнего сообщения (если уже известна)
        
        Returns:
            Словарь с данными личного чата
        """
        try:
            display_name = " ".join(
                part for part in [getattr(entity, "first_name", ""), getattr(entity, "last_name", "")] if part
            ).strip() or "Без имени"
            now = datetime.now(timezone.utc)
            threshold_90 = now - timedelta(days=90)
            threshold_year = now - timedelta(days=365)
            messages_90 = 0
            messages_year = 0
            messages_total = 0
            messages_from_me = 0
            messages_from_other = 0
            words_total = 0
            words_from_me = 0
            words_from_other = 0
            chars_total = 0
            chars_from_me = 0
            chars_from_other = 0

            # Начинаем получение информации о пользователе параллельно с подсчетом сообщений
            full_user_info_task = asyncio.create_task(
                self.client(functions.users.GetFullUserRequest(id=entity))
            )
            
            last_progress = monotonic()
            # Оптимизация: iter_messages уже оптимизирован в Telethon, но можем ускорить обработку
            # Используем более эффективную обработку сообщений
            async for message in self.client.iter_messages(entity):
                if not getattr(message, "date", None):
                    continue
                message_date = message.date
                messages_total += 1
                if message.out:
                    messages_from_me += 1
                else:
                    messages_from_other += 1
                if use_text_stats:
                    message_text = getattr(message, "message", None) or ""
                    if message_text:
                        word_count = len(message_text.split())
                        char_count = len(message_text)
                        words_total += word_count
                        chars_total += char_count
                        if message.out:
                            words_from_me += word_count
                            chars_from_me += char_count
                        else:
                            words_from_other += word_count
                            chars_from_other += char_count
                if message_date >= threshold_year:
                    messages_year += 1
                if message_date >= threshold_90:
                    messages_90 += 1
                if messages_total % 2000 == 0:
                    elapsed = monotonic() - last_progress
                    last_progress = monotonic()
                    self.logger.debug(
                        f"Личный чат {entity.id}: обработано {messages_total} сообщений "
                        f"(за {elapsed:.1f} сек)"
                    )

            average_per_day = round(messages_90 / 90, 2)
            average_per_week = round(messages_90 / (90 / 7), 2)
            average_per_month = round(messages_90 / 3, 2)

            # Получаем результат параллельной задачи получения информации о пользователе
            about = None
            common_chats_count = None
            full_user_info = None
            try:
                full_user_info = await full_user_info_task
            except Exception as e:
                self.logger.debug(
                    f"Не удалось получить полную информацию о пользователе {entity.id}: {e} "
                    f"[class: ChannelScanner | def: _collect_private_chat_info]"
                )
                full_user_info = None
                # Пробуем разные способы доступа к about и common_chats_count
                if full_user_info:
                    # Способ 1: через full_user (основной способ для UserFull)
                    if hasattr(full_user_info, "full_user"):
                        full_user = full_user_info.full_user
                        about = getattr(full_user, "about", None) or getattr(full_user, "bio", None)
                        common_chats_count = getattr(full_user, "common_chats_count", None)
                        if about:
                            self.logger.debug(
                                f"Получено 'О себе' для пользователя {entity.id} через full_user.about "
                                f"[class: ChannelScanner | def: _collect_private_chat_info]"
                            )
                        if common_chats_count is not None:
                            self.logger.debug(
                                f"Получено 'Общих чатов' для пользователя {entity.id}: {common_chats_count} "
                                f"через full_user.common_chats_count "
                                f"[class: ChannelScanner | def: _collect_private_chat_info]"
                            )
                    # Способ 2: напрямую из full_user_info
                    if not about:
                        about = getattr(full_user_info, "about", None) or getattr(full_user_info, "bio", None)
                        if about:
                            self.logger.debug(
                                f"Получено 'О себе' для пользователя {entity.id} напрямую из full_user_info "
                                f"[class: ChannelScanner | def: _collect_private_chat_info]"
                            )
                    if common_chats_count is None:
                        common_chats_count = getattr(full_user_info, "common_chats_count", None)
                        if common_chats_count is not None:
                            self.logger.debug(
                                f"Получено 'Общих чатов' для пользователя {entity.id}: {common_chats_count} "
                                f"напрямую из full_user_info "
                                f"[class: ChannelScanner | def: _collect_private_chat_info]"
                            )
                    # Способ 3: через users (если есть)
                    if not about and hasattr(full_user_info, "users") and full_user_info.users:
                        for user_obj in full_user_info.users:
                            about = getattr(user_obj, "about", None) or getattr(user_obj, "bio", None)
                            if about:
                                self.logger.debug(
                                    f"Получено 'О себе' для пользователя {entity.id} через users "
                                    f"[class: ChannelScanner | def: _collect_private_chat_info]"
                                )
                                break
                    if not about:
                        self.logger.debug(
                            f"Не найдено 'О себе' для пользователя {entity.id}, "
                            f"тип ответа: {type(full_user_info).__name__}, "
                            f"атрибуты: {[attr for attr in dir(full_user_info) if not attr.startswith('_')]} "
                            f"[class: ChannelScanner | def: _collect_private_chat_info]"
                        )
                    if common_chats_count is None:
                        self.logger.debug(
                            f"Не найдено 'Общих чатов' для пользователя {entity.id}, "
                            f"тип ответа: {type(full_user_info).__name__} "
                            f"[class: ChannelScanner | def: _collect_private_chat_info]"
                        )
            except Exception as e:
                self.logger.debug(
                    f"Не удалось получить полную информацию о пользователе {entity.id}: {e} "
                    f"[class: ChannelScanner | def: _collect_private_chat_info]"
                )
            
            # Если не получили через GetFullUserRequest, пробуем из базового объекта
            if not about:
                about = getattr(entity, "about", None) or getattr(entity, "bio", None)
            if common_chats_count is None:
                common_chats_count = getattr(entity, "common_chats_count", None)
            
            is_bot = getattr(entity, "bot", False) or getattr(entity, "is_bot", False)
            is_verified = getattr(entity, "verified", False) or getattr(entity, "is_verified", False)
            is_premium = getattr(entity, "premium", False) or getattr(entity, "is_premium", False)
            is_scam = getattr(entity, "scam", False) or getattr(entity, "is_scam", False)
            is_fake = getattr(entity, "fake", False) or getattr(entity, "is_fake", False)
            is_restricted = getattr(entity, "restricted", False) or getattr(entity, "is_restricted", False)
            
            mutual_contact = getattr(entity, "mutual_contact", False)
            contact = getattr(entity, "contact", False)
            lang_code = getattr(entity, "lang_code", None)

            return {
                "id": entity.id,
                "name": display_name,
                "username": getattr(entity, "username", None),
                "phone": getattr(entity, "phone", None),
                "last_message_date": last_message_date,
                "messages_90": messages_90,
                "avg_day": average_per_day,
                "avg_week": average_per_week,
                "avg_month": average_per_month,
                "messages_year": messages_year,
                "messages_total": messages_total,
                "messages_from_me": messages_from_me,
                "messages_from_other": messages_from_other,
                "words_from_me": words_from_me if use_text_stats else None,
                "words_from_other": words_from_other if use_text_stats else None,
                "words_total": words_total if use_text_stats else None,
                "chars_from_me": chars_from_me if use_text_stats else None,
                "chars_from_other": chars_from_other if use_text_stats else None,
                "chars_total": chars_total if use_text_stats else None,
                "is_bot": "Да" if is_bot else "Нет",
                "is_verified": "Да" if is_verified else "Нет",
                "is_premium": "Да" if is_premium else "Нет",
                "is_scam": "Да" if is_scam else "Нет",
                "is_fake": "Да" if is_fake else "Нет",
                "is_restricted": "Да" if is_restricted else "Нет",
                "about": about or "",
                "common_chats_count": common_chats_count if common_chats_count is not None else "",
                "mutual_contact": "Да" if mutual_contact else "Нет",
                "contact": "Да" if contact else "Нет",
                "lang_code": lang_code or "",
                "processing_status": "Ок",
            }
        except Exception as e:
            self.logger.error(f"Ошибка при сборе данных личного чата {entity.id}: {e}")
            return None

    def _build_private_xlsx_rows(self) -> Tuple[List[str], List[List[Any]]]:
        """
        Формирует данные для листа с личными чатами.
        
        Returns:
            Заголовки и строки для XLSX
        """
        # Проверяем, нужно ли включать колонки со словами и буквами
        include_text_stats = bool(self.private_text_timeout_ids)
        
        headers = [
            "ID",
            "Имя",
            "Username",
            "Телефон",
            "Дата последнего сообщения",
            "Сообщений за 90 дней",
            "Среднее в день",
            "Среднее в неделю",
            "Среднее в месяц",
            "Сообщений за год",
            "Сообщений всего",
            "Сообщений от вас",
            "Сообщений от собеседника",
        ]
        
        # Добавляем колонки со словами и буквами только если есть ID в списке
        if include_text_stats:
            headers.extend([
                "Слов от вас",
                "Слов от собеседника",
                "Слов всего",
                "Букв от вас",
                "Букв от собеседника",
                "Букв всего",
            ])
        
        headers.extend([
            "Бот",
            "Верифицирован",
            "Premium",
            "Мошенник",
            "Фейк",
            "Ограничен",
            "О себе",
            "Общих чатов",
            "Взаимный контакт",
            "В контактах",
            "Время обработки (сек)",
            "Статус обработки",
        ])
        rows: List[List[Any]] = []
        sorted_chats = sorted(
            self.private_chats_data,
            key=lambda item: int(item.get("messages_total", 0) or 0),
            reverse=True,
        )
        for chat in sorted_chats:
            row = [
                str(chat.get("id", "")),
                str(chat.get("name", "")),
                str(chat.get("username", "")) if chat.get("username") else "",
                str(chat.get("phone", "")) if chat.get("phone") else "",
                str(chat.get("last_message_date", "")) if chat.get("last_message_date") else "",
                int(chat.get("messages_90", 0)),
                float(chat.get("avg_day", 0.0)),
                float(chat.get("avg_week", 0.0)),
                float(chat.get("avg_month", 0.0)),
                int(chat.get("messages_year", 0)),
                int(chat.get("messages_total", 0)),
                int(chat.get("messages_from_me", 0)),
                int(chat.get("messages_from_other", 0)),
            ]
            
            # Добавляем данные о словах и буквах только если нужно
            if include_text_stats:
                row.extend([
                    chat.get("words_from_me", "") if chat.get("words_from_me") is not None else "",
                    chat.get("words_from_other", "") if chat.get("words_from_other") is not None else "",
                    chat.get("words_total", "") if chat.get("words_total") is not None else "",
                    chat.get("chars_from_me", "") if chat.get("chars_from_me") is not None else "",
                    chat.get("chars_from_other", "") if chat.get("chars_from_other") is not None else "",
                    chat.get("chars_total", "") if chat.get("chars_total") is not None else "",
                ])
            
            row.extend([
                str(chat.get("is_bot", "")),
                str(chat.get("is_verified", "")),
                str(chat.get("is_premium", "")),
                str(chat.get("is_scam", "")),
                str(chat.get("is_fake", "")),
                str(chat.get("is_restricted", "")),
                str(chat.get("about", "")),
                int(chat.get("common_chats_count", 0)) if chat.get("common_chats_count") not in (None, "") else "",
                str(chat.get("mutual_contact", "")),
                str(chat.get("contact", "")),
                float(chat.get("processing_time", 0.0)),
                str(chat.get("processing_status", "")),
            ])
            rows.append(row)
        return headers, rows

    async def _build_basic_private_chat_info(
        self,
        entity: User,
        last_message_date: Optional[str],
        status: str,
    ) -> Dict[str, Any]:
        """
        Создает базовую запись о личном чате при таймауте/ошибке.
        
        Args:
            entity: Пользователь
            last_message_date: Дата последнего сообщения
            status: Статус обработки
        
        Returns:
            Базовый словарь данных личного чата
        """
        display_name = " ".join(
            part for part in [getattr(entity, "first_name", ""), getattr(entity, "last_name", "")] if part
        ).strip() or "Без имени"
        
        # Пытаемся получить полную информацию о пользователе для about/bio и common_chats_count
        about = None
        common_chats_count = None
        try:
            full_user_info = await self.client(
                functions.users.GetFullUserRequest(id=entity)
            )
            # Пробуем разные способы доступа к about и common_chats_count
            if full_user_info:
                # Способ 1: через full_user (основной способ для UserFull)
                if hasattr(full_user_info, "full_user"):
                    full_user = full_user_info.full_user
                    about = getattr(full_user, "about", None) or getattr(full_user, "bio", None)
                    common_chats_count = getattr(full_user, "common_chats_count", None)
                    if common_chats_count is not None:
                        self.logger.debug(
                            f"Получено 'Общих чатов' для пользователя {entity.id}: {common_chats_count} "
                            f"через full_user.common_chats_count "
                            f"[class: ChannelScanner | def: _build_basic_private_chat_info]"
                        )
                # Способ 2: напрямую из full_user_info
                if not about:
                    about = getattr(full_user_info, "about", None) or getattr(full_user_info, "bio", None)
                if common_chats_count is None:
                    common_chats_count = getattr(full_user_info, "common_chats_count", None)
                    if common_chats_count is not None:
                        self.logger.debug(
                            f"Получено 'Общих чатов' для пользователя {entity.id}: {common_chats_count} "
                            f"напрямую из full_user_info "
                            f"[class: ChannelScanner | def: _build_basic_private_chat_info]"
                        )
                # Способ 3: через users (если есть)
                if not about and hasattr(full_user_info, "users") and full_user_info.users:
                    for user_obj in full_user_info.users:
                        about = getattr(user_obj, "about", None) or getattr(user_obj, "bio", None)
                        if about:
                            break
                if common_chats_count is None:
                    self.logger.debug(
                        f"Не найдено 'Общих чатов' для пользователя {entity.id}, "
                        f"тип ответа: {type(full_user_info).__name__} "
                        f"[class: ChannelScanner | def: _build_basic_private_chat_info]"
                    )
        except Exception as e:
            self.logger.debug(
                f"Не удалось получить полную информацию о пользователе {entity.id} "
                f"в _build_basic_private_chat_info: {e} "
                f"[class: ChannelScanner | def: _build_basic_private_chat_info]"
            )
        
        # Если не получили через GetFullUserRequest, пробуем из базового объекта
        if not about:
            about = getattr(entity, "about", None) or getattr(entity, "bio", None)
        if common_chats_count is None:
            common_chats_count = getattr(entity, "common_chats_count", None)
        
        is_bot = getattr(entity, "bot", False) or getattr(entity, "is_bot", False)
        is_verified = getattr(entity, "verified", False) or getattr(entity, "is_verified", False)
        is_premium = getattr(entity, "premium", False) or getattr(entity, "is_premium", False)
        is_scam = getattr(entity, "scam", False) or getattr(entity, "is_scam", False)
        is_fake = getattr(entity, "fake", False) or getattr(entity, "is_fake", False)
        is_restricted = getattr(entity, "restricted", False) or getattr(entity, "is_restricted", False)
        mutual_contact = getattr(entity, "mutual_contact", False)
        contact = getattr(entity, "contact", False)
        lang_code = getattr(entity, "lang_code", None)
        return {
            "id": entity.id,
            "name": display_name,
            "username": getattr(entity, "username", None),
            "phone": getattr(entity, "phone", None),
            "last_message_date": last_message_date,
            "messages_90": 0,
            "avg_day": 0.0,
            "avg_week": 0.0,
            "avg_month": 0.0,
            "messages_year": 0,
            "messages_total": 0,
            "messages_from_me": 0,
            "messages_from_other": 0,
            "words_from_me": None,
            "words_from_other": None,
            "words_total": None,
            "chars_from_me": None,
            "chars_from_other": None,
            "chars_total": None,
            "is_bot": "Да" if is_bot else "Нет",
            "is_verified": "Да" if is_verified else "Нет",
            "is_premium": "Да" if is_premium else "Нет",
            "is_scam": "Да" if is_scam else "Нет",
            "is_fake": "Да" if is_fake else "Нет",
            "is_restricted": "Да" if is_restricted else "Нет",
            "about": about or "",
            "common_chats_count": common_chats_count if common_chats_count is not None else "",
            "mutual_contact": "Да" if mutual_contact else "Нет",
            "contact": "Да" if contact else "Нет",
            "lang_code": lang_code or "",
            "processing_time": 0.0,
            "processing_status": status,
        }
