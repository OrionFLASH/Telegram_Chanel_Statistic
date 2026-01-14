"""
Модуль для сканирования каналов Telegram.

Содержит класс ChannelScanner для получения информации о всех каналах,
на которые подписан пользователь или в которых он является участником.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
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
    ) -> None:
        """
        Инициализация сканера каналов.
        
        Args:
            client: Экземпляр TelegramClient для работы с API
            concurrency: Максимальное количество одновременных запросов
            request_delay: Небольшая задержка между запросами для снижения нагрузки
            unsubscribe_ids: Набор ID каналов/групп для авто-отписки
        """
        self.client = client
        self.logger = get_logger("channel_scanner")
        self.channels_data: List[Dict[str, Any]] = []
        self.concurrency = max(1, concurrency)
        self.request_delay = max(0.0, request_delay)
        self.output_dir = Path(__file__).parent.parent / "OUT"
        self.output_dir.mkdir(exist_ok=True)
        self.unsubscribe_ids = unsubscribe_ids or set()

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
    
    async def _fetch_participants_count(self, entity: Channel) -> Optional[Any]:
        """
        Пытается получить количество участников разными способами.
        
        Args:
            entity: Объект канала/группы из Telegram API
        
        Returns:
            Количество участников или None
        """
        if hasattr(entity, "participants_count") and entity.participants_count:
            return entity.participants_count
        try:
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

    async def _leave_channel_or_chat(self, entity: Channel) -> None:
        """
        Выполняет отписку от канала или выход из группы.
        
        Args:
            entity: Сущность канала/группы
        """
        try:
            if isinstance(entity, Channel):
                await self.client(functions.channels.LeaveChannelRequest(channel=entity))
            elif isinstance(entity, Chat):
                await self.client(functions.messages.DeleteChatUser(chat_id=entity.id, user_id="me"))
        except Exception as e:
            self.logger.error(f"Ошибка при попытке отписки от {entity.id}: {e}")

    async def get_channel_info(self, entity: Channel) -> Optional[Dict[str, Any]]:
        """
        Получает детальную информацию о канале.
        
        Args:
            entity: Объект канала из Telegram API
        
        Returns:
            Словарь с информацией о канале или None в случае ошибки
        """
        try:
            self.logger.debug(f"Получение информации о канале: {entity.title}")
            
            # Получаем полную информацию о канале
            full_info = await self.client.get_entity(entity)
            
            # Базовые данные канала
            channel_data: Dict[str, Any] = {
                "id": entity.id,
                "title": entity.title or "Без названия",
                "username": entity.username or "Нет username",
                "is_broadcast": entity.broadcast,  # True для каналов, False для групп
                "is_megagroup": entity.megagroup,  # True для супергрупп
                "is_gigagroup": getattr(entity, 'gigagroup', False),
                "access_hash": str(entity.access_hash) if hasattr(entity, 'access_hash') else None,
                "scanned_at": datetime.now().isoformat()
            }
            
            # Пытаемся получить количество участников
            participants_count = await self._fetch_participants_count(entity)
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
                        self.logger.debug(f"Не удалось получить количество участников через iter_participants: {e}")
            channel_data["participants_count"] = participants_count
            
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
            for dialog in dialogs:
                entity = dialog.entity
                # Проверяем, является ли это каналом или группой
                if isinstance(entity, Channel):
                    channels_and_groups.append(entity)
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
                    channel_info = await self.get_channel_info(entity)
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
                            await self._leave_channel_or_chat(entity)
                    # Небольшая задержка между запросами
                    if self.request_delay:
                        await asyncio.sleep(self.request_delay)
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
            "Дата создания",
            "Дата сканирования",
        ]
        rows: List[List[Any]] = []
        for channel in self.channels_data:
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
                    str(channel.get("created_date", "")) if channel.get("created_date") else "",
                    str(channel.get("scanned_at", "")),
                ]
            )
        return headers, rows

    def _apply_xlsx_styles(self, worksheet, headers: List[str], rows: List[List[str]]) -> None:
        """
        Применяет форматирование к листу XLSX для удобного просмотра.
        
        Args:
            worksheet: Лист Excel
            headers: Список заголовков
            rows: Строки данных
        """
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="2F75B5", end_color="2F75B5", fill_type="solid")
        data_alignment = Alignment(vertical="top", wrap_text=True)
        numeric_alignment = Alignment(horizontal="right", vertical="top")
        thin_border = Border(
            left=Side(style="thin", color="D9D9D9"),
            right=Side(style="thin", color="D9D9D9"),
            top=Side(style="thin", color="D9D9D9"),
            bottom=Side(style="thin", color="D9D9D9"),
        )
        zebra_fill = PatternFill(start_color="F3F6FA", end_color="F3F6FA", fill_type="solid")

        for col_idx, header in enumerate(headers, 1):
            cell = worksheet.cell(row=1, column=col_idx, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border = thin_border

        participants_col_idx = headers.index("Участников") + 1
        for row_idx, row in enumerate(rows, start=2):
            is_zebra = row_idx % 2 == 0
            for col_idx, value in enumerate(row, 1):
                cell = worksheet.cell(row=row_idx, column=col_idx, value=value)
                if col_idx == participants_col_idx:
                    cell.alignment = numeric_alignment
                    if isinstance(value, (int, float)):
                        cell.number_format = "#,##0"
                else:
                    cell.alignment = data_alignment
                cell.border = thin_border
                if is_zebra:
                    cell.fill = zebra_fill

        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{len(rows) + 1}"

        for col_idx, header in enumerate(headers, 1):
            max_len = len(header)
            for row in rows:
                value = row[col_idx - 1]
                if value:
                    max_len = max(max_len, len(str(value)))
            adjusted = min(max(max_len + 2, 12), 60)
            worksheet.column_dimensions[get_column_letter(col_idx)].width = adjusted

    def save_to_xlsx(self, filename: str = "channels_data.xlsx") -> None:
        """
        Сохраняет данные о каналах в XLSX файл с удобным форматированием.
        
        Args:
            filename: Имя файла для сохранения
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            output_path = self.output_dir / self._append_timestamp(filename, timestamp)
            workbook = Workbook()
            worksheet = workbook.active
            worksheet.title = "Каналы"

            headers, rows = self._build_xlsx_rows()
            worksheet.append(headers)
            for row in rows:
                worksheet.append(row)

            self._apply_xlsx_styles(worksheet, headers, rows)
            workbook.save(output_path)
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
