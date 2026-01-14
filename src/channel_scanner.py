"""
Модуль для сканирования каналов Telegram.

Содержит класс ChannelScanner для получения информации о всех каналах,
на которые подписан пользователь или в которых он является участником.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from telethon import TelegramClient
from telethon.errors import ChatAdminRequiredError, FloodWaitError
from telethon.tl.types import Channel, Chat, User

from logger_config import get_logger


class ChannelScanner:
    """
    Класс для сканирования и сбора информации о каналах Telegram.
    
    Позволяет получить полный список всех каналов, групп и супергрупп,
    в которых пользователь является участником, с детальной информацией.
    """
    
    def __init__(self, client: TelegramClient) -> None:
        """
        Инициализация сканера каналов.
        
        Args:
            client: Экземпляр TelegramClient для работы с API
        """
        self.client = client
        self.logger = get_logger("channel_scanner")
        self.channels_data: List[Dict[str, Any]] = []
    
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
            try:
                if entity.broadcast:
                    # Для каналов используем get_participants
                    participants = await self.client.get_participants(entity, limit=1)
                    channel_data["participants_count"] = getattr(entity, 'participants_count', None)
                else:
                    # Для групп и супергрупп
                    full_chat = await self.client.get_entity(entity)
                    if hasattr(full_chat, 'participants_count'):
                        channel_data["participants_count"] = full_chat.participants_count
                    else:
                        # Пытаемся получить через get_participants
                        try:
                            participants = await self.client.get_participants(entity, limit=1)
                            # Получаем общее количество через итератор
                            count = 0
                            async for _ in self.client.iter_participants(entity):
                                count += 1
                                if count >= 10000:  # Ограничение для производительности
                                    break
                            channel_data["participants_count"] = count if count < 10000 else ">10000"
                        except Exception as e:
                            self.logger.debug(f"Не удалось получить количество участников: {e}")
                            channel_data["participants_count"] = None
            except ChatAdminRequiredError:
                self.logger.debug("Требуются права администратора для получения количества участников")
                channel_data["participants_count"] = "Требуются права администратора"
            except Exception as e:
                self.logger.debug(f"Ошибка при получении количества участников: {e}")
                channel_data["participants_count"] = None
            
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
            
            self.logger.info(f"Успешно получена информация о канале: {channel_data['title']}")
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
            
            # Получаем информацию о каждом канале
            for i, channel in enumerate(channels_and_groups, 1):
                self.logger.info(f"Обработка канала {i}/{len(channels_and_groups)}: {channel.title}")
                channel_info = await self.get_channel_info(channel)
                if channel_info:
                    self.channels_data.append(channel_info)
                    # Небольшая задержка, чтобы не превысить лимиты API
                    await asyncio.sleep(1)
            
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
            output_path = Path(__file__).parent.parent / filename
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
            output_path = Path(__file__).parent.parent / filename
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
        rows: List[List[str]] = []
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
            if participants_value is None:
                participants_text = "Неизвестно"
            else:
                participants_text = str(participants_value)
            rows.append(
                [
                    str(channel.get("id", "")),
                    str(channel.get("title", "")),
                    str(channel.get("username", "")),
                    channel_type,
                    "Да" if channel.get("is_public") else "Нет",
                    participants_text,
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

        for row_idx, row in enumerate(rows, start=2):
            is_zebra = row_idx % 2 == 0
            for col_idx, value in enumerate(row, 1):
                cell = worksheet.cell(row=row_idx, column=col_idx, value=value)
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
            output_path = Path(__file__).parent.parent / filename
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
