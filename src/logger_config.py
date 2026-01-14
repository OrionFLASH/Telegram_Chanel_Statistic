"""
Модуль для настройки логирования работы программы.

Содержит функции для создания и настройки логгеров с двумя уровнями:
- INFO: основные события выполнения
- DEBUG: отладочная и диагностическая информация
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional


def setup_logger(name: str, log_dir: str = "log") -> logging.Logger:
    """
    Создает и настраивает логгер с двумя уровнями детализации.
    
    Args:
        name: Имя логгера (обычно имя модуля или тема)
        log_dir: Директория для хранения логов (по умолчанию 'log')
    
    Returns:
        Настроенный объект логгера
    """
    # Создаем директорию для логов, если её нет
    log_path = Path(__file__).parent.parent / log_dir
    log_path.mkdir(exist_ok=True)
    
    # Формируем имя файла по шаблону: Уровень_тема_годмесяцдень_час.log
    timestamp = datetime.now().strftime("%Y%m%d_%H")
    info_log_file = log_path / f"INFO_{name}_{timestamp}.log"
    debug_log_file = log_path / f"DEBUG_{name}_{timestamp}.log"
    
    # Создаем логгер
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Устанавливаем максимальный уровень
    
    # Очищаем существующие обработчики
    logger.handlers.clear()
    
    # Формат для DEBUG логов (с указанием класса и функции)
    debug_formatter = logging.Formatter(
        '%(asctime)s - [%(levelname)s] - %(message)s [class: %(name)s | def: %(funcName)s]',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Формат для INFO логов (более простой)
    info_formatter = logging.Formatter(
        '%(asctime)s - [%(levelname)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Обработчик для INFO уровня
    info_handler = logging.FileHandler(info_log_file, encoding='utf-8')
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(info_formatter)
    
    # Обработчик для DEBUG уровня
    debug_handler = logging.FileHandler(debug_log_file, encoding='utf-8')
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(debug_formatter)
    
    # Обработчик для консоли (только INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(info_formatter)
    
    # Добавляем обработчики к логгеру
    logger.addHandler(info_handler)
    logger.addHandler(debug_handler)
    logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Получает существующий логгер или создает новый.
    
    Args:
        name: Имя логгера
    
    Returns:
        Объект логгера
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        return setup_logger(name)
    return logger
