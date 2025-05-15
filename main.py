#!/usr/bin/env python3
"""
Скрипт получает данные с сервера для каждого браслета, сохраняет их в локальные JSON-файлы
и обновляет файл для TouchDesigner. При Ctrl+C создает бэкапы и завершает работу.
"""

import argparse
import json
import os
import shutil
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import logging

import requests

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Параметры
API_URL = "http://157.230.95.209:30003/get_ppg_data"
MY_TZ = timezone(timedelta(hours=3))
TIME_FETCH_SEC = 60         # Интервал запроса (сек)
FETCH_INTERVAL_SEC = 5     # Пауза между опросами (сек)

USE_FIXED_START = False    # По умолчанию False, используем текущее время
FIXED_START = "2025-05-15-16-23-00"  # Формат: %Y-%m-%d-%H-%M-%S

BRACELETS_FILE = "bracelets.json"
MEASUREMENTS_FILE = "measurements.json"
TD_DATA_FILE = "td_data.json"
BACKUP_DIR = "backup"

# Шаблоны по умолчанию
DEFAULT_BRACELETS = [
    {"mac_address": "CE:D6:AD:45:ED:75", "name": "Bracelet 1"},
]
DEFAULT_MEASUREMENTS = []
DEFAULT_TD_DATA = {}


def ensure_file(filename, default_data):
    """Создает файл с исходными данными, если он отсутствует."""
    os.makedirs(os.path.dirname(filename) or '.', exist_ok=True)
    if not os.path.exists(filename):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(default_data, f, indent=2)
        logger.info("Создан файл: %s", filename)
    else:
        logger.info("Файл %s уже существует", filename)


def backup_files():
    """Создает бэкапы файлов в папке BACKUP_DIR."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for filename in [BRACELETS_FILE, MEASUREMENTS_FILE, TD_DATA_FILE]:
        if os.path.exists(filename):
            base_name = os.path.splitext(filename)[0]
            backup_filename = f"{base_name}_bp_{timestamp}.json"
            backup_path = os.path.join(BACKUP_DIR, backup_filename)
            try:
                shutil.copy(filename, backup_path)
                logger.info("Бэкап: %s -> %s", filename, backup_path)
            except (OSError, IOError) as e:
                logger.error("Ошибка при копировании %s: %s", filename, e)
        else:
            logger.warning("Файл %s не найден, бэкап пропущен", filename)


def fetch_data(session_name, mac_address, start_override=None):
    """
    Запрашивает данные для устройства с сервера.
    """
    device_name = f"{session_name}_{mac_address}"
    if not mac_address:
        logger.warning(
            "Пропущен запрос для %s: отсутствует MAC-адрес", device_name)
        return []

    if start_override:
        start_time = start_override
        end_time = start_time + timedelta(seconds=TIME_FETCH_SEC)
    else:
        now = datetime.now(MY_TZ)
        start_time = now - timedelta(seconds=TIME_FETCH_SEC)
        end_time = now

    s_start = start_time.strftime("%Y-%m-%d-%H-%M-%S")
    s_end = end_time.strftime("%Y-%m-%d-%H-%M-%S")
    params = {"device_name": device_name, "start": s_start, "end": s_end}

    logger.info("[%s] Отправка запроса на %s с параметрами: %s",
                device_name, API_URL, params)
    try:
        response = requests.get(API_URL, params=params, timeout=10)
        logger.info("[%s] Получен ответ с кодом состояния: %s",
                    device_name, response.status_code)
        logger.debug("[%s] Полный URL запроса: %s", device_name, response.url)

        response.raise_for_status()
        data = response.json()
        logger.debug("[%s] Полученные данные: %s",
                     device_name, json.dumps(data, indent=2))
    except requests.RequestException as e:
        logger.error("[%s] Ошибка запроса: %s", device_name, e)
        return []
    except json.JSONDecodeError as e:
        logger.error("[%s] Ошибка разбора JSON: %s", device_name, e)
        return []

    if data.get("message") == "No data found for the specified device.":
        logger.info("[%s] Данные не найдены для устройства", device_name)
        return []
    if not any(data.get(key) for key in ["hr", "lf_hf_ratio", "rmssd", "sdrr", "si"]):
        logger.warning("[%s] Получен пустой набор данных", device_name)
        return []

    measurements = [
        {
            "timestamp": ts,
            "hr": hr,
            "lf_hf_ratio": lf_hf,
            "rmssd": rmssd,
            "sdrr": sdrr,
            "si": si,
            "device_name": device_name,
        }
        for hr, lf_hf, rmssd, sdrr, si, ts in zip(
            data.get("hr", []),
            data.get("lf_hf_ratio", []),
            data.get("rmssd", []),
            data.get("sdrr", []),
            data.get("si", []),
            data.get("time", [])
        )
    ]
    logger.info("[%s] Успешно получено %d измерений",
                device_name, len(measurements))
    return measurements


def load_bracelets():
    """Загружает список браслетов из файла."""
    try:
        with open(BRACELETS_FILE, "r", encoding="utf-8") as f:
            bracelets = json.load(f)
        logger.info("Успешно загружено %d браслетов из %s:",
                    len(bracelets), BRACELETS_FILE)
        for b in bracelets:
            logger.info(" - %s (%s)", b.get('name', 'Без имени'),
                        b.get('mac_address', 'Без MAC'))
        return bracelets
    except (FileNotFoundError, json.JSONDecodeError, PermissionError) as e:
        logger.error("Ошибка загрузки %s: %s", BRACELETS_FILE, e)
        sys.exit(1)


def fetch_and_process_data(session_name, bracelets, current_start):
    """Выполняет запросы к серверу и обрабатывает данные."""
    all_measurements = []
    td_data = {}
    max_workers = min(len(bracelets), 5)  # Оптимизация потоков

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_data, session_name, b.get("mac_address"), current_start): b
            for b in bracelets if b.get("mac_address")
        }

        for future in as_completed(futures):
            bracelet = futures[future]
            device_name = f"{session_name}_{bracelet.get('mac_address')}"
            try:
                result = future.result()
                if result:
                    all_measurements.extend(result)
                    td_data[device_name] = result[-1]
            except Exception as e:
                logger.error("[%s] Ошибка обработки: %s", device_name, e)

    return all_measurements, td_data


def save_data(history, new_measurements, td_data):
    """Сохраняет данные в файлы."""
    if new_measurements:
        history.extend(new_measurements)
        with open(MEASUREMENTS_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2)
        logger.info("Добавлено %d новых записей в %s",
                    len(new_measurements), MEASUREMENTS_FILE)

    with open(TD_DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(td_data, f, indent=2)


def main():
    """Основной цикл программы."""
    parser = argparse.ArgumentParser(
        description="Скрипт для получения данных с сервера.")
    parser.add_argument(
        "--session_name", help="Название сессии (необязательно)")
    args = parser.parse_args()

    session_name = args.session_name or input(
        "Введите название сессии: ").strip()
    if not session_name:
        logger.error("Название сессии не может быть пустым")
        sys.exit(1)

    logger.info("Скрипт запущен с сессией: %s", session_name)

    bracelets = load_bracelets()
    if not bracelets:
        logger.error("Список браслетов пуст")
        sys.exit(1)

    # Начальное время
    current_start = None
    if USE_FIXED_START:
        try:
            current_start = datetime.strptime(
                FIXED_START, "%Y-%m-%d-%H-%M-%S").replace(tzinfo=MY_TZ)
            logger.info(
                "Используется фиксированное начальное время: %s", FIXED_START)
        except ValueError as e:
            logger.error("Ошибка формата FIXED_START: %s", e)
            sys.exit(1)
    else:
        logger.info("Используется текущее время для запросов")

    # Загрузка истории
    try:
        with open(MEASUREMENTS_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)
        logger.info("Загружено %d существующих измерений", len(history))
    except (FileNotFoundError, json.JSONDecodeError):
        history = []
        logger.info(
            "Файл %s не найден, инициализирован пустой список", MEASUREMENTS_FILE)

    # Основной цикл
    while True:
        logger.info(
            "Начинаем новый цикл запросов для %d браслетов", len(bracelets))
        new_measurements, td_data = fetch_and_process_data(
            session_name, bracelets, current_start)
        save_data(history, new_measurements, td_data)

        logger.info("Итог цикла: получено %d новых записей",
                    len(new_measurements))
        if USE_FIXED_START and current_start:
            current_start += timedelta(seconds=TIME_FETCH_SEC)
        time.sleep(FETCH_INTERVAL_SEC)


def signal_handler(_sig, _frame):
    """Обработчик Ctrl+C."""
    logger.info("Получен Ctrl+C. Создание бэкапов...")
    backup_files()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    ensure_file(BRACELETS_FILE, DEFAULT_BRACELETS)
    ensure_file(MEASUREMENTS_FILE, DEFAULT_MEASUREMENTS)
    ensure_file(TD_DATA_FILE, DEFAULT_TD_DATA)
    main()
