#!/usr/bin/env python3
"""
Скрипт получает данные с сервера для браслетов, сохраняет их в JSON и обновляет файл для TouchDesigner.
При Ctrl+C создает бэкапы.
"""

import argparse
import json
import os
import shutil
import signal
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Optional, TypedDict
import logging
from tabulate import tabulate
import urllib.parse

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
TIME_FETCH_SEC = 300
FETCH_INTERVAL_SEC = 5

USE_FIXED_START = False
FIXED_START = "2025-05-15-16-23-00"

BRACELETS_FILE = "bracelets.json"
MEASUREMENTS_FILE = "measurements.json"
TD_DATA_FILE = "td_data.json"
BACKUP_DIR = "backup"

# Шаблоны по умолчанию
DEFAULT_BRACELETS = [
    {"mac_address": "CE:D6:AD:45:ED:75", "name": "Bracelet 1"}]
DEFAULT_MEASUREMENTS: List[Dict] = []
DEFAULT_TD_DATA: Dict = {}

# Тип для измерений


class Measurement(TypedDict):
    timestamp: str
    hr: Optional[int]
    lf_hf_ratio: Optional[float]
    rmssd: Optional[int]
    sdrr: Optional[int]
    si: Optional[float]
    device_name: str


def ensure_file(filename: str, default_data: List | Dict) -> None:
    """Создает файл с данными, если он отсутствует."""
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
    if not os.path.exists(filename):
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(default_data, file, indent=2)
        logger.info("Создан файл: %s", filename)
    else:
        logger.info("Файл %s существует", filename)


def backup_files() -> None:
    """Создает бэкапы файлов в BACKUP_DIR."""
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
            except (OSError, IOError) as error:
                logger.error("Ошибка копирования %s: %s", filename, error)
        else:
            logger.warning("Файл %s не найден", filename)


def handle_fetch_error(
    device_name: str,
    error: Exception,
    start_time: datetime,
    end_time: Optional[datetime]
) -> Tuple[List[Measurement], datetime, Optional[datetime]]:
    """Обрабатывает ошибки запроса."""
    logger.error("[%s] Ошибка: %s", device_name, error)
    return [], start_time, end_time


def fetch_data(
    session_name: str,
    mac_address: str,
    start_override: Optional[datetime] = None
) -> Tuple[List[Measurement], datetime, Optional[datetime]]:
    """
    Запрашивает данные с сервера.
    Возвращает: (measurements, start_request_time, end_request_time)
    """
    device_name = f"{session_name}_{mac_address}"
    start_request_time = datetime.now(MY_TZ)
    end_request_time = None

    if not mac_address:
        logger.warning("Пропущен запрос для %s: нет MAC", device_name)
        return [], start_request_time, end_request_time

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

    # Формируем полный URL для лога
    query_string = urllib.parse.urlencode(params)
    full_url = f"{API_URL}?{query_string}"
    logger.info("[%s] Запрос: %s", device_name, full_url)
    try:
        response = requests.get(API_URL, params=params, timeout=10)
        end_request_time = datetime.now(MY_TZ)
        logger.info("[%s] Код ответа: %s", device_name, response.status_code)
        logger.debug("[%s] URL: %s", device_name, response.url)

        response.raise_for_status()
        data = response.json()
        logger.debug("[%s] Данные: %s", device_name,
                     json.dumps(data, indent=2))
    except requests.RequestException as error:
        return handle_fetch_error(device_name, error, start_request_time, end_request_time)
    except json.JSONDecodeError as error:
        return handle_fetch_error(device_name, error, start_request_time, end_request_time)

    if data.get("message") == "No data found for the specified device.":
        logger.info("[%s] Данные не найдены", device_name)
        return [], start_request_time, end_request_time
    if not any(data.get(key) for key in ["hr", "lf_hf_ratio", "rmssd", "sdrr", "si"]):
        logger.warning("[%s] Пустой набор данных", device_name)
        return [], start_request_time, end_request_time

    measurements: List[Measurement] = [
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
    logger.info("[%s] Получено %d измерений", device_name, len(measurements))
    return measurements, start_request_time, end_request_time


def load_bracelets() -> List[Dict]:
    """Загружает список браслетов из файла."""
    try:
        with open(BRACELETS_FILE, "r", encoding="utf-8") as file:
            bracelets = json.load(file)
        logger.info("Загружено %d браслетов из %s",
                    len(bracelets), BRACELETS_FILE)
        for bracelet in bracelets:
            logger.info(
                " - %s (%s)",
                bracelet.get("name", "Без имени"),
                bracelet.get("mac_address", "Без MAC")
            )
        return bracelets
    except (FileNotFoundError, json.JSONDecodeError, PermissionError) as error:
        logger.error("Ошибка загрузки %s: %s", BRACELETS_FILE, error)
        sys.exit(1)


def format_table(
    measurements: List[Measurement],
    s_start: str,
    s_end: str,
    end_request_time: Optional[datetime]
) -> str:
    """Форматирует данные в таблицу с адаптивным количеством строк."""
    headers = ["Mark", "Time", "Device", "HR", "LF/HF", "RMSSD", "SDRR", "SI"]
    table = []

    # Start row: server start time
    table.append(["Start", s_start, "", "", "", "", "", ""])

    # Measurement rows: all measurements with sequential numbers
    for idx, measurement in enumerate(measurements, 1):
        table.append([
            str(idx),
            measurement["timestamp"],
            measurement["device_name"][:20],
            measurement["hr"] or "-",
            measurement["lf_hf_ratio"] or "-",
            measurement["rmssd"] or "-",
            measurement["sdrr"] or "-",
            measurement["si"] or "-"
        ])

    # End row: server end time
    table.append(["End", s_end, "", "", "", "", "", ""])

    # Now row: response receipt time
    now_time = end_request_time.strftime(
        "%Y-%m-%d %H:%M:%S") if end_request_time else "-"
    table.append(["Now", now_time, "", "", "", "", "", ""])

    return tabulate(
        table,
        headers=headers,
        tablefmt="simple",
        maxcolwidths=[8, 20, 20, 10, 10, 10, 10, 10]
    )


def fetch_and_process_data(
    session_name: str,
    bracelets: List[Dict],
    current_start: Optional[datetime],
    executor: ThreadPoolExecutor
) -> Tuple[List[Measurement], Dict, Optional[datetime], Optional[datetime]]:
    """Выполняет запросы к серверу."""
    all_measurements: List[Measurement] = []
    td_data: Dict = {}
    earliest_start = None
    latest_end = None

    logger.info("Запуск пула для %d браслетов", len(bracelets))
    futures = {
        executor.submit(fetch_data, session_name, b.get("mac_address", ""), current_start): b
        for b in bracelets if b.get("mac_address")
    }  # pylint: disable=cell-var-from-loop

    for future in as_completed(futures):
        bracelet = futures[future]
        device_name = f"{session_name}_{bracelet.get('mac_address')}"
        try:
            measurements, start_time, end_time = future.result()
            if measurements:
                all_measurements.extend(measurements)
                td_data[device_name] = measurements[-1]
                if earliest_start is None or (start_time and start_time < earliest_start):
                    earliest_start = start_time
                if latest_end is None or (end_time and end_time > latest_end):
                    latest_end = end_time
        except Exception as error:
            logger.error(
                "[%s] Ошибка обработки: %s\n%s",
                device_name,
                error,
                "".join(traceback.format_tb(error.__traceback__))
            )

    return all_measurements, td_data, earliest_start, latest_end


def save_data(
    history: List[Measurement],
    new_measurements: List[Measurement],
    td_data: Dict,
    s_start: str,
    s_end: str,
    end_request_time: Optional[datetime]
) -> None:
    """Сохраняет данные и выводит таблицу."""
    if new_measurements:
        history.extend(new_measurements)
        with open(MEASUREMENTS_FILE, "w", encoding="utf-8") as file:
            json.dump(history, file, indent=2)
        logger.info("Добавлено %d записей в %s", len(
            new_measurements), MEASUREMENTS_FILE)
        table = format_table(new_measurements, s_start,
                             s_end, end_request_time)
        print(f"\n{table}\n")
    else:
        logger.warning("Нет новых измерений, таблица не выведена")

    with open(TD_DATA_FILE, "w", encoding="utf-8") as file:
        json.dump(td_data, file, indent=2)


def main() -> None:
    """Основной цикл программы."""
    parser = argparse.ArgumentParser(description="Получение данных с сервера")
    parser.add_argument("--session_name", help="Название сессии")
    args = parser.parse_args()

    session_name = args.session_name or input(
        "Введите название сессии: ").strip()
    if not session_name:
        logger.error("Название сессии не задано")
        sys.exit(1)

    logger.info("Запуск с сессией: %s", session_name)

    bracelets = load_bracelets()
    if not bracelets:
        logger.error("Список браслетов пуст")
        sys.exit(1)

    current_start = None
    if USE_FIXED_START:
        try:
            current_start = datetime.strptime(
                FIXED_START, "%Y-%m-%d-%H-%M-%S").replace(tzinfo=MY_TZ)
            logger.info("Фиксированное время: %s", FIXED_START)
        except ValueError as error:
            logger.error("Ошибка формата FIXED_START: %s", error)
            sys.exit(1)
    else:
        logger.info("Используется текущее время")

    try:
        with open(MEASUREMENTS_FILE, "r", encoding="utf-8") as file:
            history: List[Measurement] = json.load(file)
        logger.info("Загружено %d измерений", len(history))
    except (FileNotFoundError, json.JSONDecodeError):
        history: List[Measurement] = []
        logger.info("Файл %s не найден, создан пустой", MEASUREMENTS_FILE)

    max_workers = min(len(bracelets), 5)
    logger.info("Пул потоков: %d рабочих", max_workers)
    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        while True:
            logger.info("Новый цикл для %d браслетов", len(bracelets))
            new_measurements, td_data, start_time, end_time = fetch_and_process_data(
                session_name, bracelets, current_start, executor
            )
            # Pass server request times and response time to save_data
            s_start = start_time.strftime(
                "%Y-%m-%d-%H-%M-%S") if start_time else "-"
            s_end = end_time.strftime("%Y-%m-%d-%H-%M-%S") if end_time else "-"
            save_data(history, new_measurements,
                      td_data, s_start, s_end, end_time)

            logger.info("Цикл завершен: %d записей", len(new_measurements))
            print("")
            if USE_FIXED_START and current_start:
                current_start += timedelta(seconds=TIME_FETCH_SEC)
            time.sleep(FETCH_INTERVAL_SEC)
    except KeyboardInterrupt:
        logger.info("Завершение: закрытие пула")
        executor.shutdown(wait=True)
        raise
    finally:
        logger.info("Закрытие пула потоков")
        executor.shutdown(wait=True)


def signal_handler(_sig: int, _frame: Optional[object]) -> None:
    """Обработчик Ctrl+C."""
    logger.info("Ctrl+C: создание бэкапов")
    backup_files()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    ensure_file(BRACELETS_FILE, DEFAULT_BRACELETS)
    ensure_file(MEASUREMENTS_FILE, DEFAULT_MEASUREMENTS)
    ensure_file(TD_DATA_FILE, DEFAULT_TD_DATA)
    try:
        main()
    except Exception as error:
        logger.error("Критическая ошибка: %s", error)
        backup_files()
        sys.exit(1)
