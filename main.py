#!/usr/bin/env python3
"""
Скрипт получает данные с сервера для каждого браслета, сохраняет их в локальные JSON-файлы
и обновляет файл для TouchDesigner. При Ctrl+C создает бэкапы и завершает работу.
"""

import json
import os
import shutil
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

# ==========================
# Параметры
# ==========================
API_URL = "http://157.230.95.209:30003/get_ppg_data"
MY_TZ = timezone(timedelta(hours=3))
TIME_FETCH_SEC = 1         # Интервал запроса (сек)
FETCH_INTERVAL_SEC = 5     # Пауза между опросами (сек)

USE_FIXED_START = True
FIXED_START = "2024-11-07-16-00-31"  # Формат: %Y-%m-%d-%H-%M-%S

BRACELETS_FILE = "bracelets.json"
MEASUREMENTS_FILE = "measurements.json"
TD_DATA_FILE = "td_data.json"
BACKUP_DIR = "backup"

# ==========================
# Шаблоны по умолчанию
# ==========================
DEFAULT_BRACELETS = [
    {"swaid_id": "1330", "name": "Bracelet 1330"},
    {"swaid_id": "34", "name": "Bracelet 34"}
]
DEFAULT_MEASUREMENTS = []
DEFAULT_TD_DATA = {}

# ==========================
# Функции
# ==========================


def ensure_file(filename, default_data):
    """Создает файл с исходными данными, если он отсутствует."""
    os.makedirs(os.path.dirname(filename) or '.', exist_ok=True)
    if not os.path.exists(filename):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(default_data, f, indent=2)
        print(f"Создан файл: {filename}")


def backup_files():
    """Создает бэкапы файлов в папке BACKUP_DIR."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for filename in [BRACELETS_FILE, MEASUREMENTS_FILE, TD_DATA_FILE]:
        if os.path.exists(filename):
            base_name = os.path.splitext(filename)[0]
            backup_path = os.path.join(
                BACKUP_DIR, f"{base_name}_bp_{timestamp}.json")
            try:
                shutil.copy(filename, backup_path)
                print(f"Бэкап: {filename} -> {backup_path}")
            except (OSError, IOError) as e:
                print(f"Ошибка при копировании {filename}: {e}")
        else:
            print(f"Файл {filename} не найден, бэкап пропущен")


def fetch_data(dev_id, start_override=None):
    """
    Запрашивает данные для устройства dev_id с сервера.
    Аргументы:
        dev_id: ID устройства.
        start_override: Начальное время (datetime), если задано.
    Возвращает список измерений или пустой список при ошибке.
    """
    if start_override:
        start_time = start_override
        end_time = start_time + timedelta(seconds=TIME_FETCH_SEC)
    else:
        now = datetime.now(MY_TZ)
        start_time = now - timedelta(seconds=TIME_FETCH_SEC)
        end_time = now

    s_start = start_time.strftime("%Y-%m-%d-%H-%M-%S")
    s_end = end_time.strftime("%Y-%m-%d-%H-%M-%S")
    params = {"device_name": dev_id, "start": s_start, "end": s_end}

    try:
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()  # Проверка на HTTP-ошибки
        data = response.json()
    except requests.RequestException as e:
        print(f"[{dev_id}] Ошибка: Сервер недоступен ({e})")
        return []
    except json.JSONDecodeError as e:
        print(f"[{dev_id}] Ошибка разбора JSON: {e}")
        return []

    if data.get("message") == "No data found for the specified device.":
        return []
    if not (data.get("hr") or data.get("si")):
        print(f"[{dev_id}] Нет данных")
        return []

    measurements = [
        {
            "timestamp": ts.split(".")[0],
            "hr": hr,
            "si": si,
            "swaid_id": dev_id,
        }
        for hr, si, ts in zip(data.get("hr", []), data.get("si", []), data.get("time", []))
    ]
    return measurements


def main():
    """Основной цикл: сбор данных и обновление файлов."""
    print("Скрипт запущен.")

    # Загрузка списка браслетов
    try:
        with open(BRACELETS_FILE, "r", encoding="utf-8") as f:
            bracelets = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, PermissionError) as e:
        print(f"Ошибка загрузки {BRACELETS_FILE}: {e}")
        sys.exit(1)

    if not bracelets:
        print("Список браслетов пуст.")
        sys.exit(1)

    # Определение начального времени
    if USE_FIXED_START:
        try:
            current_start = datetime.strptime(
                FIXED_START, "%Y-%m-%d-%H-%M-%S").replace(tzinfo=MY_TZ)
        except ValueError as e:
            print(f"Ошибка формата FIXED_START: {e}")
            sys.exit(1)
    else:
        current_start = None

    # Загрузка существующих измерений один раз
    try:
        with open(MEASUREMENTS_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)
    except FileNotFoundError:
        print(f"{MEASUREMENTS_FILE} не найден, инициализируем пустой список")
        history = []
    except json.JSONDecodeError:
        print(
            f"Ошибка формата {MEASUREMENTS_FILE}, инициализируем пустой список")
        history = []

    # Основной цикл с многопоточностью
    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            all_measurements = []
            td_data = {}

            # Запуск параллельных запросов
            futures = {
                executor.submit(
                    fetch_data,
                    b.get("swaid_id"),
                    current_start if USE_FIXED_START else None
                ): b.get("swaid_id")
                for b in bracelets if b.get("swaid_id")
            }

            # Обработка результатов
            for future in as_completed(futures):
                device_id = futures[future]
                try:
                    result = future.result()
                    if result:
                        all_measurements.extend(result)
                        td_data[device_id] = result[-1]
                except TimeoutError as e:
                    print(f"[{device_id}] Превышено время ожидания: {e}")
                except Exception as e:  # pylint: disable=broad-exception-caught
                    print(f"[{device_id}] Неизвестная ошибка: {e}")

            # Обновление истории и файлов
            history.extend(all_measurements)
            with open(MEASUREMENTS_FILE, "w", encoding="utf-8") as f:
                json.dump(history, f, indent=2)
            with open(TD_DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(td_data, f, indent=2)

            # Вывод статистики
            print(
                f"[Опрошено] {len(bracelets)} устройств, {len(all_measurements)} новых записей")
            print("Ожидание следующего цикла...\n")

            # Обновление времени для фиксированного старта
            if USE_FIXED_START:
                current_start += timedelta(seconds=TIME_FETCH_SEC)
            time.sleep(FETCH_INTERVAL_SEC)


def signal_handler(_sig, _frame):
    """Обработчик Ctrl+C: создает бэкапы и завершает работу."""
    print("\nПолучен Ctrl+C. Создание бэкапов...")
    backup_files()
    os._exit(0)


# ==========================
# Запуск программы
# ==========================
if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    ensure_file(BRACELETS_FILE, DEFAULT_BRACELETS)
    ensure_file(MEASUREMENTS_FILE, DEFAULT_MEASUREMENTS)
    ensure_file(TD_DATA_FILE, DEFAULT_TD_DATA)
    try:
        main()
    except KeyboardInterrupt:
        signal_handler(None, None)
