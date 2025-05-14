#!/usr/bin/env python3
"""
Скрипт получает данные с сервера для каждого браслета, сохраняет их в локальные JSON-файлы
и обновляет файл для TouchDesigner. При Ctrl+C происходит бэкап и корректное завершение.
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
FIXED_START = "2024-11-07-16-00-31"  # формат: %Y-%m-%d-%H-%M-%S

BRACELETS_FILE = "bracelets.json"
MEASUREMENTS_FILE = "measurements.json"
TD_DATA_FILE = "td_data.json"
BACKUP_DIR = "backup"

# ==========================
# Шаблоны по умолчанию
# ==========================
default_bracelets = [
    {"swaid_id": "1330", "name": "Bracelet 1330"},
    {"swaid_id": "34", "name": "Bracelet 34"}
]
default_measurements = []
default_td_data = {}


def ensure_file(filename, default_data):
    """Создает файл с исходными данными, если он отсутствует."""
    if not os.path.exists(filename):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(default_data, f, indent=2)
        print(f"Создан {filename}")


def backup_files():
    """Создает бэкап файлов в папке BACKUP_DIR."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    for fn in [BRACELETS_FILE, MEASUREMENTS_FILE, TD_DATA_FILE]:
        bp = os.path.join(
            BACKUP_DIR, f"{os.path.splitext(fn)[0]}_bp_{ts}.json")
        try:
            shutil.copy(fn, bp)
            print(f"Бэкап {fn} -> {bp}")
        except Exception as e:
            print(f"Бэкап {fn} ошибка: {e}")


def fetch_data(dev_id, start_override=None):
    """
    Запрашивает данные для dev_id.
    Если start_override задан (datetime), используется интервал
    [start_override, start_override + TIME_FETCH_SEC].
    Иначе интервал вычисляется как [now - TIME_FETCH_SEC, now].
    Выводит:
      Q: URL запроса
      R: ответ JSON
    Возвращает список измерений или [].
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
        resp = requests.get(API_URL, params=params, timeout=10)
    except requests.exceptions.RequestException:
        print(f"[{dev_id}] Ошибка: Сервер недоступен")
        return []

    print(f"[{dev_id}] Q: {resp.url}")
    try:
        data = resp.json()
    except json.JSONDecodeError as e:
        print(f"[{dev_id}] JSON err: {e}")
        return []

    print(f"[{dev_id}] R: {data}")

    if data.get("message") == "No data found for the specified device.":
        return []
    if not data.get("hr") and not data.get("si"):
        print(f"[{dev_id}] Нет данных")
        return []

    measurements = []
    for hr, si, ts in zip(data.get("hr", []), data.get("si", []), data.get("time", [])):
        measurements.append({
            "timestamp": ts.split(".")[0],
            "hr": hr,
            "si": si,
            "swaid_id": dev_id,
        })
    return measurements


def main():
    """Основной цикл: сбор данных и обновление файлов."""
    print("Старт.")

    try:
        with open(BRACELETS_FILE, "r", encoding="utf-8") as f:
            bracelets = json.load(f)
    except Exception as e:
        print(f"Ошибка {BRACELETS_FILE}: {e}")
        sys.exit(1)

    if not bracelets:
        print("Список браслетов пуст.")
        sys.exit(1)

    if USE_FIXED_START:
        try:
            current_start = datetime.strptime(
                FIXED_START, "%Y-%m-%d-%H-%M-%S").replace(tzinfo=MY_TZ)
        except ValueError as e:
            print(f"FIXED_START ошибка: {e}")
            sys.exit(1)
    else:
        current_start = None

    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            all_meas = []
            td_data = {}

            futures = {
                executor.submit(
                    fetch_data,
                    b.get("swaid_id"),
                    current_start if USE_FIXED_START else None
                ): b.get("swaid_id")
                for b in bracelets if b.get("swaid_id")
            }
            for fut in as_completed(futures):
                dev = futures[fut]
                try:
                    res = fut.result()
                except Exception as ex:
                    print(f"[{dev}] exc: {ex}")
                    continue
                if res:
                    all_meas.extend(res)
                    td_data[dev] = res[-1]

            try:
                with open(MEASUREMENTS_FILE, "r", encoding="utf-8") as f:
                    hist = json.load(f)
            except Exception:
                hist = []
            hist.extend(all_meas)
            with open(MEASUREMENTS_FILE, "w", encoding="utf-8") as f:
                json.dump(hist, f, indent=2)
            with open(TD_DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(td_data, f, indent=2)

            num_dev = len(bracelets)
            num_new = len(all_meas)
            print(f"[Опрошено] {num_dev} устройств, {num_new} записей")
            print("Ждем...\n")

            if USE_FIXED_START:
                current_start += timedelta(seconds=TIME_FETCH_SEC)
            time.sleep(FETCH_INTERVAL_SEC)


def signal_handler(_sig, _frame):
    """Обработчик Ctrl+C: бэкап и выход."""
    print("\nCtrl+C. Бэкап...")
    backup_files()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    ensure_file(BRACELETS_FILE, default_bracelets)
    ensure_file(MEASUREMENTS_FILE, default_measurements)
    ensure_file(TD_DATA_FILE, default_td_data)
    try:
        main()
    except KeyboardInterrupt:
        signal_handler(None, None)
