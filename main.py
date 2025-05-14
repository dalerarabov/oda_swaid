#!/usr/bin/env python3
"""
Скрипт принимает данные с удалённого сервера для каждого браслета,
сохраняет их в локальные JSON-файлы и обновляет файл с данными для
TouchDesigner. При прерывании (Ctrl+C) создаются резервные копии файлов.
"""

import os
import sys
import json
import shutil
import requests
import signal
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================
# Настраиваемые параметры (конфигурация)
# ==========================
API_URL = "http://157.230.95.209:30003/get_ppg_data"
MY_TIMEZONE = timezone(timedelta(hours=3))  # часовой пояс UTC+3
TIME_FETCH_MINUTES = 5       # интервал запроса данных (в минутах)
FETCH_INTERVAL_SECONDS = 5  # интервал между опросами (в секундах)

# Имена файлов для хранения данных
BRACELETS_FILE = "bracelets.json"        # список браслетов
MEASUREMENTS_FILE = "measurements.json"  # история измерений
TD_DATA_FILE = "td_data.json"            # данные для TouchDesigner

# Папка для резервного копирования
BACKUP_DIR = "backup"

# ==========================
# Шаблонные структуры по умолчанию
# ==========================
default_bracelets = [
    {"swaid_id": "1330", "name": "Bracelet 1330"},
    {"swaid_id": "34", "name": "Bracelet 34"}
]
default_measurements = []  # пустой список измерений
default_td_data = {}       # пустой словарь для TouchDesigner

# ==========================
# Функция для создания файла, если он отсутствует
# ==========================
def ensure_file(filename, default_data):
    """Если файла нет, создает его с переданными данными по умолчанию."""
    if not os.path.exists(filename):
        with open(filename, 'w') as f:
            json.dump(default_data, f, indent=2)
        print(f"Создан файл {filename}")


# Проверяем наличие файлов и создаём их, если нужно
ensure_file(BRACELETS_FILE, default_bracelets)
ensure_file(MEASUREMENTS_FILE, default_measurements)
ensure_file(TD_DATA_FILE, default_td_data)


# ==========================
# Функция для создания резервных копий файлов
# ==========================
def backup_files():
    """Создает резервные копии всех файлов в папке BACKUP_DIR."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for filename in [BRACELETS_FILE, MEASUREMENTS_FILE, TD_DATA_FILE]:
        backup_filename = os.path.join(
            BACKUP_DIR, f"{os.path.splitext(filename)[0]}_backup_{timestamp}.json"
        )
        try:
            shutil.copy(filename, backup_filename)
            print(f"Резервная копия файла {filename} создана как {backup_filename}")
        except Exception as e:
            print(f"Ошибка при создании резервной копии {filename}: {e}")


# ==========================
# Функция для запроса данных для заданного браслета
# ==========================
def fetch_data(device_name):
    """
    Отправляет GET-запрос к API для заданного браслета (device_name)
    и возвращает список измерений.
    """
    current_time = datetime.now(MY_TIMEZONE)
    start_time = current_time - timedelta(minutes=TIME_FETCH_MINUTES)
    end_time = current_time

    formatted_start = start_time.strftime("%Y-%m-%d-%H-%M-%S")
    formatted_end = end_time.strftime("%Y-%m-%d-%H-%M-%S")

    params = {
        "device_name": device_name,
        "start": formatted_start,
        "end": formatted_end
    }
    print(f"[{device_name}] Запрос с параметрами: {params}")
    try:
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"[{device_name}] Ошибка получения данных: {e}")
        return []

    data = response.json()
    measurements = []
    for hr, si, ts in zip(data.get("hr", []), data.get("si", []), data.get("time", [])):
        ts_clean = ts.split('.')[0]  # удаляем микросекунды, если есть
        measurements.append({
            "timestamp": ts_clean,
            "hr": hr,
            "si": si,
            "swaid_id": device_name,
        })
    return measurements


# ==========================
# Основной цикл получения данных
# ==========================
def main():
    """Основной цикл: для каждого браслета запрашивает данные и обновляет файлы."""
    print("Запуск скрипта получения данных.")

    # Загрузить список браслетов
    try:
        with open(BRACELETS_FILE, 'r') as f:
            bracelets = json.load(f)
    except Exception as e:
        print(f"Ошибка загрузки {BRACELETS_FILE}: {e}")
        sys.exit(1)

    if not bracelets:
        print("Список браслетов пуст. Добавьте как минимум один браслет в bracelets.json.")
        sys.exit(1)

    while True:
        all_measurements = []
        td_data = {}

        # Параллельный запрос данных для всех браслетов
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_device = {
                executor.submit(fetch_data, bracelet.get("swaid_id")): bracelet.get("swaid_id")
                for bracelet in bracelets if bracelet.get("swaid_id")
            }
            for future in as_completed(future_to_device):
                device_id = future_to_device[future]
                try:
                    measurements = future.result()
                except Exception as exc:
                    print(f"[{device_id}] Исключение: {exc}")
                    continue
                if measurements:
                    all_measurements.extend(measurements)
                    # Сохраняем последнее измерение для данного браслета
                    td_data[device_id] = measurements[-1]

        # Обновляем историю измерений
        try:
            with open(MEASUREMENTS_FILE, 'r') as f:
                history = json.load(f)
        except Exception:
            history = []

        history.extend(all_measurements)
        with open(MEASUREMENTS_FILE, 'w') as f:
            json.dump(history, f, indent=2)

        # Обновляем данные для TouchDesigner
        with open(TD_DATA_FILE, 'w') as f:
            json.dump(td_data, f, indent=2)

        print(
            f"Обработано {len(bracelets)} устройств. "
            f"Получено {len(all_measurements)} новых записей."
        )
        print("Ожидание следующего опроса...\n")
        time.sleep(FETCH_INTERVAL_SECONDS)


# ==========================
# Обработка прерывания (Ctrl+C)
# ==========================
def signal_handler(signum, frame):
    print("\nПолучен сигнал завершения (Ctrl+C). Выполняется резервное копирование файлов...")
    backup_files()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    try:
        main()
    except KeyboardInterrupt:
        backup_files()
        print("Прерывание работы. Завершаем работу.")
        sys.exit(0)
