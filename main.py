#!/usr/bin/env python3
"""
Скрипт принимает данные с удалённого сервера для каждого браслета,
сохраняет их в локальные JSON-файлы и обновляет файл с данными для TouchDesigner.
При прерывании (Ctrl+C) выполняется резервное копирование файлов и явное завершение пула потоков.
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
# Настраиваемые параметры (конфигурация)
# ==========================
API_URL = "http://157.230.95.209:30003/get_ppg_data"
MY_TIMEZONE = timezone(timedelta(hours=3))  # часовой пояс UTC+3
TIME_FETCH_SECONDS = 1  # интервал для запроса данных (в секундах)
FETCH_INTERVAL_SECONDS = 5  # интервал между опросами (в секундах)

# Режим выбора даты начала опроса:
# Если USE_FIXED_START_DATE = False, опрос идёт от текущего момента.
# Если True, используется фиксированная дата, указанная в FIXED_START_DATE.
USE_FIXED_START_DATE = True
FIXED_START_DATE = "2024-11-03-00-23-13"  # формат: %Y-%m-%d-%H-%M-%S

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

# Глобальный пул потоков (будет создан в main())
global_executor = None

# ==========================
# Функция для создания файла, если он отсутствует
# ==========================


def ensure_file(filename, default_data):
    """Если файла нет, создаёт его с данными по умолчанию."""
    if not os.path.exists(filename):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(default_data, f, indent=2)
        print(f"Создан файл {filename}")


# Проверяем наличие файлов и создаём их при необходимости
ensure_file(BRACELETS_FILE, default_bracelets)
ensure_file(MEASUREMENTS_FILE, default_measurements)
ensure_file(TD_DATA_FILE, default_td_data)


# ==========================
# Функция для создания резервных копий файлов
# ==========================
def backup_files():
    """Создаёт резервные копии всех файлов в папке BACKUP_DIR."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for filename in [BRACELETS_FILE, MEASUREMENTS_FILE, TD_DATA_FILE]:
        backup_filename = os.path.join(
            BACKUP_DIR, f"{os.path.splitext(filename)[0]}_backup_{timestamp}.json"
        )
        try:
            shutil.copy(filename, backup_filename)
            print(f"Резервная копия {filename} создана как {backup_filename}")
        except Exception as e:
            print(f"Ошибка при создании резервной копии {filename}: {e}")


# ==========================
# Функция для запроса данных для заданного браслета
# ==========================
def fetch_data(device_name, start_time_override=None):
    """
    Отправляет GET-запрос к API для заданного устройства (device_name).

    Если start_time_override задан (тип datetime), используется интервал:
      start_time = start_time_override
      end_time = start_time_override + TIME_FETCH_SECONDS.
    Иначе используется интервал от текущего момента минус TIME_FETCH_SECONDS до текущего момента.

    Выводит в консоль:
      - GET-запрос (полное значение URL)
      - Ответ сервера (полная структура JSON)

    Возвращает список измерений или пустой список, если данные отсутствуют.
    """
    if start_time_override is not None:
        start_time = start_time_override
        end_time = start_time + timedelta(seconds=TIME_FETCH_SECONDS)
    else:
        current_time = datetime.now(MY_TIMEZONE)
        start_time = current_time - timedelta(seconds=TIME_FETCH_SECONDS)
        end_time = current_time

    formatted_start = start_time.strftime("%Y-%m-%d-%H-%M-%S")
    formatted_end = end_time.strftime("%Y-%m-%d-%H-%M-%S")
    params = {
        "device_name": device_name,
        "start": formatted_start,
        "end": formatted_end
    }
    try:
        response = requests.get(API_URL, params=params, timeout=10)
    except requests.RequestException as e:
        print(f"[{device_name}] Ошибка получения данных: {e}")
        return []

    url_str = response.url
    print(f"[{device_name}] Запрос: {url_str}")

    try:
        data = response.json()
    except Exception as e:
        print(f"[{device_name}] Ошибка декодирования JSON: {e}")
        return []

    print(f"[{device_name}] Ответ: {data}")

    # Если ответ содержит специальное сообщение, сигнализирующее об отсутствии данных:
    if data.get("message") == "No data found for the specified device.":
        return []
    if not data.get("hr") and not data.get("si"):
        print(f"[{device_name}] Нет данных для указанного промежутка времени.")
        return []

    measurements = []
    for hr, si, ts in zip(data.get("hr", []), data.get("si", []), data.get("time", [])):
        ts_clean = ts.split(".")[0]  # удаляем микросекунды, если есть
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
    """Основной цикл получает данные для каждого браслета и обновляет локальные файлы."""
    global global_executor
    print("Запуск скрипта получения данных.")

    try:
        with open(BRACELETS_FILE, "r", encoding="utf-8") as f:
            bracelets = json.load(f)
    except Exception as e:
        print(f"Ошибка загрузки {BRACELETS_FILE}: {e}")
        sys.exit(1)

    if not bracelets:
        print("Список браслетов пуст. Добавьте хотя бы один браслет в bracelets.json.")
        sys.exit(1)

    fixed_mode = USE_FIXED_START_DATE
    if fixed_mode:
        try:
            current_fixed_start = datetime.strptime(
                FIXED_START_DATE, "%Y-%m-%d-%H-%M-%S")
            current_fixed_start = current_fixed_start.replace(
                tzinfo=MY_TIMEZONE)
        except ValueError as e:
            print(f"Ошибка формата FIXED_START_DATE: {e}")
            sys.exit(1)
    else:
        current_fixed_start = None

    # Создаем глобальный пул потоков
    global_executor = ThreadPoolExecutor(max_workers=5)

    try:
        while True:
            all_measurements = []
            td_data = {}

            future_to_device = {
                global_executor.submit(
                    fetch_data,
                    bracelet.get("swaid_id"),
                    current_fixed_start if fixed_mode else None
                ): bracelet.get("swaid_id")
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

            # Читаем существующую историю измерений и добавляем новые данные
            try:
                with open(MEASUREMENTS_FILE, "r", encoding="utf-8") as f:
                    history = json.load(f)
            except Exception:
                history = []
            history.extend(all_measurements)
            with open(MEASUREMENTS_FILE, "w", encoding="utf-8") as f:
                json.dump(history, f, indent=2)

            with open(TD_DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(td_data, f, indent=2)

            print(
                f"Обработано {len(bracelets)} устройств. Получено {len(all_measurements)} новых записей."
            )
            print("Ожидание следующего опроса...\n")

            if fixed_mode:
                current_fixed_start += timedelta(seconds=TIME_FETCH_SECONDS)

            time.sleep(FETCH_INTERVAL_SECONDS)
    finally:
        # Явное завершение пула потоков перед выходом
        global_executor.shutdown(wait=True)
        print("Пул потоков был завершён.")


# ==========================
# Обработка прерывания (Ctrl+C)
# ==========================
def signal_handler(_signum, _frame):
    """Обработчик сигнала прерывания, выполняет резервное копирование файлов и завершение работы."""
    print("\nПолучен сигнал завершения (Ctrl+C). Выполняется резервное копирование файлов...")
    backup_files()
    if global_executor is not None:
        global_executor.shutdown(wait=True)
        print("Пул потоков был завершён.")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    try:
        main()
    except KeyboardInterrupt:
        backup_files()
        if global_executor is not None:
            global_executor.shutdown(wait=True)
            print("Пул потоков был завершён.")
        print("Прерывание работы. Завершаем работу.")
        sys.exit(0)
