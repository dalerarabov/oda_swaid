#!/usr/bin/env python3
import os
import sys
import json
import shutil
import requests
import signal
import time
from datetime import datetime, timedelta, timezone

# ==========================
# Настраиваемые параметры (конфигурация)
# ==========================
API_URL = "http://157.230.95.209:30003/get_ppg_data"
# Таймзона, например UTC+3
MY_TIMEZONE = timezone(timedelta(hours=3))
# Интервал за который запрашиваются данные (в минутах)
TIME_FETCH_MINUTES = 5
# Интервал между опросами сервера (в секундах)
FETCH_INTERVAL_SECONDS = 300  # 5 минут

# Имена файлов для хранения данных
BRACELETS_FILE = "bracelets.json"        # список браслетов
SETTINGS_FILE = "settings.json"          # параметры работы
MEASUREMENTS_FILE = "measurements.json"  # история измерений
TD_DATA_FILE = "td_data.json"            # данные для TouchDesigner

# Папка для резервного копирования
BACKUP_DIR = "backup"

# ==========================
# Функция для создания файла, если он отсутствует
# ==========================
def ensure_file(filename, default_data):
    if not os.path.exists(filename):
        with open(filename, 'w') as f:
            json.dump(default_data, f, indent=2)
        print(f"Создан файл {filename}")

# Шаблонная структура для каждого файла:
# Обновлённый список браслетов по умолчанию с двумя записями
default_bracelets = [
    {"swaid_id": "1330", "name": "Bracelet 1330"},
    {"swaid_id": "34", "name": "Bracelet 34"}
]
default_settings = {
    "time_fetch_start_shift": 40,
    "time_fetch_end_shift": 30,
    "need_update_screen": True,
    "need_update_translation": True,
    "nice_data": False,
    "peak_treshhold": 350
}
default_measurements = []  # пустой список измерений
default_td_data = {}       # для TouchDesigner – словарь, где ключом будет "swaid_id", а значением последнее измерение

# Проверяем наличие файлов и создаём при необходимости
ensure_file(BRACELETS_FILE, default_bracelets)
ensure_file(SETTINGS_FILE, default_settings)
ensure_file(MEASUREMENTS_FILE, default_measurements)
ensure_file(TD_DATA_FILE, default_td_data)

# ==========================
# Функция для создания резервных копий JSON файлов
# ==========================
def backup_files():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for filename in [BRACELETS_FILE, SETTINGS_FILE, MEASUREMENTS_FILE, TD_DATA_FILE]:
        backup_filename = os.path.join(BACKUP_DIR, f"{os.path.splitext(filename)[0]}_backup_{timestamp}.json")
        try:
            shutil.copy(filename, backup_filename)
            print(f"Резервная копия файла {filename} создана как {backup_filename}")
        except Exception as e:
            print(f"Ошибка при создании резервной копии {filename}: {e}")

# ==========================
# Функция для запроса данных для заданного браслета
# ==========================
def fetch_data(device_name, settings):
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
    response = requests.get(API_URL, params=params)
    if response.status_code != 200:
        print(f"[{device_name}] Ошибка получения данных: статус {response.status_code}")
        return []  # В случае ошибки возвращаем пустой список
    data = response.json()
    measurements = []
    for hr, si, ts in zip(data.get("hr", []), data.get("si", []), data.get("time", [])):
        ts_clean = ts.split('.')[0]  # удаление микросекунд, если есть
        measurements.append({
            "timestamp": ts_clean,
            "hr": hr,
            "si": si,
            "swaid_id": device_name
        })
    return measurements

# ==========================
# Основной цикл – получение данных для всех браслетов
# ==========================
def main():
    print("Запуск скрипта получения данных.")
    # Загрузить список браслетов и настройки
    with open(BRACELETS_FILE, 'r') as f:
        bracelets = json.load(f)
    with open(SETTINGS_FILE, 'r') as f:
        settings = json.load(f)

    # Если список браслетов пуст, уведомляем пользователя и завершаем работу
    if not bracelets:
        print("Список браслетов пуст. Добавьте хотя бы один браслет в bracelets.json.")
        sys.exit(1)

    while True:
        all_measurements = []
        td_data = {}
        for bracelet in bracelets:
            device_id = bracelet.get("swaid_id")
            if not device_id:
                continue  # пропустить, если ни одного ID нет
            m = fetch_data(device_id, settings)
            if m:
                all_measurements.extend(m)
                td_data[device_id] = m[-1]  # сохраняем последнее измерение для данного браслета

        # Обновляем историю измерений
        try:
            with open(MEASUREMENTS_FILE, 'r') as f:
                history = json.load(f)
        except Exception:
            history = []
        history.extend(all_measurements)
        with open(MEASUREMENTS_FILE, 'w') as f:
            json.dump(history, f, indent=2)

        # Сохраняем данные для TouchDesigner
        with open(TD_DATA_FILE, 'w') as f:
            json.dump(td_data, f, indent=2)

        print(f"Обработано {len(bracelets)} устройств. Получено {len(all_measurements)} новых записей.")
        print("Ожидание следующего опроса...\n")
        time.sleep(FETCH_INTERVAL_SECONDS)

# ==========================
# Обработка прерывания: резервное копирование при завершении
# ==========================
def signal_handler(signum, frame):
    print("\nПолучен сигнал завершения (Ctrl+C). Выполняется резервное копирование файлов...")
    backup_files()
    sys.exit(0)

if __name__ == "__main__":
    # Регистрируем обработчик сигнала (Ctrl+C)
    signal.signal(signal.SIGINT, signal_handler)
    try:
        main()
    except KeyboardInterrupt:
        backup_files()
        print("Прерывание работы. Завершаем работу.")
        sys.exit(0)
