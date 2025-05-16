#!/usr/bin/env python3
"""
Получает данные с сервера для браслетов, сохраняет их в JSON и обновляет файл
для TouchDesigner. Создаёт резервные копии при Ctrl+C.
"""

import argparse
import json
import logging
import os
import shutil
import signal
import sys
import time
import traceback
import urllib.parse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, TypedDict, Union

import requests
from tabulate import tabulate

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-8s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

API_URL = "http://157.230.95.209:30003/get_ppg_data"
MY_TZ = timezone(timedelta(hours=3))
TIME_FETCH_SEC = 60         # Длительность окна для выборки данных с сервера
FETCH_INTERVAL_SEC = 5      # Интервал между запросами

USE_FIXED_START = False
FIXED_START = "2025-05-15-16-23-00"

BRACELETS_FILE = "bracelets.json"
MEASUREMENTS_FILE = "measurements.json"
TD_DATA_FILE = "td_data.json"
BACKUP_DIR = "backup"

DEFAULT_BRACELETS = [
    {
        "mac_address": "CE:D6:AD:45:ED:75",
        "name": "swaid 1330",
        "process": True  # Обрабатывай данные только если True
    }
]
DEFAULT_MEASUREMENTS: List[Dict] = []
DEFAULT_TD_DATA: Dict = {}


class Measurement(TypedDict):
    """Тип для представления измерений одного браслета."""
    session: Union[int, str]
    device_mac: str
    device_name: str
    timestamp: str
    hr: Optional[int]
    lf_hf_ratio: Optional[float]
    rmssd: Optional[int]
    sdrr: Optional[int]
    si: Optional[float]


def ensure_file(filename: str, default_data: List | Dict) -> None:
    """Создаёт файл с данными по умолчанию, если он отсутствует.

    Args:
        filename: Имя файла для проверки/создания.
        default_data: Данные для записи в файл в формате JSON.
    """
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
    if not os.path.exists(filename):
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(default_data, file, indent=2)
        logger.info("Создан файл: %s", filename)
    else:
        logger.info("Файл %s существует", filename)


def backup_files() -> None:
    """Создаёт резервные копии основных JSON-файлов.

    Для каждого файла (bracelets, measurements, td_data) создаётся копия в
    директории BACKUP_DIR с добавлением временной метки.
    """
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
    device_log: str,
    error: Exception,
    start_time: datetime,
    end_time: Optional[datetime]
) -> Tuple[List[Measurement], datetime, Optional[datetime]]:
    """Обрабатывает ошибки запроса данных.

    Args:
        device_log: Форматированная строка для идентификации устройства.
        error: Возникшая ошибка.
        start_time: Время начала запроса.
        end_time: Время окончания запроса (может быть None).

    Returns:
        Кортеж: пустой список измерений, start_time и end_time.
    """
    logger.error("%s Ошибка: %s", device_log, error)
    return [], start_time, end_time


def get_status_color(status_code: int) -> str:
    """Возвращает ANSI цвет для статуса ответа HTTP.

    Args:
        status_code: Код ответа HTTP.

    Returns:
        ANSI строка цвета.
    """
    if status_code == 200:
        return "\033[36m"  # Синий
    elif status_code == 404:
        return "\033[33m"  # Желтый
    else:
        return "\033[0m"   # Сброс


def fetch_data(
    session_name: str,
    bracelet: Dict,
    start_time: datetime,
    end_time: datetime
) -> Tuple[List[Measurement], datetime, Optional[datetime]]:
    """Запрашивает данные с сервера для одного браслета.

    Args:
        session_name: Название сессии для формирования имени.
        bracelet: Словарь с данными браслета.
        start_time: Начало окна запроса.
        end_time: Конец окна запроса.

    Returns:
        Кортеж: (список измерений, время начала запроса, время окончания запроса).
    """
    device_name = bracelet.get("name", "Без имени")
    mac_address = bracelet.get("mac_address", "")
    # Формирование префикса для логирования: Имя устройства и последние 6 символов MAC
    log_prefix = f"[{device_name} {mac_address[-6:]}]" if mac_address else "[Без MAC]"
    start_request_time = datetime.now(MY_TZ)
    end_request_time = None

    # Если MAC-адрес не задан, пропускаем этот запрос
    if not mac_address:
        logger.warning("%s Пропущен запрос: нет MAC", log_prefix)
        return [], start_request_time, end_request_time

    # Форматируем временные метки для передачи в параметры запроса
    s_start = start_time.strftime("%Y-%m-%d-%H-%M-%S")
    s_end = end_time.strftime("%Y-%m-%d-%H-%M-%S")
    query_device_name = f"{session_name}_{mac_address}"
    params = {"device_name": query_device_name, "start": s_start, "end": s_end}

    query_string = urllib.parse.urlencode(params)
    full_url = f"{API_URL}?{query_string}"
    logger.info("%s Запрос: %s", log_prefix, full_url)

    try:
        response = requests.get(API_URL, params=params, timeout=10)
        end_request_time = datetime.now(MY_TZ)
        logger.info("%s%s Код ответа: %s\033[0m",
                    log_prefix, get_status_color(response.status_code), response.status_code)
        logger.debug("%s URL: %s", log_prefix, response.url)
        response.raise_for_status()
        data = response.json()
        logger.debug("%s Данные: %s", log_prefix, json.dumps(data, indent=2))
    except (requests.RequestException, json.JSONDecodeError) as error:
        return handle_fetch_error(log_prefix, error, start_request_time, end_request_time)

    if data.get("message") == "No data found for the specified device.":
        logger.info("%s Данные не найдены", log_prefix)
        return [], start_request_time, end_request_time

    if not any(data.get(key) for key in ["hr", "lf_hf_ratio", "rmssd", "sdrr", "si"]):
        logger.warning("%s Пустой набор данных", log_prefix)
        return [], start_request_time, end_request_time

    try:
        session_val: Union[int, str] = int(session_name)
    except ValueError:
        session_val = session_name

    measurements: List[Measurement] = [
        {
            "session": session_val,
            "device_mac": mac_address,
            "device_name": device_name,
            "timestamp": ts,
            "hr": hr,
            "lf_hf_ratio": lf_hf,
            "rmssd": rmssd,
            "sdrr": sdrr,
            "si": si,
        }
        for hr, lf_hf, rmssd, sdrr, si, ts in zip(
            data.get("hr", []), data.get("lf_hf_ratio", []),
            data.get("rmssd", []), data.get("sdrr", []),
            data.get("si", []), data.get("time", [])
        )
    ]
    logger.info("%s Получено %d измерений", log_prefix, len(measurements))
    return measurements, start_request_time, end_request_time


def load_bracelets() -> List[Dict]:
    """Загружает список браслетов из файла BRACELETS_FILE.

    Возвращает:
        Список словарей с информацией о браслетах, обрабатываемых только если 'process' True.
    """
    try:
        with open(BRACELETS_FILE, "r", encoding="utf-8") as file:
            bracelets = json.load(file)
        filtered_bracelets = [
            b for b in bracelets if b.get("mac_address") and b.get("process", False) is True
        ]
        logger.info("Загружено %d браслетов из %s", len(
            filtered_bracelets), BRACELETS_FILE)
        for bracelet in filtered_bracelets:
            logger.info(" - %s (%s)", bracelet.get("name", "Без имени"),
                        bracelet.get("mac_address", "Без MAC"))
        return filtered_bracelets
    except (FileNotFoundError, json.JSONDecodeError, PermissionError) as error:
        logger.error("Ошибка загрузки %s: %s", BRACELETS_FILE, error)
        sys.exit(1)


def format_table(
    measurements: List[Measurement],
    s_start: str,
    s_end: str,
    s_received: str
) -> str:
    """
    Форматирует данные измерений в таблицу для вывода с разделением данных
    от разных устройств.

    Args:
        measurements: Список измерений нового формата.
        s_start: Строковое представление начала окна.
        s_end: Строковое представление конца окна.
        s_received: Строковое представление последнего ответа.

    Returns:
        Строка, содержащая табличное представление данных.
    """
    from collections import defaultdict  # если не импортирован глобально
    from tabulate import tabulate       # если не импортирован глобально

    headers = ["Mark", "Time", "Device", "MAC",
               "HR", "LF/HF", "RMSSD", "SDRR", "SI"]

    # Первая строка: информация о начале окна
    table = [
        ["Start", s_start, "", "", "", "", "", "", ""]
    ]

    # Группируем измерения по устройствам (по имени и MAC)
    grouped = defaultdict(list)
    for m in measurements:
        key = (m["device_name"], m["device_mac"])
        grouped[key].append(m)

    mark = 1
    sorted_keys = sorted(grouped.keys(), key=lambda x: x[0])
    for key in sorted_keys:
        # Сортируем данные в группе по времени
        grp = sorted(grouped[key], key=lambda x: x["timestamp"])
        for m in grp:
            row = [
                str(mark),
                m["timestamp"],
                m["device_name"],
                m["device_mac"],
                m["hr"] if m["hr"] is not None else "-",
                m["lf_hf_ratio"] if m["lf_hf_ratio"] is not None else "-",
                m["rmssd"] if m["rmssd"] is not None else "-",
                m["sdrr"] if m["sdrr"] is not None else "-",
                m["si"] if m["si"] is not None else "-"
            ]
            table.append(row)
            mark += 1
        # Добавляем пустую строку между группами
        table.append([""] * len(headers))

    # Добавляем строки окончания окна и время последнего ответа
    table.append(["End", s_end, "", "", "", "", "", "", ""])
    table.append(["Received", s_received, "", "", "", "", "", "", ""])

    return tabulate(table, headers=headers, tablefmt="simple")


def fetch_and_process_data(
    session_name: str,
    bracelets: List[Dict],
    current_start: Optional[datetime],
    executor: ThreadPoolExecutor
) -> Tuple[List[Measurement], List[Measurement], datetime, datetime, datetime]:
    """Параллельно запрашивает данные для всех браслетов.

    Вычисляет общее окно времени и передаёт его в запросы. Собирает
    результаты, формирует словарь последних измерений, а также возвращает
    время последнего полученного ответа от сервера.

    Args:
        session_name: Название сессии.
        bracelets: Список браслетов.
        current_start: Начало окна запроса (если задано).
        executor: Пул для параллельного выполнения запросов.

    Returns:
        Кортеж из:
          - списка измерений,
          - словаря последних измерений,
          - start_time и end_time для запроса,
          - времени последнего ответа от сервера.
    """
    all_measurements: List[Measurement] = []
    td_data: List[Measurement] = []
    last_received: Optional[datetime] = None

    if current_start:
        start_time = current_start
        end_time = start_time + timedelta(seconds=TIME_FETCH_SEC)
    else:
        now = datetime.now(MY_TZ)
        start_time = now - timedelta(seconds=TIME_FETCH_SEC)
        end_time = now

    logger.info("Расчет окна: start = %s, end = %s", start_time, end_time)
    logger.info("Запуск пула для %d браслетов", len(bracelets))

    futures = {
        executor.submit(fetch_data, session_name, b, start_time, end_time): b
        for b in bracelets if b.get("mac_address") and b.get("process", False) is True
    }

    for future in as_completed(futures):
        bracelet = futures[future]
        device_name = bracelet.get("name", "Без имени")
        mac_address = bracelet.get("mac_address", "")
        log_prefix = f"[{device_name} {mac_address[-6:]}]" if mac_address else "[Без MAC]"
        logger.info("%s Ожидание завершения задачи...", log_prefix)
        try:
            measurements, req_start, req_end = future.result()
            if measurements:
                logger.info("%s Получены данные: %d измерений",
                            log_prefix, len(measurements))
                all_measurements.extend(measurements)
                td_data.extend(measurements)
                logger.info("%s Последнее измерение сохранено", log_prefix)
            if req_end and (last_received is None or req_end > last_received):
                last_received = req_end
                logger.info(
                    "%s Обновлено время последнего ответа сервера: %s", log_prefix, last_received)
        except Exception as error:
            logger.error("%s Ошибка обработки: %s\n%s", log_prefix, error,
                         "".join(traceback.format_tb(error.__traceback__)))
    if last_received is None:
        last_received = end_time
        logger.warning(
            "Не удалось получить ни одного ответа от сервера, используем end_time: %s", last_received)
    return all_measurements, td_data, start_time, end_time, last_received


def save_data(
    history: List[Measurement],
    new_measurements: List[Measurement],
    td_data: List[Measurement],
    s_start: str,
    s_end: str,
    s_received: str
) -> None:
    """Сохраняет измерения в историю и обновляет файлы.

    Если новые измерения есть, они добавляются к истории, затем
    данные записываются в файл, и выводится таблица с разделением данных по устройствам.

    Args:
        history: Существующая история измерений.
        new_measurements: Новые измерения за цикл.
        td_data: Словарь последних значений для каждого устройства.
        s_start: Строковое представление начала окна.
        s_end: Строковое представление конца окна.
        s_received: Строковое представление последнего ответа.
    """
    if new_measurements:
        history.extend(new_measurements)
        with open(MEASUREMENTS_FILE, "w", encoding="utf-8") as file:
            json.dump(history, file, indent=2)
        logger.info("Добавлено %d записей в %s", len(
            new_measurements), MEASUREMENTS_FILE)
        table = format_table(new_measurements, s_start, s_end, s_received)
        print(f"\n{table}\n")
    else:
        logger.warning("Нет новых измерений, таблица не выведена")
    with open(TD_DATA_FILE, "w", encoding="utf-8") as file:
        json.dump(td_data, file, indent=2)


def main() -> None:
    """Запускает основной цикл получения данных с сервера."""
    print("")
    parser = argparse.ArgumentParser(
        description="Получение данных с сервера")
    parser.add_argument("--session_name", help="Название сессии")
    args = parser.parse_args()
    session_name = (args.session_name or input(
        "Введите название сессии: ").strip())
    if not session_name:
        logger.error("Название сессии не задано")
        sys.exit(1)
    logger.info("\033[31mЗапуск с сессией: %s\033[0m", session_name)
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
    try:
        with open(MEASUREMENTS_FILE, "r", encoding="utf-8") as file:
            history: List[Measurement] = json.load(file)
        logger.info("Загружено %d измерений", len(history))
    except (FileNotFoundError, json.JSONDecodeError):
        history = []
        logger.info("Файл %s не найден, создан пустой", MEASUREMENTS_FILE)
    max_workers = min(len(bracelets), 6)
    logger.info("Пул потоков: %d рабочих", max_workers)
    executor = ThreadPoolExecutor(max_workers=max_workers)
    print("")
    try:
        while True:
            logger.info(
                "\033[32mНовый цикл для %d браслетов\033[0m", len(bracelets))
            new_measurements, td_data, start_time, end_time, last_received = fetch_and_process_data(
                session_name, bracelets, current_start, executor)
            s_start = start_time.strftime(
                "%Y-%m-%d %H:%M:%S.%f")[:-3] if start_time else "-"
            s_end = end_time.strftime(
                "%Y-%m-%d %H:%M:%S.%f")[:-3] if end_time else "-"
            s_received = last_received.strftime(
                "%Y-%m-%d %H:%M:%S.%f")[:-3] if last_received else "-"
            save_data(history, new_measurements, td_data,
                      s_start, s_end, s_received)
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
    """Обрабатывает сигнал SIGINT и создаёт резервные копии файлов."""
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
