import json
import time
import datetime
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.columns import Columns
from rich.text import Text
from rich import box

console = Console()


def load_data(file_path='td_data.json'):
    """Загружает данные из JSON-файла. При ошибке возвращает пустой список."""
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except Exception as error:
        console.print(f"[red]Ошибка загрузки данных:[/red] {error}")
        return []


def group_data_by_device(data):
    """Группирует записи по полю 'device_name'."""
    grouped = {}
    for entry in data:
        device_name = entry.get('device_name', 'Unknown')
        grouped.setdefault(device_name, []).append(entry)
    return grouped


def build_device_table(data_points):
    """
    Создаёт таблицу для отображения данных устройства.
    Таблица имеет строго фиксированную ширину (48 символов для контента, чтобы вместе с
    рамкой панели (2 символа) итоговая ширина была 50) и строго 10 строк в теле.
    """
    OUTPUT_ROW_COUNT = 10

    # Создаём таблицу с фиксированными размерами: суммарное содержимое должно быть 48 символов.
    # Считаем так: при 6 столбцах и 7 вертикальных разделителях общее содержимое = 48 - 7 = 41 символ.
    # Распределяем: "Time" – 7, "HR" – 7, "LF/HF" – 7, "RMSSD" – 7, "SDRR" – 6, "SI" – 7.
    table = Table(show_header=True, header_style="bold magenta",
                  expand=False, box=box.SIMPLE)
    table.add_column("Time", justify="center", no_wrap=True, width=15)
    table.add_column("HR", justify="center", no_wrap=True, width=10)
    table.add_column("LF/HF", justify="center", width=12)
    table.add_column("RMSSD", justify="center", width=12)
    table.add_column("SDRR", justify="center", width=11)
    table.add_column("SI", justify="center", width=10)

    sorted_points = []
    try:
        if data_points:
            sorted_points = sorted(
                data_points,
                key=lambda x: datetime.datetime.strptime(
                    x.get('timestamp', ''), "%Y-%m-%d %H:%M:%S")
            )
    except Exception as error:
        console.print(f"[red]Ошибка сортировки данных:[/red] {error}")
        sorted_points = data_points or []

    # Берём последние OUTPUT_ROW_COUNT записей
    rows_to_display = sorted_points[-OUTPUT_ROW_COUNT:] if sorted_points else []
    for point in rows_to_display:
        try:
            ts = datetime.datetime.strptime(
                point.get('timestamp', ''), "%Y-%m-%d %H:%M:%S"
            ).strftime("%H:%M:%S") if point.get('timestamp') else ""
        except Exception:
            ts = point.get('timestamp', '')
        table.add_row(
            ts,
            str(point.get('hr', '')),
            str(point.get('lf_hf_ratio', '')),
            str(point.get('rmssd', '')),
            str(point.get('sdrr', '')),
            str(point.get('si', ''))
        )

    # Если записей меньше OUTPUT_ROW_COUNT, дополняем пустыми строками
    for _ in range(OUTPUT_ROW_COUNT - len(rows_to_display)):
        table.add_row("", "", "", "", "", "")
    return table


def build_device_panel(device_name, data_points):
    """
    Оборачивает таблицу с данными в панель с заголовком устройства.
    Панель имеет фиксированную ширину 50 символов.
    """
    content = build_device_table(data_points)
    return Panel(content, title=device_name, width=50, expand=False)


def build_layout(data, session, current_time, countdown):
    """
    Формирует общий макет TUI:
      – Заголовок с информацией о сессии, текущем времени и обратном отсчёте.
      – Сетку 3x2 с панелями устройств, где каждая панель имеет ширину 50.
    """
    grouped_data = group_data_by_device(data)
    devices = ["swaid 1330", "swaid 1329", "swaid 1328",
               "swaid 1327", "swaid 1326", "swaid 1319"]
    panels = [build_device_panel(
        device, grouped_data.get(device, [])) for device in devices]

    # Располагаем панели в три ряда по две колонки с равной и фиксированной шириной
    grid = [
        Columns([panels[0], panels[3]], expand=False, equal=True, padding=1),
        Columns([panels[1], panels[4]], expand=False, equal=True, padding=1),
        Columns([panels[2], panels[5]], expand=False, equal=True, padding=1)
    ]

    # Заголовок с информацией: сессия, текущее время и обратный отсчёт
    header = Table.grid(expand=True)
    header.add_column(justify="left")
    header.add_column(justify="center")
    header.add_column(justify="right")
    header.add_row(
        f"Session: {session}",
        f"Time: {current_time}",
        f"Next update in: {countdown} s"
    )

    layout = Table.grid(expand=True)
    layout.add_row(header)
    for row in grid:
        layout.add_row(row)
    return layout


def main():
    update_interval = 5  # Интервал обновления данных в секундах
    last_update = time.time() - update_interval
    data = load_data()
    session = data[0].get('session', 'N/A') if data else "N/A"

    with Live(console=console, refresh_per_second=4, screen=True) as live:
        try:
            while True:
                now = time.time()
                countdown = max(0, update_interval - (now - last_update))
                if countdown <= 0:
                    data = load_data()
                    session = data[0].get('session', 'N/A') if data else "N/A"
                    last_update = now

                current_time = datetime.datetime.now().strftime("%H:%M:%S")
                layout = build_layout(
                    data, session, current_time, int(countdown))
                live.update(layout)
                time.sleep(1)
        except KeyboardInterrupt:
            console.print("[bold red]Exiting...[/bold red]")


if __name__ == "__main__":
    main()
