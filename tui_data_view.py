import json
import time
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.layout import Layout

# Константы
TD_DATA_FILE = "td_data.json"
UPDATE_INTERVAL = 4  # Интервал обновления в секундах
DEVICES = [
    "swaid 1330", "swaid 1329", "swaid 1341",
    "swaid 1336", "swaid 1327", "swaid 1319"
]


def read_td_data():
    """Считывает данные из td_data.json."""
    try:
        with open(TD_DATA_FILE, "r", encoding="utf-8") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def filter_measurements(data, device_name):
    """Фильтрует измерения по имени устройства."""
    return [m for m in data if m.get("device_name") == device_name]


def format_timestamp(timestamp):
    """Извлекает время из timestamp."""
    return timestamp.split(" ")[1] if " " in timestamp else timestamp


def create_table(device_name, measurements):
    """Создаёт таблицу для устройства с последними 10 измерениями."""
    table = Table(title=device_name, title_style="bold magenta")
    table.add_column("Time", justify="center", style="cyan")
    table.add_column("HR", justify="center", style="green")
    table.add_column("LF/HF", justify="center", style="yellow")
    table.add_column("RMSSD", justify="center", style="blue")
    table.add_column("SDRR", justify="center", style="red")
    table.add_column("SI", justify="center", style="purple")

    for m in measurements[-10:]:  # Показываем последние 10 записей
        time_str = format_timestamp(m.get("timestamp", "-"))
        hr = str(m.get("hr", "-")) if m.get("hr") is not None else "-"
        lf_hf = str(m.get("lf_hf_ratio", "-")
                    ) if m.get("lf_hf_ratio") is not None else "-"
        rmssd = str(m.get("rmssd", "-")) if m.get("rmssd") is not None else "-"
        sdrr = str(m.get("sdrr", "-")) if m.get("sdrr") is not None else "-"
        si = str(m.get("si", "-")) if m.get("si") is not None else "-"
        table.add_row(time_str, hr, lf_hf, rmssd, sdrr, si)
    return table


def main():
    """Основная функция для запуска TUI."""
    console = Console()
    layout = Layout()

    # Разделяем layout на 6 горизонтальных секций
    layout.split_column(
        Layout(name="table_0"),
        Layout(name="table_1"),
        Layout(name="table_2"),
        Layout(name="table_3"),
        Layout(name="table_4"),
        Layout(name="table_5")
    )

    with Live(layout, console=console, refresh_per_second=4):
        while True:
            data = read_td_data()
            for i, device in enumerate(DEVICES):
                measurements = filter_measurements(data, device)
                table = create_table(device, measurements)
                layout[f"table_{i}"].update(table)
            time.sleep(UPDATE_INTERVAL)


if __name__ == "__main__":
    # Убедитесь, что библиотека rich установлена: pip install rich
    try:
        main()
    except KeyboardInterrupt:
        print("\nЗавершение работы по Ctrl+C")
