#!/usr/bin/env python3
"""
TUI Dashboard для отображения данных устройств с гибкой настройкой вывода.
Пользователь может задавать:
  - Список устройств (таблиц), которые выводятся на экран.
  - Количество строк для данных в таблице (панель автоматически подстраивается).
  - Перечень столбцов и их порядок, а также ширину столбцов.
  - Варианты компоновки панелей (например, 1x6, 6x1, 3x2, 2x3).
"""

import json
import time
import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

console = Console()


@dataclass
class DashboardConfig:
    data_row_count: int = 7
    panel_width: int = 60
    columns_order: List[str] = field(
        # default_factory=lambda: ["Time", "HR", "LF/HF", "RMSSD", "SDRR", "SI"]
        default_factory=lambda: ["Time", "HR", "SI"]
    )
    columns_width: Dict[str, int] = field(
        default_factory=lambda: {
            "Time": 15,
            "HR": 8,
            "LF/HF": 7,
            "RMSSD": 8,
            "SDRR": 7,
            "SI": 8,
        }
    )
    devices_to_display: List[str] = field(
        default_factory=lambda: [
            # "swaid 1319",
            "swaid 1341",
            "swaid 1330",
            "swaid 1327",
            # "swaid 1329",
            "swaid 1336",
        ]
    )
    grid_layout: Tuple[int, int] = (2, 6)  # (rows, columns)
    include_reference_panel: bool = False


def get_color_for_param(param: str, value: Any) -> Optional[str]:
    """
    Возвращает цветовую разметку для указанного параметра по пороговым значениям.

    Пороговые значения:
      - HR: <60 → blue, 60–100 → green, >100 → red.
      - LF/HF: <1 → green, 1–2 → yellow, >2 → red.
      - RMSSD: <20 → red, 20–50 → yellow, ≥50 → green.
      - SDRR: <50 → red, 50–100 → yellow, ≥100 → green.
      - SI: <50 → green, 50–100 → yellow, ≥100 → red.
    """
    try:
        val = float(value)
    except (ValueError, TypeError):
        return None

    if param == "HR":
        if val < 60:
            return "blue"
        elif 60 <= val <= 100:
            return "green"
        else:
            return "red"
    elif param == "LF/HF":
        if val < 1:
            return "green"
        elif 1 <= val <= 2:
            return "yellow"
        else:
            return "red"
    elif param == "RMSSD":
        if val < 35:
            return "red"
        elif 35 <= val < 50:
            return "yellow"
        else:
            return "green"
    elif param == "SDRR":
        if val < 50:
            return "red"
        elif 50 <= val < 100:
            return "yellow"
        else:
            return "green"
    elif param == "SI":
        if val < 150:
            return "green"
        elif 150 <= val < 500:
            return "yellow"
        else:
            return "red"
    return None


def formatted_cell(param: str, value: Any) -> str:
    """
    Форматирует значение ячейки с цветовой разметкой для заданного параметра.
    Если значение пустое или не поддаётся преобразованию, возвращает пустую строку.
    """
    if not value:
        return ""
    color = get_color_for_param(param, value)
    if color:
        return f"[{color}]{value}[/{color}]"
    return str(value)


def load_data(file_path: str = "td_data.json") -> List[Dict[str, Any]]:
    """
    Загружает данные из JSON-файла.
    При возникновении ошибки загрузки выводит сообщение и возвращает пустой список.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as error:
        console.print(f"[red]Ошибка загрузки данных:[/red] {error}")
        return []


def group_data_by_device(data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Группирует записи по значению поля 'device_name'.
    Если поле отсутствует, используется 'Unknown'.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for entry in data:
        device_name = entry.get("device_name", "Unknown")
        grouped.setdefault(device_name, []).append(entry)
    return grouped


def build_device_table(
    data_points: List[Dict[str, Any]], config: DashboardConfig
) -> Table:
    """
    Создаёт таблицу для отображения данных устройства.
    Таблица выводит ровно config.data_row_count рядов данных,
    заполняя недостающие строки пустыми значениями.
    Столбцы и их порядок определяются конфигурацией.
    """
    table = Table(
        show_header=True, header_style="bold white", expand=False, box=box.SIMPLE
    )

    # Добавляем столбцы согласно конфигурации
    for col in config.columns_order:
        width = config.columns_width.get(col)
        table.add_column(col, justify="center", no_wrap=True, width=width)

    sorted_points: List[Dict[str, Any]] = []
    try:
        if data_points:
            sorted_points = sorted(
                data_points,
                key=lambda x: datetime.datetime.strptime(
                    x.get("timestamp", ""), "%Y-%m-%d %H:%M:%S"
                ),
            )
    except Exception as error:
        console.print(f"[red]Ошибка сортировки данных:[/red] {error}")
        sorted_points = data_points or []

    rows_to_display = sorted_points[-config.data_row_count:] if sorted_points else []

    # Сопоставление столбцов с ключами данных
    column_key_map = {
        "Time": "timestamp",
        "HR": "hr",
        "LF/HF": "lf_hf_ratio",
        "RMSSD": "rmssd",
        "SDRR": "sdrr",
        "SI": "si",
    }

    for point in rows_to_display:
        row_values = []
        for col in config.columns_order:
            key = column_key_map.get(col, col)
            value = point.get(key, "")
            if col == "Time" and value:
                try:
                    formatted_time = datetime.datetime.strptime(
                        value, "%Y-%m-%d %H:%M:%S"
                    ).strftime("%H:%M:%S")
                    value = formatted_time
                except Exception:
                    pass
            row_values.append(formatted_cell(col, value))
        table.add_row(*row_values)

    # Если записей меньше требуемого числа, дополняем пустыми строками
    for _ in range(config.data_row_count - len(rows_to_display)):
        table.add_row(*["" for _ in config.columns_order])

    return table


def build_device_panel(
    device_name: str, data_points: List[Dict[str, Any]], config: DashboardConfig
) -> Panel:
    """
    Оборачивает таблицу устройства в панель с фиксированной шириной.
    Высота панели автоматически определяется содержимым.
    """
    content = build_device_table(data_points, config)
    return Panel(content, title=device_name, width=config.panel_width, expand=False)


def build_reference_panel(config: DashboardConfig) -> Panel:
    """
    Создаёт панель со справочной информацией по параметрам и их цветовой разметке.
    """
    reference_text = (
        "[bold underline]Справочная информация:[/bold underline]\n"
        "[bold]HR:[/bold] 60–100 [green]зеленый[/green], <60 [blue]синий[/blue], >100 [red]красный[/red]\n"
        "[bold]LF/HF:[/bold] <1 [green]зеленый[/green], 1–2 [yellow]желтый[/yellow], >2 [red]красный[/red]\n"
        "[bold]RMSSD:[/bold] <20 [red]красный[/red], 20–50 [yellow]желтый[/yellow], ≥50 [green]зеленый[/green]\n"
        "[bold]SDRR:[/bold] <50 [red]красный[/red], 50–100 [yellow]желтый[/yellow], ≥100 [green]зеленый[/green]\n"
        "[bold]SI:[/bold] <50 [green]зеленый[/green], 50–100 [yellow]желтый[/yellow], ≥100 [red]красный[/red]"
    )
    return Panel(
        Text.from_markup(reference_text),
        title="Справка",
        width=config.panel_width,
        expand=False,
    )


def build_empty_panel(config: DashboardConfig) -> Panel:
    """
    Возвращает пустую панель с фиксированной шириной для заполнения пустых ячеек.
    """
    return Panel("", width=config.panel_width, expand=False, box=box.SIMPLE)


def build_generic_layout(
    panels: List[Panel],
    config: DashboardConfig,
    session: str,
    current_time: str,
    countdown: int,
) -> Table:
    """
    Формирует общий макет с заголовком и сеткой, определяемой конфигурацией.
    Заголовок содержит информацию о сессии, времени и обратном отсчёте.
    Если переданное число панелей меньше требуемых ячеек сетки,
    оставшиеся заполняются пустыми панелями.
    """
    grid_rows, grid_cols = config.grid_layout
    grid = Table.grid(padding=(0, 1))
    total_cells = grid_rows * grid_cols
    panels_extended = panels + [
        build_empty_panel(config) for _ in range(total_cells - len(panels))
    ]

    for row in range(grid_rows):
        row_items = panels_extended[row * grid_cols:(row + 1) * grid_cols]
        grid.add_row(*row_items)

    header = Table.grid(expand=True)
    header.add_column(justify="left")
    header.add_column(justify="center")
    header.add_column(justify="right")
    header.add_row(
        f"Session: {session}",
        f"Time: {current_time}",
        f"Next update in: {countdown} s",
    )

    layout = Table.grid(expand=True)
    layout.add_row(header)
    layout.add_row(grid)
    return layout


def main() -> None:
    """
    Запускает цикл обновления TUI с периодической перезагрузкой данных.
    """
    update_interval: int = 3  # Интервал обновления данных в секундах
    last_update: float = time.time() - update_interval
    config = DashboardConfig()  # Здесь можно изменить значения для кастомизации
    data: List[Dict[str, Any]] = load_data()
    session: str = data[0].get("session", "N/A") if data else "N/A"

    with Live(console=console, refresh_per_second=4, screen=True) as live:
        try:
            while True:
                now: float = time.time()
                countdown: int = max(0, update_interval -
                                     int(now - last_update))
                if countdown <= 0:
                    data = load_data()
                    session = data[0].get("session", "N/A") if data else "N/A"
                    last_update = now

                current_time: str = datetime.datetime.now().strftime("%H:%M:%S")
                grouped_data = group_data_by_device(data)
                device_panels = [
                    build_device_panel(
                        device, grouped_data.get(device, []), config)
                    for device in config.devices_to_display
                ]
                if config.include_reference_panel:
                    device_panels.append(build_reference_panel(config))

                layout = build_generic_layout(
                    device_panels, config, session, current_time, countdown
                )
                live.update(layout)
                time.sleep(1)
        except KeyboardInterrupt:
            console.print("[bold red]Exiting...[/bold red]")


if __name__ == "__main__":
    main()
