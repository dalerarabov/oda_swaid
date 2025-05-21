#!/usr/bin/env python3
"""
TUI Dashboard для отображения графиков данных устройств в реальном времени.
Пользователь по-прежнему может задавать:
  - Список устройств (панели), которые выводятся на экран.
  - Ширину панелей (на основе которой адаптируется ширина графика).
  - Варианты компоновки панелей (например, 2x6, 3x2 и т.д.).

Визуализация графика:
  - Ось X – временная шкала в секундах, показывающая последние 80 секунд.
    Правый край помечен как Now с текущим временем (ЧЧ:ММ:СС), а отметки на 20, 40, 60, 80 секунд назад фиксированы.
  - Ось Y (левая) – параметр HR (55–200 уд./мин).
  - Ось Y (правая) – параметр SI (50–900 усл. ед.).
  - На графике выводятся только данные, попадающие в окно [Now - 80, Now].
  - Старые данные автоматически удаляются, а новые добавляются с корректной временной привязкой.
  - График адаптируется под ширину окна терминала, а значения на оси Y выводятся реже (каждый второй ряд) с округлением.
"""

import json
import time
import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
# используется для построения макета, а не самого графика
from rich.table import Table
from rich.text import Text
from rich import box

console = Console()


@dataclass
class DashboardConfig:
    # Начальное значение, которое будет перенастроено динамически в цикле обновления.
    panel_width: int = 60
    graph_height: int = 10  # высота графика (число строк)
    devices_to_display: List[str] = field(
        default_factory=lambda: [
            "swaid 1319",
            "swaid 1341",
            "swaid 1330",
            "swaid 1327",
            "swaid 1329",
            "swaid 1336",
        ]
    )
    grid_layout: Tuple[int, int] = (2, 6)  # (строк, колонок)
    # можно добавить справочную панель, если нужно
    include_reference_panel: bool = False


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


def build_device_graph(device_name: str, data_points: List[Dict[str, Any]], config: DashboardConfig) -> Panel:
    """
    Строит график для устройства на основе данных за последние 80 секунд.
    По оси X изображён временной интервал: от (Now - 80 сек) до Now.
    По оси Y накладываются два графика:
      - HR (уд./мин) (левая шкала, диапазон 55–200): отмечается символом [red]*[/red]
      - SI (услов. ед.) (правая шкала, диапазон 50–900): отмечается символом [cyan]o[/cyan]
    Если два значения попадают в одну и ту же ячейку, ставится комбинированный символ [bold magenta]X[/bold magenta].
    """
    now = datetime.datetime.now()
    # Задаём отступы для подписей по осям
    left_margin = 6    # для меток HR
    right_margin = 6   # для меток SI
    drawing_width = config.panel_width - left_margin - right_margin
    plot_height = config.graph_height

    # Инициализируем пустой графический "холст"
    grid = [[" " for _ in range(drawing_width)] for _ in range(plot_height)]

    # Отбираем данные, попадающие в окно последних 80 секунд
    valid_points = []
    for point in data_points:
        ts_str = point.get("timestamp", "")
        try:
            ts = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        diff = (now - ts).total_seconds()
        if 0 <= diff <= 80:
            valid_points.append((diff, point))

    # Для каждого измерения вычисляем координаты графика
    for diff, point in valid_points:
        # Ось X: 0 соответствует now-80 сек, drawing_width-1 — now
        x = int((80 - diff) / 80 * (drawing_width - 1))

        # Обработка HR
        hr_val = point.get("hr")
        if hr_val is not None:
            try:
                hr = float(hr_val)
                norm_hr = (hr - 55) / (200 - 55)
                norm_hr = max(0.0, min(1.0, norm_hr))
                y_hr = plot_height - 1 - int(norm_hr * (plot_height - 1))
            except Exception:
                y_hr = None
            if y_hr is not None:
                current_cell = grid[y_hr][x]
                if current_cell.strip() == "":
                    grid[y_hr][x] = "[red]*[/red]"
                else:
                    grid[y_hr][x] = "[bold magenta]X[/bold magenta]"

        # Обработка SI
        si_val = point.get("si")
        if si_val is not None:
            try:
                si = float(si_val)
                norm_si = (si - 50) / (900 - 50)
                norm_si = max(0.0, min(1.0, norm_si))
                y_si = plot_height - 1 - int(norm_si * (plot_height - 1))
            except Exception:
                y_si = None
            if y_si is not None:
                current_cell = grid[y_si][x]
                if current_cell.strip() == "":
                    grid[y_si][x] = "[cyan]o[/cyan]"
                else:
                    grid[y_si][x] = "[bold magenta]X[/bold magenta]"

    # Формирование строк для графика с подписями по оси Y.
    # Подписи выводятся только для каждой второй строки (остальные строки оставляются пустыми)
    lines = []
    for row in range(plot_height):
        hr_label_val = 200 - ((200 - 55) / (plot_height - 1)) * row
        si_label_val = 900 - ((900 - 50) / (plot_height - 1)) * row
        if row % 2 == 0:
            left_label = f"{round(hr_label_val):>3}"
            right_label = f"{round(si_label_val):>3}"
        else:
            left_label = "   "
            right_label = "   "
        content_line = "".join(grid[row])
        lines.append(f"{left_label} {content_line} {right_label}")

    # Рисуем ось X с горизонтальной линией и фиксированными метками
    x_axis_line = " " * left_margin + "-" * drawing_width
    tick_values = [80, 60, 40, 20, 0]
    tick_labels = ["-80", "-60", "-40", "-20",
                   datetime.datetime.now().strftime("%H:%M:%S")]
    tick_line_list = [" " for _ in range(drawing_width)]
    for tick, label in zip(tick_values, tick_labels):
        pos = int((80 - tick) / 80 * (drawing_width - 1))
        for i, ch in enumerate(label):
            if pos + i < drawing_width:
                tick_line_list[pos + i] = ch
    tick_line = " " * left_margin + "".join(tick_line_list)
    axis_label = "Время: сек"
    offset = left_margin + (drawing_width - len(axis_label)) // 2
    x_axis_label_line = " " * offset + axis_label

    graph_text = "\n".join(lines + [x_axis_line, tick_line, x_axis_label_line])
    return Panel(graph_text, title=device_name, width=config.panel_width, expand=False)


def build_reference_panel(config: DashboardConfig) -> Panel:
    """
    Создаёт панель со справочной информацией по графику.
    """
    reference_text = (
        "[bold underline]Справка по графику:[/bold underline]\n"
        "Ось X: последние 80 секунд. Отметки: -80, -60, -40, -20, Now (текущее время)\n"
        "Левая ось Y: HR, 55–200 уд./мин (отмечается символом [red]*[/red])\n"
        "Правая ось Y: SI, 50–900 усл. ед. (отмечается символом [cyan]o[/cyan])\n"
        "Если оба показателя попадают в одну ячейку, отображается [bold magenta]X[/bold magenta]."
    )
    return Panel(Text.from_markup(reference_text), title="Справка", width=config.panel_width, expand=False)


def build_empty_panel(config: DashboardConfig) -> Panel:
    """
    Возвращает пустую панель для заполнения пустых ячеек в макете.
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
    Заголовок содержит информацию о сессии, времени и оставшемся обратном отсчёте.
    Если число панелей меньше количества ячеек в сетке,
    оставшиеся заполняются пустыми панелями.
    """
    grid_rows, grid_cols = config.grid_layout
    grid = Table.grid(padding=(0, 1))
    total_cells = grid_rows * grid_cols
    panels_extended = panels + \
        [build_empty_panel(config) for _ in range(total_cells - len(panels))]

    for row in range(grid_rows):
        row_items = panels_extended[row * grid_cols: (row + 1) * grid_cols]
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
    Вместо таблиц выводятся графики, обновляемые в реальном времени.
    """
    update_interval: int = 3  # интервал обновления данных в секундах
    last_update: float = time.time() - update_interval
    config = DashboardConfig()  # начальные настройки (ширина, список устройств и т.д.)
    data: List[Dict[str, Any]] = load_data()
    session: str = data[0].get("session", "N/A") if data else "N/A"

    with Live(console=console, refresh_per_second=4, screen=True) as live:
        try:
            while True:
                now = time.time()
                countdown: int = max(0, update_interval -
                                     int(now - last_update))
                if countdown <= 0:
                    data = load_data()
                    session = data[0].get("session", "N/A") if data else "N/A"
                    last_update = now

                # Адаптируем ширину панелей под текущий размер терминала.
                grid_rows, grid_cols = config.grid_layout
                term_width = console.size.width
                # Рассчитываем новую ширину для каждой панели с учетом небольшого отступа.
                adjusted_panel_width = max(30, (term_width // grid_cols) - 2)
                config.panel_width = adjusted_panel_width

                current_time_str = datetime.datetime.now().strftime("%H:%M:%S")
                grouped_data = group_data_by_device(data)
                device_panels = [
                    build_device_graph(
                        device, grouped_data.get(device, []), config)
                    for device in config.devices_to_display
                ]
                if config.include_reference_panel:
                    device_panels.append(build_reference_panel(config))

                layout = build_generic_layout(
                    device_panels, config, session, current_time_str, countdown)
                live.update(layout)
                time.sleep(1)
        except KeyboardInterrupt:
            console.print("[bold red]Exiting...[/bold red]")


if __name__ == "__main__":
    main()
