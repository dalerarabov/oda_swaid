import json
import time
import datetime
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.columns import Columns
from rich.text import Text
from rich import box

console = Console()

# Глобальные параметры для задания размеров таблиц
DATA_ROW_COUNT: int = 7  # число строк данных в таблице (без заголовка)
# высота панели = число строк данных + строка заголовка таблицы
PANEL_HEIGHT: int = DATA_ROW_COUNT + 7
PANEL_WIDTH: int = 60  # ширина всех блоков

###############################################################################
# Цветовое форматирование ячеек
###############################################################################


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
        if val < 20:
            return "red"
        elif 20 <= val < 50:
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
        if val < 50:
            return "green"
        elif 50 <= val < 100:
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

###############################################################################
# Загрузка и группировка данных
###############################################################################


def load_data(file_path: str = "td_data.json") -> List[Dict[str, Any]]:
    """
    Загружает данные из JSON-файла.

    При возникновении ошибки загрузки функция выводит сообщение в консоль
    и возвращает пустой список.
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

    Если для записи отсутствует поле 'device_name', используется значение 'Unknown'.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for entry in data:
        device_name = entry.get("device_name", "Unknown")
        grouped.setdefault(device_name, []).append(entry)
    return grouped

###############################################################################
# Построение таблиц и панелей устройств
###############################################################################


def build_device_table(data_points: List[Dict[str, Any]]) -> Table:
    """
    Создает Rich-таблицу для отображения данных устройства.

    Таблица имеет фиксированную ширину столбцов и выводит ровно DATA_ROW_COUNT строк
    данных. Если данных меньше, недостающие строки заполняются пустыми значениями.

    Столбцы:
      - Time  : ширина 15 символов
      - HR    : ширина 8 символов
      - LF/HF : ширина 7 символов
      - RMSSD : ширина 8 символов
      - SDRR  : ширина 7 символов
      - SI    : ширина 8 символов
    """
    table = Table(
        show_header=True,
        header_style="bold magenta",
        expand=False,
        box=box.SIMPLE,
    )
    table.add_column("Time", justify="center", no_wrap=True, width=15)
    table.add_column("HR", justify="center", no_wrap=True, width=8)
    table.add_column("LF/HF", justify="center", width=7)
    table.add_column("RMSSD", justify="center", width=8)
    table.add_column("SDRR", justify="center", width=7)
    table.add_column("SI", justify="center", width=8)

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

    # Берем последние DATA_ROW_COUNT записей
    rows_to_display = sorted_points[-DATA_ROW_COUNT:] if sorted_points else []

    for point in rows_to_display:
        try:
            ts = (
                datetime.datetime.strptime(
                    point.get("timestamp", ""), "%Y-%m-%d %H:%M:%S")
                .strftime("%H:%M:%S")
                if point.get("timestamp")
                else ""
            )
        except Exception:
            ts = point.get("timestamp", "")
        table.add_row(
            formatted_cell("Time", ts),
            formatted_cell("HR", point.get("hr", "")),
            formatted_cell("LF/HF", point.get("lf_hf_ratio", "")),
            formatted_cell("RMSSD", point.get("rmssd", "")),
            formatted_cell("SDRR", point.get("sdrr", "")),
            formatted_cell("SI", point.get("si", "")),
        )
    # Если записей меньше требуемого количества, дополняем пустыми строками
    for _ in range(DATA_ROW_COUNT - len(rows_to_display)):
        table.add_row("", "", "", "", "", "")
    return table


def build_device_panel(device_name: str, data_points: List[Dict[str, Any]]) -> Panel:
    """
    Оборачивает таблицу устройства в панель с фиксированными размерами.

    Панель имеет ширину PANEL_WIDTH и высоту PANEL_HEIGHT,
    где PANEL_HEIGHT = DATA_ROW_COUNT + 1 (учитывается строка заголовка таблицы).
    """
    content = build_device_table(data_points)
    return Panel(
        content,
        title=device_name,
        width=PANEL_WIDTH,
        height=PANEL_HEIGHT,
        expand=False,
    )

###############################################################################
# Построение панели со справкой и пустых панелей
###############################################################################


def build_reference_panel() -> Panel:
    """
    Создает панель со справочной информацией по параметрам и их цветовой разметке.

    Панель имеет фиксированные размеры: ширина PANEL_WIDTH, высота PANEL_HEIGHT.
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
        width=PANEL_WIDTH,
        height=PANEL_HEIGHT,
        expand=False,
    )


def build_empty_panel() -> Panel:
    """
    Возвращает пустую панель фиксированного размера.

    Используется для заполнения пустых ячеек в макете.
    """
    return Panel("", width=PANEL_WIDTH, height=PANEL_HEIGHT, expand=False, box=box.SIMPLE)

###############################################################################
# Построение общего макета (3×3)
###############################################################################


def build_layout(
    data: List[Dict[str, Any]], session: str, current_time: str, countdown: int
) -> Table:
    """
    Формирует общий макет TUI с заголовком и сеткой 3×3 блоков.

    Расположение блоков:
      1-я строка: [Устройство 1, Устройство 2, Справка]
      2-я строка: [Устройство 3, Устройство 4, пустой блок]
      3-я строка: [Устройство 5, Устройство 6, пустой блок]

    Заголовок (с информацией о сессии, времени и обратном отсчете) не входит в размер блоков.
    """
    grouped_data = group_data_by_device(data)
    devices = [
        "swaid 1319",
        "swaid 1341",
        "swaid 1330",
        "swaid 1327",
        "swaid 1329",
        "swaid 1336",
    ]
    device_panels = [
        build_device_panel(device, grouped_data.get(device, [])) for device in devices
    ]

    # Формируем ряды:
    # 1-я строка: устройство 1, устройство 2, справочная панель.
    # 2-я строка: устройство 3, устройство 4, пустая панель.
    # 3-я строка: устройство 5, устройство 6, пустая панель.
    row1 = [device_panels[0], device_panels[1], build_reference_panel()]
    row2 = [device_panels[2], device_panels[3], build_empty_panel()]
    row3 = [device_panels[4], device_panels[5], build_empty_panel()]

    grid = Table.grid(padding=(0, 1))
    grid.add_row(*row1)
    grid.add_row(*row2)
    grid.add_row(*row3)

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

###############################################################################
# Основной цикл обновления TUI
###############################################################################


def main() -> None:
    """
    Запускает цикл обновления TUI с периодической перезагрузкой данных.

    Интервал обновления задается в секундах.
    """
    update_interval: int = 1  # Интервал обновления данных в секундах
    last_update: float = time.time() - update_interval
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
                layout = build_layout(data, session, current_time, countdown)
                live.update(layout)
                time.sleep(1)
        except KeyboardInterrupt:
            console.print("[bold red]Exiting...[/bold red]")


if __name__ == "__main__":
    main()
