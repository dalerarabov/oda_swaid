from rich.console import Console
from rich.panel import Panel
from datetime import datetime, timedelta
import random
import time

console = Console()

def generate_mock_data(t_now, duration=120, min_dt=1, max_dt=10):
    """Генерирует имитацию данных SI с неравномерным шагом ΔT."""
    data = []
    t = t_now - timedelta(seconds=duration)
    while t < t_now:
        si = random.randint(40, 70)  # Моделируем SI в диапазоне 40–70
        data.append((t, si))
        dt = random.randint(min_dt, max_dt)  # Шаг 1–10 секунд
        t += timedelta(seconds=dt)
    return data

def calculate_trend(data):
    """Определяет тренд на основе последних двух значений."""
    if len(data) < 2:
        return "→"
    last_si, prev_si = data[-1][1], data[-2][1]
    return "↑" if last_si > prev_si else "↓" if last_si < prev_si else "→"

def render_graph(data, t_now, width=48, height=10, si_max=100):
    """Рисует ASCII-график SI с использованием Rich."""
    grid = [[" " for _ in range(width)] for _ in range(height)]
    si_scale = si_max / (height - 1)
    time_scale = 120 / (width - 1)

    for t, si in data:
        seconds_ago = (t_now - t).total_seconds()
        if seconds_ago > 120:
            continue
        x = int(seconds_ago / time_scale)
        y = height - 1 - int(si / si_scale)
        if 0 <= y < height and 0 <= x < width:
            grid[y][x] = ">" if t == data[-1][0] else "*"

    lines = []
    for i in range(height):
        si_value = int((height - 1 - i) * si_scale)
        line = f"[white]{si_value:3d}[/] |"
        for char in grid[i]:
            if char == ">":
                line += "[red]>[/]"
            elif char == "*":
                line += "[white]*[/]"
            else:
                line += " "
        line += "|"
        lines.append(line)
    lines.append("[white]    +" + "-" * width + "+[/]")
    labels = ["-120", "-80", "-60", "-40", "-20", f"Tnow {t_now.strftime('%H:%M:%S')}"]
    label_positions = [0, 10, 19, 29, 38, width - 10]
    time_line = [" "] * width
    for label, pos in zip(labels, label_positions):
        for i, char in enumerate(label):
            if pos + i < width:
                time_line[pos + i] = char
    lines.append("[white]    " + "".join(time_line) + "[/]")
    return "\n".join(lines)

def render_info(data, t_now):
    """Формирует текстовую информацию о SI."""
    if not data:
        return "No data available."
    last_t, last_si = data[-1]
    seconds_ago = int((t_now - last_t).total_seconds())
    trend = calculate_trend(data)
    return (
        f"[bold]Last SI:[/] [cyan]{last_si}[/] (data from [yellow]{seconds_ago}s[/] ago, window [-80s, -20s])\n"
        f"[bold]Trend:[/] [magenta]{trend}[/]\n"
        f"[bold]Note:[/] Actions (e.g., push-ups) affect SI in [red]~1:20+[/]"
    )

def main():
    """Основная функция для отображения инфографики."""
    while True:
        t_now = datetime.now()
        data = generate_mock_data(t_now)
        
        console.clear()
        console.print(Panel("[bold]Stress Index (SI) Trend[/]", style="white"))
        console.print(Panel(render_graph(data, t_now), style="white"))
        console.print(Panel(render_info(data, t_now), style="white"))
        console.print("\n[yellow]Updating every 5 seconds...[/]")

        time.sleep(5)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[red]Stopped by user.[/]")