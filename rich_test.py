import signal
import sys
from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text
from rich.live import Live
import time


def create_six_column_layout(console):
    # Create a layout
    layout = Layout()

    # Get console width to help with adaptive sizing
    console_width = console.width

    # Calculate a minimum size for each column (e.g., at least 10 characters)
    min_column_width = max(10, console_width // 8)

    # Split layout into 6 columns with equal ratios for adaptability
    layout.split_row(
        Layout(name="col1", minimum_size=min_column_width),
        Layout(name="col2", minimum_size=min_column_width),
        Layout(name="col3", minimum_size=min_column_width),
        Layout(name="col4", minimum_size=min_column_width),
        Layout(name="col5", minimum_size=min_column_width),
        Layout(name="col6", minimum_size=min_column_width),
    )

    # Add content to each column
    for i in range(1, 7):
        layout[f"col{i}"].update(
            Panel(
                Text(f"Column {i}\nSample Content\nAdaptive Width",
                     justify="center"),
                title=f"Column {i}",
                border_style="blue"
            )
        )

    return layout


def main():
    console = Console()

    # Create a Live context for dynamic updates
    with Live(console=console, auto_refresh=False, transient=True) as live:
        def signal_handler(sig, frame):
            # Handle Ctrl+C gracefully
            live.stop()
            console.clear()
            sys.exit(0)

        # Register Ctrl+C handler
        signal.signal(signal.SIGINT, signal_handler)

        # Main loop to check for window resize
        last_size = console.size
        while True:
            current_size = console.size
            if current_size != last_size:
                # Update layout on resize
                layout = create_six_column_layout(console)
                live.update(layout)
                last_size = current_size
            live.refresh()
            time.sleep(0.1)  # Small delay to reduce CPU usage


if __name__ == "__main__":
    main()
