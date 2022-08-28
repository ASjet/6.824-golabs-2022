import sys
from rich.console import Console
from rich.table import Table


def parse(columns: int, line: str) -> tuple[list[str], bool]:
    row = [None] * (1 + columns)
    row[0] = line[16:21]
    num = int(line[23:24])
    content = line[25:-1]
    if num >= columns:
        content = f"[{num}]{content}"
    row[num % columns + 1] = content
    return row, "won" in content


def print_table(file: str, title: str, columns: int = 3, lines: int = -1) -> None:
    table = Table(title=title)

    table.add_column("LEVEL", justify="left", style="cyan", no_wrap=False)
    for i in range(columns):
        table.add_column(f"[{i}]", no_wrap=False)

    with open(file, "r") as f:
        end_section = False
        first = True
        last_row = []
        for num, line in enumerate(f.readlines()):
            if lines > 0 and num > lines:
                break
            if first:
                try:
                    last_row, won = parse(columns, line)
                    first = False
                except:
                    if line[22:26] == "Make":
                        continue
            else:
                try:
                    row, won = parse(columns, line)
                    table.add_row(*last_row, end_section=end_section or won)
                    last_row = row
                    end_section = False
                except:
                    if line[22:26] == "Make" and not end_section:
                        end_section = True
                    continue
        table.add_row(*last_row)

    console = Console()
    console.print(table)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print(f"Usage: {sys.argv[0]} <filename> [lines] [columns]")
        exit(0)
    filename = sys.argv[1]
    try:
        linenum = int(sys.argv[2])
    except:
        linenum = -1
    try:
        columns = int(sys.argv[3])
    except:
        columns = 3

    print_table(filename, "Raft Log", columns, linenum)
