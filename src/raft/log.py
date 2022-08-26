import sys
from rich.console import Console
from rich.table import Table


def parse(line: str) -> list[str]:
    row = [None] * 4
    row[0] = line[16:21]
    num = int(line[23:24])
    row[num + 1] = line[25:-1]
    return row


def print_table(file: str, title: str, lines: int = -1) -> None:
    table = Table(title=title)

    table.add_column("LEVEL", justify="left", style="cyan", no_wrap=False)
    table.add_column("[0]", no_wrap=False)
    table.add_column("[1]", no_wrap=False)
    table.add_column("[2]", no_wrap=False)

    with open(file, "r") as f:
        end_section = False
        first = True
        last_row = []
        line_cnt = 0
        for line in f.readlines():
            if first:
                try:
                    last_row = parse(line)
                    first = False
                except:
                    if line[22:26] == "Make":
                        continue
            else:
                try:
                    row = parse(line)
                    table.add_row(*last_row, end_section=end_section)
                    last_row = row
                    end_section = False
                except:
                    if line[22:26] == "Make" and not end_section:
                        end_section = True
                    continue
            if lines > 0 and line_cnt > lines:
                break
            line_cnt += 1

    console = Console()
    console.print(table)


if __name__ == "__main__":
    filename = sys.argv[1]
    try:
        linenum = int(sys.argv[2])
    except:
        linenum = -1

    print_table(filename, "Raft Log", linenum)
