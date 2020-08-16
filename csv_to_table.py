import sys
import csv
from tabulate import tabulate

[inputpath] = sys.argv[1:]

with open(inputpath, newline="") as f:
    cr = csv.reader(f)
    rows = list(cr)

print(tabulate(rows, tablefmt="fancy_grid"))
