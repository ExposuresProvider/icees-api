import sys
import Levenshtein
import argparse
from argparse import RawTextHelpFormatter
from prettytable import PrettyTable
import difflib
from itertools import chain
import asyncio
import curses
import logging
from tx.functional.either import Left, Right
from window import Window, Pane, HIGHLIGHT, RESET, init_colors, SELECTED_NORMAL_COLOR
from file import make_file

APPLICATION_TITLE = "ICEES FHIR-PIT Configuration Tool"
HELP_TEXT = "TAB switch table UP DOWN PAGE UP PAGE DOWN navigate A use a B use b C customize S skip U update tables Q exit"

# from https://stackoverflow.com/a/6386990
logging.basicConfig(filename="qctool.log",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

logger = logging.getLogger(__name__)

class Noop:
    def __str__(self):
        return ""

    def update(self, name, key_a, file_a, key_b, file_b):
        pass

    
class UseA:
    def __str__(self):
        return "use a"

    def update(self, name, key_a, file_a, key_b, file_b):
        file_b.update_key(name, key_b, key_a)

        
class UseB:
    def __str__(self):
        return "use b"

    def update(self, name, key_a, file_a, key_b, file_b):
        file_a.update_key(name, key_a, key_b)

    
class Customize:
    def __init__(self, var_name):
        self.var_name = var_name

    def __str__(self):
        return f"customize: {self.var_name}"

    def update(self, name, key_a, file_a, key_b, file_b):
        file_a.update_key(name, key_a, self.var_name)
        file_b.update_key(name, key_b, self.var_name)

    
def difference_ignore_suffix(a, b, ignore_suffix):
    diff = []
    for an in a:
        found = False
        for bn in b:
            if an == bn or any([an == bn + suffix or an + suffix == bn for suffix in ignore_suffix]):
                found = True
                break
        if not found:
            diff.append(an)
    return diff
            
    
def truncate_set(a, b, a_only, b_only, similarity_threshold, n, ignore_suffix):
    diff_a = [] if b_only else difference_ignore_suffix(a, b, ignore_suffix)
    diff_b = [] if a_only else difference_ignore_suffix(b, a, ignore_suffix)

    def find_match(b, an):
        bns = [(bn, Levenshtein.ratio(an, bn)) for bn in b]
        if len(bns) == 0:
            bn = None
            ratio = 0
        else:
            bn, ratio = max(bns, key=lambda x: x[1])
        return (an, bn, ratio)

    diff_a_match = [find_match(b, an) for an in diff_a]
    diff_b_match = [find_match(a, bn) for bn in diff_b]

    diff_a_match_truncated = [(an, bn, ratio) if ratio >= similarity_threshold else (an, None, None) for an, bn, ratio in diff_a_match]
    diff_b_match_truncated = [(bn, an, ratio) if ratio >= similarity_threshold else (bn, None, None) for bn, an, ratio in diff_b_match]

    diff_b_match_switched = [(an, bn, ratio) for bn, an, ratio in diff_b_match_truncated]

    diff_a_match_dir = {(an, bn, ratio, "ab" if bn in diff_b else "a") for an, bn, ratio in diff_a_match_truncated}
    diff_b_match_dir = {(an, bn, ratio, "ab" if an in diff_a else "b") for an, bn, ratio in diff_b_match_switched}

    ls = sorted(list(diff_a_match_dir | diff_b_match_dir), reverse=True, key=lambda t: t[2] or 0)
    
    if n == -1:
        topn = ls
    else:
        topn = ls[:n]

    topnaction = list(map(lambda x: [x[0], x[1], x[2], x[3], Noop()], ls))
        
    return topnaction, n >= 0 and len(ls) > n


def colorize_diff(a, b):
    sm = difflib.SequenceMatcher(a=a, b=b, autojunk=False)
    opcodes = sm.get_opcodes()
    a_colorized = ""
    b_colorized = ""
    ab = HIGHLIGHT
    bb = HIGHLIGHT
    reset_all = RESET
    for tag, i1, i2, j1, j2 in opcodes:
        a_segment = a[i1:i2]
        b_segment = b[j1:j2]
        if tag == "equal":
            a_colorized += a_segment
            b_colorized += b_segment
        elif tag == "delete":
            a_colorized += ab + a_segment + reset_all
        elif tag == "insert":
            b_colorized += bb + b_segment + reset_all
        elif tag == "replace":
            a_colorized += ab + a_segment + reset_all
            b_colorized += bb + b_segment + reset_all
    return (a_colorized, b_colorized)
    
    
def to_prettytable(l):
    if l[0] is None:
        return ["", l[1], "", l[3], str(l[4])]
    elif l[1] is None:
        return [l[0], "", "", l[3], str(l[4])]
    else:
        return list(colorize_diff(l[0], l[1])) + [f"{l[2]:.2f}", l[3], str(l[4])]


def print_matches(window, left, right, a_type, b_type, a_only, b_only, a_update, b_update, table_names, similarity_threshold, max_entries, ignore_suffix):
    
    top_pane = window.children["top_pane"]
    left_pane = window.children["left_pane"]
    right_pane = window.children["right_pane"]
    horizontal_splitter = window.children["horizontal_splitter"]
    vertical_splitter = window.children["vertical_splitter"]
    
    ntables = len(table_names)
    current_table = 0

    def get_current_row_id():
        return max(0, top_pane.current_document_y - 3)

    def get_total_rows(tables):
        name = table_names[current_table]
        table, _ = tables[name]
        return len(table)
        
    def get_current_row(tables):
        name = table_names[current_table]
        table, ellipsis = tables[name]
        table_y = get_current_row_id()
        row = table[table_y] if table_y < len(table) else [None, None, None, None, None]
        return row

    def refresh_bottom_panes(a_file, b_file, tables):
        name = table_names[current_table]
        key_a, key_b, _, _, _ = get_current_row(tables)
        if key_a is None:
            dump_get_a = ""
        else:
            dump_get_a = a_file.dump_get(name, key_a)
    
        if key_b is None:
            dump_get_b = ""
        else:
            dump_get_b = b_file.dump_get(name, key_b)

        left_pane._clear()
        right_pane._clear()
        left_pane._append(dump_get_a)
        right_pane._append(dump_get_b)
        i = get_current_row_id() + 1
        n = get_total_rows(tables)
        if i > n: # i might be on the ...
            i = n

        footer = f"{HIGHLIGHT}{i} / {n}{RESET} {HELP_TEXT}"
        window._set_footer(footer)

        window.update()

    def refresh_content(a_file, b_file, tables):
        nav = f"{APPLICATION_TITLE} "
        for i, n in enumerate(table_names):
            if i > 0:
                nav += " "
            if i == current_table:
                nav += HIGHLIGHT
                nav += n
                nav += RESET
            else:
                nav += n
        window._set_header(nav)

        name = table_names[current_table]
        table, ellipsis = tables[name]
        table_copy = list(table)
        if ellipsis:
            table_copy.append(["...", None, None, "", Noop()])

        x = PrettyTable()
        x.field_names = [left, right, "ratio", "direction", "update"]

        x.add_rows(map(to_prettytable, table_copy))
        top_pane.top_padding = 3
        top_pane.bottom_padding = 1
        top_pane._replace(str(x))
        window.update()
        refresh_bottom_panes(a_file, b_file, tables)

    def refresh(a_file, b_file, tables):
        top_pane._clear()
        nav = f"{APPLICATION_TITLE} "
        for i, n in enumerate(table_names):
            if i > 0:
                nav += " "
            if i == current_table:
                nav += HIGHLIGHT
                nav += n
                nav += RESET
            else:
                nav += n
        window._set_header(nav)

        name = table_names[current_table]
        table, ellipsis = tables[name]
        table_copy = list(table)
        if ellipsis:
            table_copy.append(["...", None, None, "", Noop()])

        x = PrettyTable()
        x.field_names = [left, right, "ratio", "direction", "update"]

        x.add_rows(map(to_prettytable, table_copy))
        top_pane.top_padding = 3
        top_pane.bottom_padding = 1
        top_pane._append(str(x))
        window.update()
        refresh_bottom_panes(a_file, b_file, tables)

    def refresh_files():
        window.set_header(APPLICATION_TITLE)
        window.set_footer(f"loading {left} ...")
        try:
            a_file = make_file(a_type, left)
        except Exception as e:
            sys.stderr.write(f"error loading {left}: {e}\n")
            sys.exit(-1)

        window.set_footer(f"loading {right} ...")
        try:
            b_file = make_file(b_type, right)
        except Exception as e:
            sys.stderr.write(f"error loading {right}: {e}\n")
            sys.exit(-1)

        window.set_footer(f"comparing...")
        tables = {}
        for table in table_names:
            a_var_names = a_file.get_keys(table)

            b_var_names = b_file.get_keys(table)

            tables[table] = truncate_set(a_var_names, b_var_names, a_only, b_only, similarity_threshold, max_entries, ignore_suffix)

        refresh(a_file, b_file, tables)
        return a_file, b_file, tables

    a_file, b_file, tables = refresh_files()

    while True:

        ch = window.getch()
        if ch == curses.KEY_RESIZE:
            height, width = window.size()
            splitterx = width // 2
            splittery = height // 2
            top_height = max(0, splittery - 1)
            bottom_height = max(0, height - splittery - 2)
            left_width = splitterx
            right_width = width - splitterx - 1
            top_pane.resize_move(top_height, width, 1, 0)
            left_pane.resize_move(bottom_height, left_width, splittery + 1, 0)
            right_pane.resize_move(bottom_height, right_width, splittery + 1, splitterx + 1)
            horizontal_splitter.resize_move(1, width, splittery, 0)
            vertical_splitter.resize_move(bottom_height, 1, splittery + 1, splitterx)
            window.update()
        elif ch == curses.KEY_UP:
            top_pane.move(-1, 0)
            refresh_bottom_panes(a_file, b_file, tables)
        elif ch == curses.KEY_DOWN:
            top_pane.move(+1, 0)
            refresh_bottom_panes(a_file, b_file, tables)
        elif ch == curses.KEY_PPAGE:
            top_pane.move_page(-1, 0)
            refresh_bottom_panes(a_file, b_file, tables)
        elif ch == curses.KEY_NPAGE:
            top_pane.move_page(+1, 0)
            refresh_bottom_panes(a_file, b_file, tables)
        elif ch == ord("\t"):
            current_table += 1
            current_table %= ntables
            refresh(tables)
        elif ch == ord("a"):
            if b_update is not None:
                name = table_names[current_table]
                row = get_current_row(tables)
                row[4] = UseA()
                refresh_content(a_file, b_file, tables)
        elif ch == ord("b"):
            if a_update is not None:
                name = table_names[current_table]
                row = get_current_row(tables)
                row[4] = UseB()
                refresh_content(a_file, b_file, tables)
        elif ch == ord("c"):
            if a_update is not None or b_update is not None:
                name = table_names[current_table]
                row = get_current_row(tables)
                key_a, key_b, _, _, _ = row

                window.set_footer("ENTER confirm ESCAPE exit")
                h, w = window.size()
                popw = w * 4 // 5
                poph = 5
                popy = (h - 5) // 2
                popx = w // 10
                pop = curses.newwin(poph, popw, popy, popx)
                c = ""

                def refresh_customization():
                    pop.clear()
                    pop.insstr(0,0,"-" * popw)
                    pop.insstr(1,0,f"|a: {key_a}")
                    pop.insstr(2,0,f"|b: {key_b}")
                    pop.insstr(3,0,f"|c: ")
                    pop.insstr(4,0,"-" * popw)
                    text = c[-popw+6:]
                    padding = " " * (popw - 5)
                    pop.insstr(3,4, padding, curses.color_pair(SELECTED_NORMAL_COLOR))
                    for i in range(1,4):
                        pop.insstr(i,popw-1, "|")
                    pop.addstr(3,4,text, curses.color_pair(SELECTED_NORMAL_COLOR))
                    pop.refresh()

                refresh_customization()

                while True:
                    ch = pop.getch()
                    if ch == curses.KEY_ENTER or ch == ord("\n") or ch == ord("\r"):
                        break
                    elif ch == curses.KEY_BACKSPACE or ch == ord("\b") or ch == 127:
                        c = c[:-1]
                        refresh_customization()
                    elif ch == 27:
                        c = None
                        break
                    else:
                        c += chr(ch)
                        refresh_customization()
                del pop

                if c is not None:
                    row[4] = Customize(c)
                refresh_content(a_file, b_file, tables)
        elif ch == ord("s"):
            if a_update is not None:
                name = table_names[current_table]
                row = get_current_row(tables)
                row[4] = Noop()
                refresh_content(a_file, b_file, tables)
        elif ch == ord("u"):
            for name, (table, _) in tables.items():
                window.set_footer(f"updating table {name}...")
                for row in table:
                    key_a, key_b, _, _, action = row
                    action.update(name, key_a, a_file, key_b, b_file)
                    row[4] = Noop()
                if a_update is not None:
                    a_file.dump(a_update)
                if b_update is not None:
                    b_file.dump(b_update)
            file_a, file_b, tables = refresh_files()
        elif ch == ord("q"):
            break
        else:
            continue


def curses_main(stdscr, args):
    a_filename = args.a
    a_type = args.a_type
    a_update = args.update_a
    a_only = args.a_only

    b_filename = args.b
    b_type = args.b_type
    b_update = args.update_b
    b_only = args.b_only

    tables = args.table
    max_entries = args.number_entries
    ignore_suffix = args.ignore_suffix
    similarity_threshold = args.similarity_threshold

    init_colors()
    height, width = stdscr.getmaxyx()
    window = Window(stdscr)
    splitterx = width // 2
    splittery = height // 2
    top_height = max(0, splittery - 1)
    bottom_height = max(0, height - splittery - 2)
    left_width = splitterx
    right_width = width - splitterx - 1
    top_pane = window.pane("top_pane", top_height, width, 1, 0, True)
    left_pane = window.pane("left_pane", bottom_height, left_width, splittery + 1, 0, False)
    right_pane = window.pane("right_pane", bottom_height, right_width, splittery + 1, splitterx + 1, False)
    horizontal_splitter = window.fill("horizontal_splitter", 1, width, splittery, 0, "-")
    vertical_splitter = window.fill("vertical_splitter", bottom_height, 1, splittery + 1, splitterx, "|")

    print_matches(window, a_filename, b_filename, a_type, b_type, a_only, b_only, a_update, b_update, tables, similarity_threshold, max_entries, ignore_suffix)


def main():
    parser = argparse.ArgumentParser(description="""ICEES FHIR-PIT QC Tool

Compare feature variable names in two files. Use --a and --b to specify filenames, --a_type and --b_type to specify file types, --update_a and --update_b to specify output files. Files types are one of features, mapping, and identifiers. If --update_a or --update_b is not specified then the files cannot be updated.""", formatter_class=RawTextHelpFormatter)
    parser.add_argument('--a', metavar='A', type=str, required=True,
                        help='file a')
    parser.add_argument('--b', metavar='B', type=str, required=True,
                        help='file b')
    parser.add_argument('--a_type', metavar='A_TYPE', choices=["features", "mapping", "identifiers"], required=True,
                        help='type of file a')
    parser.add_argument('--b_type', metavar='B_TYPE', choices=["features", "mapping", "identifiers"], required=True,
                        help='type of file b')
    parser.add_argument('--a_only', default=False, action="store_true",
                        help='only show variable names in a that are not in b')
    parser.add_argument('--b_only', default=False, action="store_true",
                        help='only show variable names in b that are not in a')
    parser.add_argument('--number_entries', metavar='NUMBER_ENTRIES', type=int, default=-1,
                        help='number of entries to display, -1 for unlimited')
    parser.add_argument('--ignore_suffix', metavar='IGNORE_SUFFIX', type=str, default=[], nargs="*",
                        help='the suffix to ignore')
    parser.add_argument("--similarity_threshold", metavar="SIMILARITY_THRESHOLD", type=float, default=0.5,
                        help="the threshold for similarity suggestions")
    parser.add_argument('--table', metavar='TABLE', type=str, required=True, nargs="+",
                        help='tables')
    parser.add_argument("--update_a", metavar="UPDATE_A", type=str,
                        help="YAML file for the updated a. If this file is not specified then a cannot be updated")
    parser.add_argument("--update_b", metavar="UPDATE_B", type=str,
                        help="YAML file for the updated b. If this file is not specified then b cannot be updated")

    args = parser.parse_args()

    curses.wrapper(lambda stdscr: curses_main(stdscr, args))


if __name__ == "__main__":
    main()

