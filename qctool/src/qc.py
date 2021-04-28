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
import os
from functools import reduce
from tx.functional.either import Left, Right
from window import Window, Pane, HIGHLIGHT, RESET, init_colors, SELECTED_NORMAL_COLOR, popup, draw_textfield, WindowExit, WindowPass, WindowContinue
from file import make_file

APPLICATION_TITLE = "ICEES FHIR-PIT Configuration Tool"
HELP_TEXT_SHORT = "H help U update tables Q exit "
HELP_TEXT_LONG = """
COMMA     previous table        PERIOD    next table 
UP        move up               PAGE UP   page up
DOWN      move down             PAGE DOWN page down
A         use a                 B         use b 
D         customize b           E         customize a
C         customize             S         skip
F         pick candidate from a G         pick candidate from b 
U         update tables         H         help
Q         quit

In the a and b columns
          variable exists in other file
x         variable doesn't exist in other file
o         variable is a candidate
"""

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

    
class CustomizeA:
    def __init__(self, var_name):
        self.var_name = var_name

    def __str__(self):
        return f"customize a: {self.var_name}"

    def update(self, name, key_a, file_a, key_b, file_b):
        file_a.update_key(name, key_a, self.var_name)

    
class CustomizeB:
    def __init__(self, var_name):
        self.var_name = var_name

    def __str__(self):
        return f"customize b: {self.var_name}"

    def update(self, name, key_a, file_a, key_b, file_b):
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
            
    
def find_candidates(a, bn, similarity_threshold, n, ignore_suffix):
    ans = [(an, Levenshtein.ratio(an, bn)) for an in a]
    return sorted(ans, reverse=True, key=lambda t: t[1])


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

    diff_a_match_dir = {(an, bn, ratio, "x", "x" if bn in diff_b else "") for an, bn, ratio in diff_a_match_truncated}
    diff_b_match_dir = {(an, bn, ratio, "x" if an in diff_a else "", "x") for an, bn, ratio in diff_b_match_switched}

    ls = sorted(list(diff_a_match_dir | diff_b_match_dir), reverse=True, key=lambda t: t[2] or 0)
    
    if n == -1:
        topn = ls
    else:
        topn = ls[:n]

    topnaction = list(map(lambda x: [x[0], x[1], x[2], x[3], x[4], Noop()], ls))
        
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
        return ["", l[1], "", l[3], l[4], str(l[5])]
    elif l[1] is None:
        return [l[0], "", "", l[3], l[4], str(l[5])]
    else:
        return list(colorize_diff(l[0], l[1])) + [f"{l[2]:.2f}", l[3], l[4], str(l[5])]

    
def help(window):
    def create_window(popwindow):
        poph, popw = popwindow.size()
        text = popwindow.text("text_pane", poph - 1, popw - 1, 1, 1, HELP_TEXT_LONG)

    def key_handler(win, ch):
        return WindowExit(None) if ch == 27 else WindowPass()

    window.popup("ESCAPE exit", create_window, key_handler, None)

    
def enter_var_name(window, key_a, key_b):

    def create_window(popwindow):
        _, popw = popwindow.size()
        a_text = popwindow.text("a_text", 1, popw - 2, 1, 1, f"a: {key_a}")
        b_text = popwindow.text("b_text", 1, popw - 2, 2, 1, f"b: {key_b}")
        c_textfield = popwindow.textfield("c_textfield", popw - 2, 3, 1, "c:", "")
        popwindow.focus = "c_textfield"

    def key_handler(window, ch):
        if ch == curses.KEY_ENTER or ch == ord("\n") or ch == ord("\r"):
            c_textfield = window.children["c_textfield"]
            return WindowExit(c_textfield.text)
        elif ch == 27:
            return WindowExit(None)
        else:
            return WindowPass()

    c = window.popup("ENTER confirm ESCAPE exit", create_window, key_handler, h = 5)

    return c


def choose_candidate(window, candidates):
    help_text_short = "UP DOWN PAGE UP PAGE DOWN navigate ENTER confirm ESCAPE exit"

    candidates_w = max(len("candidate"), max(len(candidate) for candidate, _ in candidates)) + 2
    ratios_w = len("ratio") + 2
    column_ws = [candidates_w, ratios_w]
    table_w = reduce(lambda x, y: x + y + 1, column_ws, 0)
    headers = ["candidate", "ratio"]

    def format_row(row, column_ws):
        return "|".join([elem.center(w) for elem, w in zip(row, column_ws)])
        
    header_lines = [
        format_row(headers, column_ws),
        "-" * table_w
    ]
    header = "\n".join(header_lines)
    
    def format_table(candidates):
    
        lines = [format_row([candidate, f"{ratio:.2f}"], column_ws) for candidate, ratio in candidates] + ["-" * table_w]

        content = "\n".join(lines)

        return content

    content = format_table(candidates)
    _, w = window.size()
    popw = min(len(header.split("\n")[0]) + 2, w * 4 // 5)

    popwindow = None

    def match(c, candidate):
        return c.lower() in candidate.lower()
        
    def update_content(source, oc, c):
        popcontent_pane = popwindow.children["content_pane"]
        candidates_filtered = [(candidate, ratio) for candidate, ratio in candidates if match(c, candidate)]
        content = format_table(candidates_filtered)
        popcontent_pane._replace(content)

    def create_window(window):
        nonlocal popwindow
        popwindow = window
        popwindow.window.keypad(1)
        poph, popw = popwindow.size()
        popheader_pane = popwindow.pane("header_pane", 3, popw - 2, 1, 1, False)
        popcontent_pane = popwindow.pane("content_pane", poph - 5, popw - 2, 3, 1, True)
        popcontent_pane.bottom_padding = 1
        popsearch_textfield = popwindow.textfield("search_textfield", popw - 2, poph - 2, 1, "search:", "")
        popsearch_textfield.addChangeHandler(update_content)
        popwindow.focus = "search_textfield"

        popheader_pane._replace(header)
        popcontent_pane._replace(content)

    def candidates_get_current_row_id(popcontent_pane):
        return max(0, popcontent_pane.current_document_y)

    def key_handler(popwindow, ch):
        popcontent_pane = popwindow.children["content_pane"]
        search_textfield = popwindow.children["search_textfield"]

        if ch == curses.KEY_ENTER or ch == ord("\n") or ch == ord("\r"):
            i = candidates_get_current_row_id(popcontent_pane)
            c = candidates[i]
            return WindowExit(c)
        elif ch == 27:
            return WindowExit(None)

    c = window.popup(help_text_short, create_window, key_handler, w=popw)

    return c


def print_matches(window, left, right, a_type, b_type, a_only, b_only, a_update, b_update, table_names, similarity_threshold, max_entries, ignore_suffix):
    
    header_pane = window.children["header_pane"]
    top_pane = window.children["top_pane"]
    left_pane = window.children["left_pane"]
    right_pane = window.children["right_pane"]
    horizontal_splitter = window.children["horizontal_splitter"]
    vertical_splitter = window.children["vertical_splitter"]
    
    ntables = len(table_names)
    current_table = 0

    def get_current_row_id():
        return max(0, top_pane.current_document_y)

    def get_total_rows(tables):
        name = table_names[current_table]
        table, _ = tables[name]
        return len(table)
        
    def get_current_row(tables):
        name = table_names[current_table]
        table, ellipsis = tables[name]
        table_y = get_current_row_id()
        row = table[table_y] if table_y < len(table) else None
        return row

    def refresh_bottom_panes(a_file, b_file, tables):
        name = table_names[current_table]
        row = get_current_row(tables)
        if row is not None:
            key_a, key_b, _, _, _, _ = row
            if key_a is None:
                dump_get_a = ""
            else:
                dump_get_a = a_file.dump_get(name, key_a)

            if key_b is None:
                dump_get_b = ""
            else:
                dump_get_b = b_file.dump_get(name, key_b)
        else:
            dump_get_a = ""
            dump_get_b = ""
            
        left_pane._clear()
        right_pane._clear()
        left_pane._append(dump_get_a)
        right_pane._append(dump_get_b)
        i = get_current_row_id() + 1
        n = get_total_rows(tables)
        if i > n: # i might be on the ...
            i = n

        footer = f"{HIGHLIGHT}{i} / {n}{RESET} {HELP_TEXT_SHORT}"
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
        x.field_names = [left, right, "ratio", "a", "b", "update"]

        x.add_rows(map(to_prettytable, table_copy))
        top_pane.bottom_padding = 1
        lines = str(x).split("\n")
        header = "\n".join(lines[:3])
        content = "\n".join(lines[3:])
        header_pane._replace(header)
        top_pane._replace(content)
        window.update()
        refresh_bottom_panes(a_file, b_file, tables)

    def refresh(a_file, b_file, tables):
        refresh_content(a_file, b_file, tables)
        top_pane._move_abs(0,0)
        window.update()

    def refresh_files(a_filename, b_filename):
        window.set_header(APPLICATION_TITLE)
        window.set_footer(f"loading {a_filename} ...")
        try:
            a_file = make_file(a_type, a_filename)
        except Exception as e:
            logger.error(f"error loading {a_filename}\n")
            raise

        window.set_footer(f"loading {b_filename} ...")
        try:
            b_file = make_file(b_type, b_filename)
        except Exception as e:
            logger.error(f"error loading {b_filename}\n")
            raise

        window.set_footer(f"comparing...")
        tables = {}
        for table in table_names:
            a_var_names = a_file.get_keys(table)

            b_var_names = b_file.get_keys(table)

            tables[table] = truncate_set(a_var_names, b_var_names, a_only, b_only, similarity_threshold, max_entries, ignore_suffix)

        refresh(a_file, b_file, tables)
        return a_file, b_file, tables

    a_file, b_file, tables = refresh_files(left, right)

    def handleCursorMove(source, oc, c):
        refresh_bottom_panes(a_file, b_file, tables)
        
    top_pane.addCursorMoveHandler(handleCursorMove)

    while True:
        
        ch = window.getch()
        if ch == curses.KEY_RESIZE:
            handle_window_resize(window)
        elif ch == ord("."):
            current_table += 1
            current_table %= ntables
            refresh(a_file, b_file, tables)
        elif ch == ord(","):
            current_table += ntables - 1
            current_table %= ntables
            refresh(a_file, b_file, tables)
        elif ch == ord("f"):
            name = table_names[current_table]
            row = get_current_row(tables)
            if row is not None:
                key_b = row[1]
                a = a_file.get_keys(name)            
                candidates_a = find_candidates(a, key_b, similarity_threshold, max_entries, ignore_suffix)
                c = choose_candidate(window, candidates_a)
                if c is not None:
                    candidate_a, ratio = c
                    row[0] = candidate_a
                    row[2] = ratio
                    row[3] = "o"
                refresh_content(a_file, b_file, tables)
        elif ch == ord("g"):
            name = table_names[current_table]
            row = get_current_row(tables)
            if row is not None:
                key_a = row[0]
                b = b_file.get_keys(name)            
                candidates_b = find_candidates(b, key_a, similarity_threshold, max_entries, ignore_suffix)
                c = choose_candidate(window, candidates_b)
                if c is not None:
                    candidate_b, ratio = c
                    row[1] = candidate_b
                    row[2] = ratio
                    row[4] = "o"
                refresh_content(a_file, b_file, tables)
        elif ch == ord("h"):
            help(window)
            refresh_content(a_file, b_file, tables)
        elif ch == ord("a"):
            if b_update is not None:
                name = table_names[current_table]
                row = get_current_row(tables)
                if row is not None:
                    row[5] = UseA()
                    refresh_content(a_file, b_file, tables)
        elif ch == ord("b"):
            if a_update is not None:
                name = table_names[current_table]
                row = get_current_row(tables)
                if row is not None:
                    row[5] = UseB()
                    refresh_content(a_file, b_file, tables)
        elif ch == ord("c"):
            if a_update is not None or b_update is not None:
                name = table_names[current_table]
                row = get_current_row(tables)
                if row is not None:
                    key_a, key_b, _, _, _, _ = row

                    c = enter_var_name(window, key_a, key_b)
                    if c is not None:
                        row[5] = Customize(c)
                    refresh_content(a_file, b_file, tables)
        elif ch == ord("d"):
            if a_update is not None or b_update is not None:
                name = table_names[current_table]
                row = get_current_row(tables)
                if row is not None:
                    key_a, key_b, _, _, _, _ = row

                    c = enter_var_name(window, key_a, key_b)
                    if c is not None:
                        row[5] = CustomizeB(c)
                    refresh_content(a_file, b_file, tables)
        elif ch == ord("e"):
            if a_update is not None or b_update is not None:
                name = table_names[current_table]
                row = get_current_row(tables)
                if row is not None:
                    key_a, key_b, _, _, _, _ = row

                    c = enter_var_name(window, key_a, key_b)
                    if c is not None:
                        row[5] = CustomizeA(c)
                    refresh_content(a_file, b_file, tables)
        elif ch == ord("s"):
            if a_update is not None:
                name = table_names[current_table]
                row = get_current_row(tables)
                if row is not None:
                    row[5] = Noop()
                    refresh_content(a_file, b_file, tables)
        elif ch == ord("u"):
            for name, (table, _) in tables.items():
                window.set_footer(f"updating table {name} ...")
                for row in table:
                    key_a, key_b, _, _, _, action = row
                    action.update(name, key_a, a_file, key_b, b_file)
                    row[5] = Noop()
                if a_update is not None:
                    window.set_footer(f"writing to file {a_update} ...")
                    a_file.dump(a_update)
                if b_update is not None:
                    window.set_footer(f"writing to file {b_update} ...")
                    b_file.dump(b_update)
            file_a, file_b, tables = refresh_files(left if a_update is None else a_update, right if b_update is None else b_update)
        elif ch == ord("q"):
            break
        else:
            window._onKey(ch)
        window.update()


def handle_window_resize(window):
    return
    header_pane = window.children["header_pane"]
    top_pane = window.children["top_pane"]
    left_pane = window.children["left_pane"]
    right_pane = window.children["right_pane"]
    horizontal_splitter = window.children["horizontal_splitter"]
    vertical_splitter = window.children["vertical_splitter"]

    height, width = window.size()
    splitterx = width // 2
    splittery = height // 2
    top_height = max(0, splittery - 1)
    bottom_height = max(0, height - splittery - 2)
    left_width = splitterx
    right_width = width - splitterx - 1
    header_pane.resize_move(3, width, 1, 0)
    top_pane.resize_move(top_height - 3, width, 4, 0)
    left_pane.resize_move(bottom_height, left_width, splittery + 1, 0)
    right_pane.resize_move(bottom_height, right_width, splittery + 1, splitterx + 1)
    horizontal_splitter.resize_move(1, width, splittery, 0)
    vertical_splitter.resize_move(bottom_height, 1, splittery + 1, splitterx)
    window.update()

    
def create_window(stdscr):
    height, width = stdscr.getmaxyx()
    window = Window(stdscr)
    splitterx = width // 2
    splittery = height // 2
    top_height = max(0, splittery - 1)
    bottom_height = max(0, height - splittery - 2)
    left_width = splitterx
    right_width = width - splitterx - 1
    header_pane = window.pane("header_pane", 3, width, 1, 0, False)
    top_pane = window.pane("top_pane", top_height - 3, width, 4, 0, True)
    left_pane = window.pane("left_pane", bottom_height, left_width, splittery + 1, 0, False)
    right_pane = window.pane("right_pane", bottom_height, right_width, splittery + 1, splitterx + 1, False)
    horizontal_splitter = window.fill("horizontal_splitter", 1, width, splittery, 0, "-")
    vertical_splitter = window.fill("vertical_splitter", bottom_height, 1, splittery + 1, splitterx, "|")

    return window


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
    window = create_window(stdscr)

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
    parser.add_argument("--similarity_threshold", metavar="SIMILARITY_THRESHOLD", type=float, default=0,
                        help="the threshold for similarity suggestions")
    parser.add_argument('--table', metavar='TABLE', type=str, required=True, nargs="+",
                        help='tables')
    parser.add_argument("--update_a", metavar="UPDATE_A", type=str,
                        help="YAML file for the updated a. If this file is not specified then a cannot be updated")
    parser.add_argument("--update_b", metavar="UPDATE_B", type=str,
                        help="YAML file for the updated b. If this file is not specified then b cannot be updated")

    args = parser.parse_args()

    os.environ.setdefault('ESCDELAY', '25')

    curses.wrapper(lambda stdscr: curses_main(stdscr, args))


if __name__ == "__main__":
    main()

