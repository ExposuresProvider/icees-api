import sys
from dataclasses import dataclass
import logging
from colorama import init, Fore, Back, Style
import curses
from tx.functional.either import Left, Right

init()

logging.basicConfig(filename="qctool.log",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

logger = logging.getLogger(__name__)

HIGHLIGHT = Fore.WHITE + Back.BLUE
RESET = Style.RESET_ALL

NORMAL_COLOR = 0
SELECTED_NORMAL_COLOR = 1
HIGHLIGHT_COLOR = 2
SELECTED_HIGHLIGHT_COLOR = 3


def init_colors():
    curses.init_pair(HIGHLIGHT_COLOR, curses.COLOR_WHITE, curses.COLOR_BLUE)
    curses.init_pair(SELECTED_NORMAL_COLOR, curses.COLOR_BLACK, curses.COLOR_WHITE)
    curses.init_pair(SELECTED_HIGHLIGHT_COLOR, curses.COLOR_WHITE, curses.COLOR_BLUE)

    
def clip(a, b, c):
    if a < b:
        return b
    elif a > c:
        return c
    else:
        return a

    
def colorize_line(s):
    color_string = []

    while True:
        start = s.find(HIGHLIGHT)
        if start == -1:
            if len(s) > 0:
                color_string.append((s, NORMAL_COLOR))
            break
        else:
            end = s.find(RESET)
            color_string.append((s[:start], NORMAL_COLOR))
            color_string.append((s[start + len(HIGHLIGHT):end], HIGHLIGHT_COLOR))
            s = s[end + len(RESET):]
    return color_string


def colorize(s):
    return [colorize_line(line) for line in s.split("\n")]


def selection_color(c, selected):
    if selected:
        if c == NORMAL_COLOR:
            return SELECTED_NORMAL_COLOR
        elif c == HIGHLIGHT_COLOR:
            return SELECTED_HIGHLIGHT_COLOR
        else:
            return c
    else:
        return c


@dataclass
class WindowExit:
    data: object

@dataclass
class WindowContinue:
    pass

@dataclass
class WindowPass:
    pass
    

class Widget:
    def __init__(self, window):
        self.window = window

    def resize_move(self, h, w, window_y, window_x):
        self._resize_move(h, w, window_y, window_x)
        self.update()
        
    def _resize_move(self, h, w, window_y, window_x):
        self.resize(h, w)
        self.move_window(window_y, window_x)
        
    def move_window(self, window_y, window_x):
        self._move_window(window_y, window_x)
        self.update()
        
    def _move_window(self, window_y, window_x):
        self.window.mvwin(window_y, window_x)

    def resize(self, h, w):
        self._resize(h, w)
        self.update()

    def _resize(self, h, w):
        self.window.resize(h, w)        

    def getch(self):
        return self.window.getch()

    def size(self):
        return self.window.getmaxyx()
        
    def _update_line(self, window_y, window_x, line, window_w, selected):
        current_window_x = window_x
        for string, color in line:
            self.window.insnstr(window_y, current_window_x, string, window_w - window_x, curses.color_pair(selection_color(color, selected)))
            current_window_x += len(string)
            if current_window_x >= window_w:
                break

    def update(self, clear=True, refresh=True):
        if clear:
            self.window.clear()
        if refresh:
            self.window.refresh()

    def onKey(self, ch):
        res = self._onKey(ch)
        self.update()
        return res

    def _onKey(self, ch):
        return WindowPass()


def draw_border(win):
    h, w = win.getmaxyx()
    win.insstr(0, 0, "-" * w)
    for i in range(1, h - 1):
        win.addstr(i, 0, "|")
        win.addstr(i, w-1, "|")
    win.insstr(h-1, 0, "-" * w)


def draw_textfield(pop, w, y, x, label, c):
    pop.insstr(y, x, label)
    xoffset = len(label) + 1
    text = c[-w+xoffset + 1:]
    padding = " " * (w - xoffset - len(text))
    pop.insstr(y,x + xoffset, padding, curses.color_pair(SELECTED_NORMAL_COLOR))
    pop.insstr(y,x + xoffset, text, curses.color_pair(SELECTED_NORMAL_COLOR))
    return xoffset + len(text)

# key_handler returns True for exit
def popup(window, h, w, y, x, footer, create_window, key_handler):
    window.set_footer(footer)
    pop = curses.newwin(h, w, y, x)
    popwindow = Window(pop)
    popwindow.border = True
    create_window(popwindow)

    popwindow.update()

    while True:
        ch = pop.getch()
        res = key_handler(popwindow, ch)
        if isinstance(res, WindowExit):
            data = res.data
            break
        res = popwindow._onKey(ch)
        if isinstance(res, WindowExit):
            data = res.data
            break
        popwindow.update()
        
    del popwindow
    return data
    
    
class Pane(Widget):
    def __init__(self, window, selectable, max_lines=None):
        super().__init__(window)
        self.lines = []
        # coordinates of the cursor relative to the document        
        self.current_document_y = 0
        self.current_document_x = 0
        # coordinates of the top left corner of the window relative to the document
        self.window_document_y = 0
        self.window_document_x = 0
        self.max_lines = max_lines
        self.selectable = selectable
        # padding in the document where the cursor cannot pass
        self.top_padding = 0
        self.bottom_padding = 0
        self.left_padding = 0
        self.right_padding = 0
        # event handlers
        self.cursorMoveHandlers = []

    def print(self, s):
        self.current_document_y += len(s.split("\n"))
        self.append(s)
            
    def append(self, s):
        self._append(s)
        self.update()

    def _append(self, s):
        nlines = len(s)
        if self.max_lines is not None and len(self.lines) + nlines > self.max_lines:
            self.lines = self.lines[len(self.lines) + nlines - self.max_lines:]
        self.lines.extend(colorize(s))
        self._clip_coordinates()

    def replace(self, s):
        self._replace(s)
        self.update()

    def _replace(self, s):
        self.lines = []
        self._append(s)

    def move(self, document_dy, document_dx):
        self._move(document_dy, document_dx)
        self.update()

    def _move(self, document_dy, document_dx):
        self._move_abs(self.current_document_y + document_dy, self.current_document_x + document_dx)

    def move_abs(self, document_y, document_x):
        self._move_abs(document_y, document_x)
        self.update()

    def _move_abs(self, document_y, document_x):
        oc = (self.current_document_y, self.current_document_x)
        self.current_document_y = document_y
        self.current_document_x = document_x
        self._clip_coordinates()
        c = (self.current_document_y, self.current_document_x)
        if c != oc:
            self._onCursorMove(oc, c)

    def _clip_coordinates(self):
        document_height = len(self.lines)
        self.current_document_y = clip(self.current_document_y, self.top_padding, max(0, document_height - 1 - self.bottom_padding))
        self.current_document_x = clip(self.current_document_x, self.left_padding, max(0, len(self.lines[self.current_document_y]) - 1 - self.right_padding) if document_height > 0 else 0)
        
    def move_page(self, document_dy, document_dx):
        window_h, window_w = self.window.getmaxyx()
        self.move(document_dy * window_h, document_dx * window_w)
        
    def _scroll_window(self, window_h, window_w):
        document_h = len(self.lines)
        self.window_document_y = clip(self.window_document_y, self.current_document_y - window_h + 1, self.current_document_y)
        self.window_document_y = clip(self.window_document_y, 0, max(0, document_h - window_h))

    def _clear(self):
        self.lines = []
        self._move(0, 0)

    def clear(self):
        self._clear()
        self.update()

    def __del__(self):
        del self.window

    def update(self, clear=True, refresh=True):
        if clear:
            self.window.clear()
        window_h, window_w = self.window.getmaxyx()
        document_h = len(self.lines)

        self._scroll_window(window_h, window_w)
        min_document_y = self.window_document_y
        max_document_y = min(min_document_y + window_h, document_h)

        for document_y in range(min_document_y, max_document_y):
            line = self.lines[document_y]
            self._update_line(document_y - min_document_y, 0, line, window_w, self.selectable and document_y == self.current_document_y)

        if refresh:
            self.window.refresh()

        return self.current_document_y - self.window_document_y, self.current_document_x - self.window_document_x

    def _onKey(self, ch):
        if self.selectable:
            if ch == curses.KEY_UP:
                self.move(-1, 0)
                return WindowContinue()
            elif ch == curses.KEY_DOWN:
                self.move(+1, 0)
                return WindowContinue()
            elif ch == curses.KEY_PPAGE:
                self.move_page(-1, 0)
                return WindowContinue()
            elif ch == curses.KEY_NPAGE:
                self.move_page(+1, 0)
                return WindowContinue()
            else:
                return WindowPass()
        else:
            return WindowPass()

    def _onCursorMove(self, oc, c):
        for h in self.cursorMoveHandlers:
            h(self, oc, c)

    def addCursorMoveHandler(self, h):
        self.cursorMoveHandlers.append(h)


class Fill(Widget):
    def __init__(self, window, ch):
        super().__init__(window)
        self.ch = ch

    def update(self, clear=True, refresh=True):
        if clear:
            self.window.clear()
        window_h, window_w = self.window.getmaxyx()

        for window_x in range(window_w):
            for window_y in range(window_h):
                self.window.insch(window_y, window_x, self.ch)
        if refresh:
            self.window.refresh()

        return 0, 0


class Text(Widget):
    def __init__(self, window, text):
        super().__init__(window)
        self.text = text
        self.window_document_y = 0
        self.window_document_x = 0

    def update(self, clear=True, refresh=True):
        if clear:
            self.window.clear()
        window_h, window_w = self.window.getmaxyx()

        lines = self.text.split("\n")

        for window_y in range(min(window_h, len(lines) - self.window_document_y)):
            line = lines[self.window_document_y + window_y]
            self.window.insnstr(window_y, 0, line, window_w)
            
        if refresh:
            self.window.refresh()

        return 0, 0

    
class TextField(Widget):
    def __init__(self, window, label, initial_text):
        super().__init__(window)
        self.label = label
        self.text = initial_text
        self.changeHandlers = []

    def update(self, clear=True, refresh=True):
        if clear:
            self.window.clear()
        window_h, window_w = self.window.getmaxyx()

        text_w = min(window_w - len(self.label) - 2, len(self.text))
        text = self.text[-text_w:]

        cursor_x = draw_textfield(self.window, window_w, 0, 0, self.label, text)

        if refresh:
            self.window.refresh()

        return 0, cursor_x

    def _onKey(self, ch):
        if ch == curses.KEY_BACKSPACE or ch == ord("\b") or ch == 127:
            oc = self.text
            self.text = self.text[:-1]
            self._onChange(oc, self.text)
            return WindowContinue()
        elif 32 <= ch <= 126 or 128 <= ch <= 255:
            oc = self.text
            self.text += chr(ch)
            self._onChange(oc, self.text)
            return WindowContinue()
        return WindowPass()

    def _onChange(self, oc, c):
        for h in self.changeHandlers:
            h(self, oc, c)
        
    def addChangeHandler(self, h):
        self.changeHandlers.append(h)
        

        
class Window(Widget):
    def __init__(self, window):
        super().__init__(window)
        self.footer = []
        self.header = []
        self.children = {}
        self.focus = None
        self.border = False

    def set_footer(self, s):
        self._set_footer(s)
        self.update()

    def _set_footer(self, s):
        self.footer = colorize(s)

    def set_header(self, s):
        self._set_header(s)
        self.update()

    def _set_header(self, s):
        self.header = colorize(s)        

    def _update_header(self, window_h, window_w):
        for header_y, line in enumerate(self.header):
            if header_y >= window_h:
                break
            header_window_y = header_y
            self._update_line(header_window_y, 0, line, window_w, False)
            
    def _update_footer(self, window_h, window_w):
        footer_height = len(self.footer)
        for footer_y, line in enumerate(self.footer[max(0, footer_height - window_h):]):
            footer_window_y = window_h - (footer_height - footer_y)
            self._update_line(footer_window_y, 0, line, window_w, False)

    def _new_window(self, h, w, window_y, window_x):
        return self.window.derwin(h, w, window_y, window_x)

    def pane(self, name, h, w, window_y, window_x, selectable):
        window = self._new_window(h, w, window_y, window_x)
        pane = Pane(window, selectable)
        self.children[name] = pane
        return pane

    def fill(self, name, h, w, window_y, window_x, ch):
        window = self._new_window(h, w, window_y, window_x)
        fill = Fill(window, ch)
        self.children[name] = fill
        return fill

    def textfield(self, name, w, window_y, window_x, label, initial_text):
        window = self._new_window(1, w, window_y, window_x)
        textfield = TextField(window, label, initial_text)
        self.children[name] = textfield
        return textfield
        
    def textarea(self, name, h, w, window_y, window_x, initial_text):
        window = self._new_window(1, w, window_y, window_x)
        textarea = TextArea(window, initial_text)
        self.children[name] = textarea
        return textarea
        
    def text(self, name, h, w, window_y, window_x, text):
        window = self._new_window(h, w, window_y, window_x)
        text = Text(window, text)
        self.children[name] = text
        return text

    def popup(self, footer, create_window, key_handler, h=None, w=None, window_y=None, window_x=None):
        window_h, window_w = self.size()

        poph = window_h * 4 // 5 if h is None else h
        popw = window_w * 4 // 5 if w is None else w
        popy = (window_h - poph) // 2
        popx = (window_w - popw) // 2

        return popup(self, poph, popw, popy, popx, footer, create_window, key_handler)
        
    def update(self, clear=True, refresh=True):
        if clear:
            self.window.clear()

        super().update(False, False)
        
        window_h, window_w = self.window.getmaxyx()
        
        self._update_header(window_h, window_w)
        self._update_footer(window_h, window_w)

        cursor_window_y = 0
        cursor_window_x = 0
        
        for name, c in self.children.items():
            if name == self.focus:
                cursor_widget_y, cursor_widget_x = c.update(False, False)
                widget_window_y, widget_window_x = c.window.getparyx()
                cursor_window_y = widget_window_y + cursor_widget_y
                cursor_window_x = widget_window_x + cursor_widget_x
            else:
                c.update(False, False)

        if self.border:
            draw_border(self.window)

        if self.focus is None:
            curses.curs_set(0)
        else:
            curses.curs_set(1)
            self.window.move(cursor_window_y, cursor_window_x)

        if refresh:
            self.window.refresh()

        return cursor_window_y, cursor_window_x

    def _onKey(self, ch):

        if self.focus is not None:
            focused_child = self.children[self.focus]
            res = focused_child._onKey(ch)
            if not isinstance(res, WindowPass):
                return res
            
        for name, c in self.children.items():
            if name != self.focus:
                res = c._onKey(ch)
                if not isinstance(res, WindowPass):
                    return res

        return WindowPass()
                

