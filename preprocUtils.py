import pandas as pd
import math

def quantile(df, col, n, bin="qcut"):
    if bin == "qcut":
        df[col] = pd.qcut(df[col], n, labels=list(map(str, range(1,n+1))))
    elif bin == "cut":
        df[col] = pd.cut(df[col], n, labels=list(map(str, range(1,n+1))))
    else:
        raise "unsupported binning method"

indices = pd.IntervalIndex([pd.Interval(0, 2500, closed="left"),pd.Interval(2500, 50000, closed="left"), pd.Interval(50000, float("inf"), closed="left")])

def intStr(n):
    if math.isinf(n):
        if n > 0:
            return "inf"
        else:
            return "-inf"
    else:
        return str(int(n))

def intervalToLabel(i):
    if i.closed_left:
        s = "["
    else:
        s = "("
    
    s += intStr(i.left)
    s += ","
    s += intStr(i.right)
    if i.closed_right:
        s += "]"
    else:
        s += ")"
    
    return s

