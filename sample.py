from features import features
import pandas as pd
import numpy as np
from sqlalchemy import Integer, String, Enum
import sys
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('table', type=str)
    parser.add_argument('years', type=int, nargs="+")
    parser.add_argument('size', type=int)
    parser.add_argument('filename', type=str)

    args = parser.parse_args()
    t = args.table
    years = args.years
    n = args.size
    fn = args.filename

    generate_data(t, years, n, fn)

def generate_data(t, years, n, fn):
    df = pd.DataFrame({t[0].upper() + t[1:] + "Id":range(1,n+1)})

    df["year"] = np.random.choice(years, size=n)

    for col, t, levels, *_ in features.features[t]:
        if levels is None:
            if t == Integer:
                df[col] = np.random.randint(10, size=n)
            elif t == String:
                df[col] = [''.join(chr(x + 97) for x in np.random.randint(26, size=2)) for _ in range(n)]
            else:
                print ("error: " + col)
        else:
            df[col] = np.random.choice(levels, size=n)

    df.to_csv(fn, index=False)

