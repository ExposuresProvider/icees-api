from features import features
import pandas as pd
import numpy as np
from sqlalchemy import Integer, String, Enum
import sys
import argparse


def generate_data(table_name, years, n, fn):
    df_all = None
    for year in years:
        df = pd.DataFrame({table_name[0].upper() + table_name[1:] + "Id":range(1,n+1)})

        df["year"] = year

        for col, t, levels, *_ in features.features[table_name]:
            if levels is None:
                if t == Integer:
                    df[col] = np.random.randint(10, size=n)
                elif t == String:
                    df[col] = [''.join(chr(x + 97) for x in np.random.randint(26, size=2)) for _ in range(n)]
                else:
                    print ("error: " + col)
            else:
                df[col] = np.random.choice(levels, size=n)
        if df_all is None:
            df_all = df
        else:
            df_all = df_all.append(df, ignore_index=True)

    df_all.to_csv(fn, index=False)


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
