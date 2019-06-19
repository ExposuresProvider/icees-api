from features2_0_0.features import features
import pandas as pd
import numpy as np
from sqlalchemy import Integer, String, Enum
import sys

t = sys.argv[1]
n = int(sys.argv[2])
fn = sys.argv[3]

df = pd.DataFrame({t[0].upper() + t[1:] + "Id":range(1,n+1)})

for col, t, levels, _ in features[t]:
    if levels is None:
        if t == Integer:
            df[col] = np.random.randint(10, size=n)
        else:
            print ("error: " + col)
    else:
        df[col] = np.random.choice(levels, size=n)


df.to_csv(fn, index=False)

