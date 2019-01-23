import pandas as pd
import sys
from preprocUtils import quantile, preprocSocial

input_file = sys.argv[1]
output_file = sys.argv[2]
year = sys.argv[3]
binstr = sys.argv[4]

df = pd.read_csv(input_file)

quantile(df, "AvgDailyPM2.5Exposure", 5, "qcut", "AvgDailyPM2.5Exposure_qcut")
quantile(df, "MaxDailyPM2.5Exposure", 5, "qcut", "MaxDailyPM2.5Exposure_qcut")
quantile(df, "AvgDailyOzoneExposure", 5, "qcut", "AvgDailyOzoneExposure_qcut")
quantile(df, "MaxDailyOzoneExposure", 5, "qcut", "MaxDailyOzoneExposure_qcut")
quantile(df, "AvgDailyPM2.5Exposure", 5, binstr)
quantile(df, "MaxDailyPM2.5Exposure", 5, binstr)
quantile(df, "AvgDailyOzoneExposure", 5, binstr)
quantile(df, "MaxDailyOzoneExposure", 5, binstr)
preprocSocial(df)
df["Mepolizumab"] = 0

df["year"] = year

df.to_csv(output_file, index=False)
