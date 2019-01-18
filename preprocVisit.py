import pandas as pd
import sys
from preprocUtils import quantile, indices, intervalToLabel

input_file = sys.argv[1]
output_file = sys.argv[2]
year = sys.argv[3]
binstr = sys.argv[4]

df = pd.read_csv(input_file)

quantile(df, "Avg24hPM2.5Exposure", 5, binstr)
quantile(df, "Max24hPM2.5Exposure", 5, binstr)
quantile(df, "Avg24hOzoneExposure", 5, binstr)
quantile(df, "Max24hOzoneExposure", 5, binstr)
df["EstResidentialDensity"] = pd.cut(df["EstResidentialDensity"], indices).apply(intervalToLabel)
quantile(df, "EstResidentialDensity25Plus", 5, binstr)
quantile(df, "EstProbabilityNonHispWhite", 4, binstr)
quantile(df, "EstProbabilityHouseholdNonHispWhite", 4, binstr)
quantile(df, "EstProbabilityHighSchoolMaxEducation", 4, binstr)
quantile(df, "EstProbabilityNoAuto", 4, binstr)
quantile(df, "EstProbabilityNoHealthIns", 4, binstr)
quantile(df, "EstProbabilityESL", 4, binstr)
quantile(df, "EstHouseholdIncome", 5, binstr)
df["MajorRoadwayHighwayExposure"] = pd.cut(df["MajorRoadwayHighwayExposure"], [-1, 0, 50, 100, 200, 300, 500], labels=list(map(str, [6, 1, 2, 3, 4, 5])))
df["MepolizumabVisit"] = 0

df["year"] = year

df.to_csv(output_file, index=False)
