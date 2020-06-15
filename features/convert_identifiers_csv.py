import csv
import os
import logging
import yaml

pat_dict = {}
visit_dict = {}
input_file = os.path.join(os.path.dirname(__file__), "..", "config", "identifiers.csv")
output_file = os.path.join(os.path.dirname(__file__), "..", "config", "identifiers.yaml")
with open(input_file, newline="") as f:
    csvreader = csv.reader(f, delimiter=",", quotechar="\"")
    next(csvreader)
    for row in csvreader:
        pat, visit, *row1 = list(map(lambda x : x.replace(" ", "").replace(u"\xa0",""), row))
        row2 = filter(lambda x : x != "", row1)
        ids = list(row2)
        if pat != "N/A" and pat != "":
            pat_dict[pat] = ids
        if visit != "N/A" and visit != "":
            visit_dict[visit] = ids
        
pat_dict["Sex2"] = pat_dict["Sex"]
visit_dict["Sex2"] = visit_dict["Sex"]

for exposure in ["PM2.5", "Ozone"]:
    for stat in ["Avg", "Max"]:
        for stat2 in ["", "_StudyAvg", "_StudyMax"]:
            for cut in ["", "_qcut"]:
                pat_dict[f"{stat}Daily{exposure}{stat2}{cut}"] = pat_dict[exposure]
                visit_dict[f"{stat}24h{exposure}{stat2}{cut}"] = pat_dict[exposure]
                

for exposure2, stat in zip(["PM2.5", "Ozone", "NO", "NO2", "NOx", "SO2", "Benzene", "Formaldehyde"], ["Avg", "Max"] + ["Avg"] * 6):
    for cut in ["", "_qcut"]:
        pat_dict[f"{stat}Daily{exposure2}_2{cut}"] = pat_dict[exposure2]
        visit_dict[f"{stat}24h{exposure2}_2{cut}"] = pat_dict[exposure2]
    del pat_dict[exposure2]
    del visit_dict[exposure2]

with open(output_file, "w") as of:
    yaml.dump({
        "patient": pat_dict,
        "visit": visit_dict
    }, of)
