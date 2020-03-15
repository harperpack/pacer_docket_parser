import json
import pandas as pd
import unicodedata

nos = {}
roles = {}

# nos_df = pd.read_csv("nos.csv")
# for row in nos_df.iterrows():
#     for element in row:
#         print(element)
#     break
#     if row[1] not in nos:
#         nos[row[1]] = {"nos":row[2],"category":row[0]}
# data = []
# with open("nos.csv", "r", encoding='utf-8') as f:
#     for line in f:
#         line = line.replace("\xa0"," ")
#         line = line.replace("\n","").strip()
#
#         test = str(line).split(",")
#
#         nos[str(test[1])] = {"code":str(test[1]),"label":str(test[2]),"category":str(test[0]).title()}
#
# print(nos)
# print("wuzz")
# with open("nos.json", "w", encoding="utf-8") as f:
#     json.dump(nos, f, ensure_ascii=False, indent=4)
#
# with open("roles.csv", "r", encoding="utf-8") as f:
#     for line in f:
#         line = line.replace("\xa0"," ")
#         line = line.replace("\n","").strip()
#         test = str(line).split(",")
#         if len(test) > 1 and test[1]:
#             roles[str(test[0])] = {"label":str(test[0]),"alt_label":str(test[1])}
#         else:
#             roles[str(test[0])] = {"label":str(test[0])}
#
# print(roles)
# with open("roles.json","w",encoding="utf-8") as f:
#     json.dump(roles, f, ensure_ascii=False, indent=4)

data = {
    "PRO SE": {
        "label": "Pro Se"
    },
    "ATTORNEY TO BE NOTICED": {
        "label": "Attorney to be Noticed"
    },
    "LEAD ATTORNEY": {
        "label": "Lead Attorney"
    },
    "Public Defender or Community Defender Appointment": {
        "label": "Public Defender or Community Defender Appointment"
    },
    "CJA Appointment": {
        "label": "CJA Appointment"
    },
    "Retained": {
        "label": "Retained"
    },
    "Assistant US Attorney": {
        "label": "Assistant US Attorney"
    },
    "Government Attorney": {
        "label": "Government Attorney"
    },
    "Pretrial Services": {
        "label": "Pretrial Services"
    },
    "PRO HAC VICE": {
        "label": "Pro Hac Vice"
    },
}
with open("./table_json/designations.json","w",encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
