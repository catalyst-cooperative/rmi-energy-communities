# imports

import json

import pandas as pd
import prettytable
import requests


def extract_API():
    headers = {"Content-type": "application/json"}
    data = json.dumps(
        {"seriesid": ["LNS14000000"], "startyear": "2009", "endyear": "2021"}
    )
    p = requests.post(
        "https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers
    )
    json_data = json.loads(p.text)
    for series in json_data["Results"]["series"]:
        x = prettytable.PrettyTable(
            ["series id", "year", "period", "value", "footnotes"]
        )
        seriesId = series["seriesID"]
        for item in series["data"]:
            year = item["year"]
            period = item["period"]
            value = item["value"]
            footnotes = ""
            for footnote in item["footnotes"]:
                if footnote:
                    footnotes = footnotes + footnote["text"] + ","
            if "M01" <= period <= "M12":
                x.add_row([seriesId, year, period, value, footnotes[0:-1]])
    output = open(seriesId + ".txt", "w")
    output.write(x.get_string())
    output.close()

    return x
