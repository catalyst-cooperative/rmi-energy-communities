import pandas as pd

# from link
# https://www.epa.gov/re-powering/how-identify-sites#looking
SOURCE_URL = "https://www.epa.gov/system/files/documents/2022-04/re-powering-screening-dataset-2022.xlsx"


def extract():
    # download area titles and convert to pandas dataframe

    d = pd.read_excel(SOURCE_URL, sheet_name="RE-Powering Sites ")

    cols = ["Site Name", "Zip Code", "Latitude", "Longitude", "State"]

    d = d[cols]

    words = ["Site Name"]

    for word in words:
        d[word] = d[word].str.title()

    return d
