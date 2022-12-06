# import dispatch
import glob
import io
import os
import zipfile
from io import BytesIO

# import qcew_2014_2021
# from qcew_2014_2021 import *
from pathlib import Path

import pandas as pd
import requests

from energy_comms.constants import YEARS

"""Function that downloads zip file through an online URL"""

# years = ['2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021']
CACHE_DIR = Path.home() / "Documents/qcew_cache"


def download_all(yrs):
    if not CACHE_DIR.exists():
        CACHE_DIR.mkdir(parents=True)

    for yr in yrs:
        file_save_path = CACHE_DIR / f"{yr}_annual_by_area.zip"

        if not file_save_path.exists():

            url = (
                f"https://data.bls.gov/cew/data/files/{yr}/csv/{yr}_annual_by_area.zip"
            )
            with file_save_path.open("wb") as f:
                f.write(requests.get(url, allow_redirects=True, timeout=10).content)


def make_year_df_2(yr):
    # things_we_want = ("County", "MSA")
    with zipfile.ZipFile(CACHE_DIR / f"{yr}_annual_by_area.zip") as z:
        dfs = [
            pd.read_csv(BytesIO(z.read(x)))
            for x in z.namelist()
            if "County" in x or "MSA" in x
        ]
    return pd.concat(dfs, ignore_index=True)


def extract():
    my_dict = {}
    for yr in YEARS:
        my_dict["df_" + yr] = make_year_df_2(yr)
    df = pd.concat(my_dict.values())

    return df
