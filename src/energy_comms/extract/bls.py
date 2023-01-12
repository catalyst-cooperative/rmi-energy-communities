"""Extract data from Bureau of Labor Statistics into dataframes for employment criteria."""
from __future__ import annotations

import io
import json
import logging
from datetime import date

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.models import HTTPError

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

LAU_URL = "https://download.bls.gov/pub/time.series/la/"

LAU_DATA_FILENAMES = [
    "la.data.0.CurrentU10-14",
    "la.data.0.CurrentU15-19",
    "la.data.0.CurrentU20-24",
]

LAU_AREA_FILENAME = "la.area"

MSA_URL = "https://www.bls.gov/oes/current/msa_def.htm"
EXPECTED_MSA_FILENAME = "/oes/2021/may/area_definitions_m2021.xlsx"

logger = logging.getLogger(__name__)


def extract_national_unemployment_rates() -> pd.DataFrame:
    """Extract national unemployment rate data from Current Population Survey."""
    headers = {"Content-type": "application/json"}
    # criteria specifies national unemployment rate of the previous year
    # so starting with 2009
    data = json.dumps(
        {
            "seriesid": ["LNS14000000"],
            "startyear": "2009",
            "endyear": str(date.today().year),
        }
    )
    resp = requests.post(BLS_API_URL, data=data, headers=headers)
    json_data = json.loads(resp.text)
    status = json_data["status"]
    if status != "REQUEST_SUCCEEDED":
        raise HTTPError(
            f"Bad response from BLS API: {BLS_API_URL}. Got status {status}"
        )
    df = pd.DataFrame()
    for series in json_data["Results"]["series"]:
        series_df = pd.json_normalize(series["data"])
        series_df["series_id"] = series["seriesID"]
        df = pd.concat([df, series_df])
    df = df[(df.period >= "M01") & (df.period <= "M12")]
    return df


def extract_lau_data(file_list: list[str] = []) -> pd.DataFrame:
    """Extract local area unemployment data."""
    df = pd.DataFrame()
    for filename in file_list:
        file_url = LAU_URL + filename
        resp = requests.get(file_url)
        if resp.status_code != 200:
            raise HTTPError(
                f"Bad response from BLS URL: {file_url}. Status code: {resp.status_code}"
            )
        else:
            df = pd.concat([df, pd.read_table(io.StringIO(resp.text))])
    return df


def extract_lau_rates() -> pd.DataFrame:
    """Extract local area unemployment rates from 2010 - 2024."""
    if date.today().year > 2024:
        logger.warning(
            "Local area unemployment rate data is only being extracted up to year 2024."
        )
    df = extract_lau_data(file_list=LAU_DATA_FILENAMES)
    return df


def extract_lau_area_table() -> pd.DataFrame:
    """Extract local area unemployment table of area codes and names."""
    df = extract_lau_data(file_list=[LAU_AREA_FILENAME])
    return df


def extract_msa_codes() -> pd.DataFrame:
    """Extract code definitions of Metropolitan Statistical Areas."""
    resp = requests.get(MSA_URL)
    soup = BeautifulSoup(resp.text, "html.parser")

    excel_files = []
    for link in soup.select('a[href*=".xls"]'):
        excel_files.append(link["href"])
    if excel_files == []:
        raise AssertionError(f"No Excel files for MSA definitions found at {MSA_URL}")
    if EXPECTED_MSA_FILENAME not in excel_files:
        logger.warning(
            f"The expected May 2021 MSA definitions filename was not found. \
            Using {excel_files[0]}. The old filename was likely replaced with new definitions."
        )
        file_url = excel_files[0]
    else:
        file_url = EXPECTED_MSA_FILENAME
    df = pd.read_excel("https://www.bls.gov" + file_url)
    return df
