"""Extract data from Bureau of Labor Statistics into dataframes for employment criteria."""
from __future__ import annotations

import io
import json
import logging
import zipfile
from datetime import date

import numpy as np
import pandas as pd
import requests
from requests.models import HTTPError

import energy_comms

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

LAU_URL = "https://download.bls.gov/pub/time.series/la/"

LAU_DATA_FILENAMES = [
    "la.data.0.CurrentU10-14",
    "la.data.0.CurrentU15-19",
    "la.data.0.CurrentU20-24",
]

LAU_AREA_FILENAME = "la.area"

LAU_FOOTNOTE_FILENAME = "la.footnote"

MSA_URL = "https://www.bls.gov/oes/current/msa_def.htm"

MSA_COUNTY_CROSSWALK_URL = "https://www.bls.gov/cew/classifications/areas/qcew-county-msa-csa-crosswalk-csv.csv"

EXPECTED_MSA_FILENAME = "/oes/2021/may/area_definitions_m2021.xlsx"

QCEW_URL = "https://data.bls.gov/cew/data/files/"

QCEW_YEARS = list(np.arange(2010, date.today().year))

QCEW_AREA_URL = "https://www.bls.gov/cew/classifications/areas/area-titles-csv.csv"

logger = logging.getLogger(__name__)


def extract_national_unemployment_rates() -> pd.DataFrame:
    """Extract national unemployment rate data from Current Population Survey.

    The BLS API restricts data access to 10 years at a time, so it's necessary
    to make 2 requests (2009 - 2018 and then 2018 onwards). These requests are
    concatenated into one dataframe.
    """
    # criteria specifies national unemployment rate of the previous year
    # so starting with 2009
    # API only retrieves 10 years of data at a time
    start_end_year_intervals = [["2009", "2018"], ["2019", str(date.today().year)]]
    if date.today().year > 2028:
        logger.warning("Only retrieving data up to 2028.")

    df = pd.DataFrame()
    headers = {"Content-type": "application/json"}
    for i in range(len(start_end_year_intervals)):
        data = json.dumps(
            {
                "seriesid": ["LNS14000000"],
                "startyear": start_end_year_intervals[i][0],
                "endyear": start_end_year_intervals[i][1],
            }
        )
        resp = requests.post(BLS_API_URL, data=data, headers=headers)
        json_data = json.loads(resp.text)
        status = json_data["status"]
        if status != "REQUEST_SUCCEEDED":
            raise HTTPError(
                f"Bad response from BLS API: {BLS_API_URL}. Got status {status}"
            )
        for series in json_data["Results"]["series"]:
            series_df = pd.json_normalize(series["data"])
            series_df["series_id"] = series["seriesID"]
            df = pd.concat([df, series_df])
    return df


def extract_lau_data(file_list: list[str] = [], update: bool = False) -> pd.DataFrame:
    """Download local area unemployment data or read in from inputs directory.

    Arguments:
        file_list: list of LAU file names to download from BLS website
        update: If True, download a fresh copy of the annual data for every year instead of
            using the data in the ``energy_comms.DATA_INPUTS`` directory. Default is False.
    """
    df = pd.DataFrame()
    data_dir = energy_comms.DATA_INPUTS / "lau"
    data_dir.mkdir(parents=True, exist_ok=True)
    for filename in file_list:
        file_path = data_dir / filename
        if not (file_path.exists()) or update:
            file_url = LAU_URL + filename
            resp = requests.get(file_url)
            if resp.status_code != 200:
                raise HTTPError(
                    f"Bad response from BLS URL: {file_url}. Status code: {resp.status_code}"
                )
            else:
                with open(file_path, "wb") as file:
                    file.write(resp.content)
                new_df = pd.read_table(io.StringIO(resp.text))
        else:
            new_df = pd.read_table(file_path)
        df = pd.concat([df, new_df])
    return df


def extract_lau_rates(update: bool = False) -> pd.DataFrame:
    """Extract local area unemployment rates from 2010 - 2024."""
    if date.today().year > 2024:
        logger.warning(
            "Local area unemployment rate data is only being extracted up to year 2024."
        )
    df = extract_lau_data(file_list=LAU_DATA_FILENAMES, update=update)
    return df


def extract_lau_area_table(update: bool = False) -> pd.DataFrame:
    """Extract local area unemployment table of area codes and names."""
    df = extract_lau_data(file_list=[LAU_AREA_FILENAME], update=update)
    return df


def extract_lau_footnote_table(update: bool = False) -> pd.DataFrame:
    """Extract LAU footnotes codes table."""
    df = extract_lau_data(file_list=[LAU_FOOTNOTE_FILENAME], update=update)
    return df


def extract_msa_county_crosswalk() -> pd.DataFrame:
    """Extract crosswalk from Metropolitan Statistical Areas to counties.

    MSAs are a group of counties, we can go from MSA to their contained
    counties with this crosswalk. Download CSV from
    https://www.bls.gov/cew/classifications/areas/county-msa-csa-crosswalk.htm
    """
    df = pd.read_csv(
        MSA_COUNTY_CROSSWALK_URL,
        encoding="latin",
    )
    return df


def download_qcew_data(years: list[int] = QCEW_YEARS, update: bool = False) -> None:
    """Download Quarterly Census of Employment and Wages annual averages by area.

    Checks for zip files of annual data in the ``energy_comms.DATA_INPUTS``
    directory. If a zip file isn't present or ``update`` is True, the files will
    be downloaded from the BLS website and saved to the ``energy_comms.DATA_INPUTS`` directory.
    Note that the most recent year of data might not be available yet in which case a warning
    will be produced if the file cannot be downloaded.

    Concatenates all QCEW data for counties and MSAs for each year in ``years`` and writes
    this to another CSV in ``yearly_concatenated_csvs`` directory.

    Note: This function takes a while to run depending on how fast your network
    connection is.

    Args:
        years: A list of years to download QCEW annual data for. Defaults to ``QCEW_YEARS``
            which is a list of years from 2010 to present.
        update: If True, download a fresh copy of the annual data for every year instead of
            using the data in the ``energy_comms.DATA_INPUTS`` directory. Default is False.
    """
    data_dir = energy_comms.DATA_INPUTS / "qcew"
    data_dir.mkdir(parents=True, exist_ok=True)
    # separate directory for concatenated CSV for every year
    yearly_concatenated_dir = data_dir / "yearly_concatenated_csvs"
    yearly_concatenated_dir.mkdir(parents=True, exist_ok=True)
    for year in years:
        file_path = data_dir / f"{year}_annual_by_area.zip"
        if not (file_path.exists()) or update:
            logger.info(f"Attempting download of {year} QCEW by area data.")
            file_url = f"{QCEW_URL}{year}/csv/{year}_annual_by_area.zip"
            resp = requests.get(file_url)
            if resp.status_code != 200:
                if year != max(years):
                    raise HTTPError(
                        f"Bad response from BLS URL: {file_url}. Status code: {resp.status_code}"
                    )
                else:
                    logger.warning(
                        f"Could not download {year} QCEW data. It's likely not available yet."
                    )
                    continue
            with open(file_path, "wb") as file:
                file.write(resp.content)
        # concatenate and write county and MSA data for the year to a separate CSV
        with open(yearly_concatenated_dir / f"{year}_counties_msas.csv", "w") as fout:
            logger.info(f"Writing {year} concatenated county and MSA data.")
            with zipfile.ZipFile(data_dir / f"{year}_annual_by_area.zip", "r") as z:
                skip_header = False
                for filename in z.namelist():
                    clean_name = filename.lower().strip().replace(" ", "_")
                    # only get files with county or MSA records
                    if "_county" in clean_name or "_msa" in clean_name:
                        with z.open(filename, "r") as fin:
                            text = io.TextIOWrapper(fin)
                            if skip_header:
                                next(text)
                            fout.write(text.read())
                            skip_header = True


def extract_qcew_data(
    years: list[int] = QCEW_YEARS, update: bool = False
) -> pd.DataFrame:
    """Read QCEW data in from CSVs to dataframe and concatenate all years.

    Note: probably best to just run one year (or a couple years) at a time
    because the dataframes are large.
    """
    if update:
        download_qcew_data(years=years)
    df = pd.DataFrame()
    for year in years:
        logger.info(f"Reading {year} CSV data into pandas dataframe.")
        file_path = (
            energy_comms.DATA_INPUTS
            / f"qcew/yearly_concatenated_csvs/{year}_counties_msas.csv"
        )
        if not file_path.exists():
            logger.info(f"No {year} QCEW data.")
            continue
        df = pd.concat(
            [
                df,
                pd.read_csv(file_path),
            ]
        )
    return df


def extract_qcew_areas() -> pd.DataFrame:
    """Download Quarterly Census of Employment and Wages area information.

    Return as dataframe.
    See https://www.bls.gov/cew/classifications/areas/qcew-area-titles.htm
    """
    df = pd.read_csv(QCEW_AREA_URL)
    return df
