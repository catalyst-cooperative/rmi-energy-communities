# imports

import numpy as np
import pandas as pd

from energy_comms.extract.qcew_area_titles import extract as area_extract

# import qcew_python_3x_example as qcew
# from qcew_python_3x_example import qcewGetIndustryData as industry


# years for this extraction step

INDUSTRY_YEARS = ["2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021"]

# qualifying fossil employment naics codes

FOSSIL_NAICS_CODES = ["2121", "211", "213", "23712", "486", "4247", "22112"]

# employment code for total employment figures

TOTAL_EMPLOYMENT_NAICS_CODE = ["10"]

# group cols list based on fossil vs. total

FOSSIL_GROUP_COLS = [
    "area_fips",
    "area_title",
    "geographic_level",
    "industry_code",
    "year",
]

# total employment needs to be grouped by ownership code since they have a total already

TOTAL_GROUP_COLS = [
    "area_fips",
    "area_title",
    "geographic_level",
    "own_code",
    "industry_code",
    "year",
]

# calling
area = area_extract()


# first chunk of data - from industry extraction provided by BLS sample code

# make extraction function to set up for loop


def industry_extraction(year, naics_code):
    output = industry(year, "a", naics_code)

    return output


# extraction of all NAICS codes data for all geographies


def get_2014_industry_data(key):

    codes_dict = {"total": TOTAL_EMPLOYMENT_NAICS_CODE, "fossil": FOSSIL_NAICS_CODES}

    appended_df = []

    for year in INDUSTRY_YEARS:
        for naics_code in codes_dict[key]:
            i = industry_extraction(year, naics_code)
            df = pd.DataFrame(i[1:], columns=i[0])
            appended_df.append(df)

    appended_df = pd.concat(appended_df, axis=0)

    return appended_df


# function to clean (remove quotation marks and make wide to long)


def get_cleaned_2014_industry_data(df):

    # remove quotation marks in rows
    for i, col in enumerate(df.columns):
        df.iloc[:, i] = df.iloc[:, i].str.replace('"', "")

    # remove quotation marks in columns
    df.columns = df.columns.str.replace('"', "")

    # merge in BLS area codes to get geographic names for each observations

    df_with_areas = df.merge(area, on="area_fips", how="left")

    return df_with_areas


# using BLS area codes, make a column indicating what geographic level the observation is in (county, statewide, MSA, etc) and state column
def get_2014_industry_with_bls_geographic_tag(data):
    # potentially make df a parameter

    # make geographic level column for filtering and future aggregations

    data["geographic_level"] = np.where(
        data["area_title"].str.contains("Statewide"), "state", data["area_title"]
    )
    data["geographic_level"] = np.where(
        data["area_title"].str.contains("Parish|City|Borough|County"),
        "county",
        data["geographic_level"],
    )
    data["geographic_level"] = np.where(
        data["area_title"].str.contains("MSA"),
        "metropolitan_stat_area",
        data["geographic_level"],
    )
    data["geographic_level"] = np.where(
        data["area_title"].str.contains("MicroSA"),
        "micropolitan_stat_area",
        data["geographic_level"],
    )
    data["geographic_level"] = np.where(
        data["area_title"].str.contains("(Combined)"),
        "aggregated_stat_area",
        data["geographic_level"],
    )
    data["geographic_level"] = np.where(
        data["area_title"].str.contains("TOTAL"), "nationwide", data["geographic_level"]
    )
    data["geographic_level"] = np.where(
        data["area_title"].str.contains("Unknown"),
        "undefined",
        data["geographic_level"],
    )
    data

    # make state column for filtering based on regex/np where parsing
    data[["area", "state"]] = data["area_title"].str.split(",", 1, expand=True)
    # spl_word = ['--Statewide']
    data["state"] = np.where(
        data["state"].isnull(),
        data["area_title"].str.split("-- Statewide").str.get(0),
        data["state"],
    )
    data["state"] = np.where(
        data["state"].str.contains("MSA"),
        data["area_title"].str.split(",").str.get(1),
        data["state"],
    )

    return data


# aggregate by data set type


def aggregate_data_by_type(df, key):

    # make the dictionary with key and list
    group_col_list = {"total": TOTAL_GROUP_COLS, "fossil": FOSSIL_GROUP_COLS}

    # drop none types from row before int conversion
    df = df[df["annual_avg_emplvl"] != None]

    df = df.dropna(subset=["annual_avg_emplvl"])

    # all data types are strings right now, convert column we're gonna sum to a int
    df["annual_avg_emplvl"] = df["annual_avg_emplvl"].astype(int)

    grouped_df = (
        df.groupby(group_col_list[key]).agg({"annual_avg_emplvl": "sum"}).reset_index()
    )

    # keep total column if group col list is total

    if key == "total":
        return grouped_df.query("own_code == '0'")

    return grouped_df
