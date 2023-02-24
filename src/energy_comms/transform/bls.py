"""Transform functions for Bureau of Labor Statistics data for employment criteria."""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def transform_national_unemployment_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the raw national unemployment rate data and get annual avg for each year.

    Strip and lower column names, enforce types, rename columns. Groupby year
    and get the annual average unemployment rate.

    Args:
        df: The raw dataframe extracted from BLS website.

    Returns:
        Dataframe with the annual average national unemployment rate.
    """
    df.columns = df.columns.str.strip().str.lower()
    df["series_id"] = df["series_id"].str.strip()
    df = (
        df.rename(
            columns={"periodname": "month", "value": "national_unemployment_rate"}
        )
        .dropna(subset=["year"])
        .astype(
            {
                "year": "Int64",
                "period": "string",
                "month": "string",
                "national_unemployment_rate": "float",
                "series_id": "string",
            }
        )
        .sort_values(by=["year", "period"])
    )
    # other values like averages could be included with other period values
    df = df[(df.period >= "M01") & (df.period <= "M12")]
    # now take the annual average
    # note the rounding bc BLS website specifies 1 sig figure
    df = df.groupby("year")["national_unemployment_rate"].mean().round(1).reset_index()
    # IRA criteria specifies national unemployment rate of the previous year
    df["applies_to_criteria_year"] = df["year"] + 1
    df = df.rename(columns={"year": "real_year"})

    return df


def transform_lau_table(df: pd.DataFrame) -> pd.DataFrame:
    """Transform local area unemployment rates data."""
    df.columns = df.columns.str.strip().str.lower()
    df["series_id"] = df["series_id"].str.strip()
    df = df.rename(columns={"value": "local_area_unemployment_rate"})
    # convert to float and make invalid values null
    df["local_area_unemployment_rate"] = pd.to_numeric(
        df["local_area_unemployment_rate"], errors="coerce"
    )
    df = df.astype(
        {
            "year": "Int64",
            "period": "string",
            "series_id": "string",
        }
    )
    # filter out M13 (annual average) values and do a groupby + average
    # later becuase the M13 values are null (footnote code U) when there
    # is a missing monthly value (footnote code N)
    df = df[(df.period >= "M01") & (df.period <= "M12")]
    return df


def transform_lau_areas(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Transform local areas dataframe.

    Construct the BLS series ID for records that refer to a county
    or metropolitan statistical area. Add state FIPS ID column and
    and geo ID column. For more information on these BLS series IDs
    see https://www.bls.gov/help/hlpforma.htm#LA

    This table gives the area name for the LAU data.
    """
    df = raw_df.copy()
    df.columns = df.columns.str.strip().str.lower()
    df = df.astype(
        {"area_type_code": "string", "area_code": "string", "area_text": "string"}
    )
    # only keep records for MSAs
    df = df[df.area_code.str[:2] == "MT"]
    df["state_id_fips"] = df["area_code"].str[2:4]
    # construct the local area unemployment series ID
    df["series_id"] = "LAU" + df["area_code"].str[:10]
    df["series_id"] = df["series_id"].str.pad(width=18, side="right", fillchar="0")
    # construct the MSA code for the MSA to county crosswalk
    df["msa_code"] = "C" + df["area_code"].str[4:8]
    # 03 is the unemployment rate code
    df["series_id"] = df["series_id"] + "03"
    return df


def transform_local_area_unemployment_rates(
    raw_lau_df: pd.DataFrame, raw_area_df: pd.DataFrame
) -> pd.DataFrame:
    """Get the annual average local unemployment rate and area information.

    Clean the LAU data, filter area data down to counties and metropolitan
    statistical areas, calculate annual average rate, merge on area information.

    Args:
        raw_lau_df: The raw local area unemployment data.
        raw_area_df: The raw local area unemployment area information.

    Returns:
        Dataframe giving the annual average unemployment rate for counties
        and metropolitan statistical areas.
    """
    lau_df = transform_lau_table(raw_lau_df)
    # filter for just MSAs
    lau_df = lau_df[lau_df.series_id.str[3:5] == "MT"]
    # filter for unemployent rate statistics (last two digits is measure_code)
    lau_df = lau_df[lau_df.series_id.str[-2:] == "03"]
    # take an annual average, didn't use M13 here because it is null
    # (footnote code U) when any monthly value is missing (footnote code N)
    # but maybe it's best to use M13 and not interpolate annual average
    lau_df = lau_df.dropna(subset=["local_area_unemployment_rate"])
    # note the rounding bc BLS website specifies 1 sig figure
    lau_df = (
        lau_df.groupby(by=["series_id", "year"])["local_area_unemployment_rate"]
        .mean()
        .round(1)
        .reset_index()
    )
    # join on area information
    area_df = transform_lau_areas(raw_area_df)
    df = lau_df.merge(area_df, on="series_id", how="left")
    # drop any records that we don't have MSA data for
    df = df[~df.msa_code.isnull()]
    return df


def transform_msa_area_defs(df: pd.DataFrame) -> pd.DataFrame:
    """Transform dataframe of MSA codes and names.

    Clean column names, enforce string types, pad FIPS codes with 0s,
    construct geoid. Geoid is five digit fips for nonmetropolitan stat
    areas and the five digit MSA code for metropolitan stat areas.
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    col_rename_dict = {
        "fips_code": "state_id_fips",
        "county_code": "county_id_fips",
        "township_code": "township_id_fips",
    }
    # msa names and codes column have names like may_2021_msa_code
    # make this name independent of date of data publishing
    col_rename_dict.update(
        {
            df.columns[df.columns.str.contains("msa_name")][0]: "msa_name",
            df.columns[df.columns.str.contains("msa_code")][0]: "msa_code",
        }
    )
    # all columns are string type
    df = df.rename(columns=col_rename_dict).astype("string")
    # construct FIPS codes
    df["state_id_fips"] = df["state_id_fips"].str.zfill(2)
    df["county_id_fips"] = df["county_id_fips"].str.zfill(3)
    df["township_id_fips"] = df["township_id_fips"].str.zfill(3)
    # construct geoid which will be used to merge with
    # records from unemployment criteria dataframe
    df["geoid"] = np.where(
        df["msa_name"].str.contains("nonmetropolitan"),
        df["state_id_fips"] + df["county_id_fips"],
        df["msa_code"],
    )
    df["geoid"] = df["geoid"].astype("string")
    return df


def transform_msa_county_crosswalk(df: pd.DataFrame) -> pd.DataFrame:
    """Transform MSA to county crosswalk so it can connected to QCEW data."""
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df = df.rename(columns={"county_code": "county_id_fips"})
    df = df.astype(str)
    df["county_id_fips"] = df.county_id_fips.str.zfill(5)
    return df


def transform_qcew_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the QCEW data."""
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df = df.astype(
        {
            "area_fips": "string",
            "area_title": "string",
            "industry_code": "string",
            "own_code": "Int64",
        }
    )
    df["area_fips"] = df["area_fips"].str.zfill(5)
    # only including MSA for now, not non-MSA
    df = df[df["area_title"].str.contains("MSA")]
    df = df.rename(columns={"area_fips": "msa_code"})
    cols = [
        "msa_code",
        "area_title",
        "year",
        "industry_code",
        "own_code",
        "annual_avg_emplvl",
    ]
    return df[cols]
