"""Transform functions for Bureau of Labor Statistics data for employment criteria."""

import numpy as np
import pandas as pd


def transform_national_unemployment_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the raw national unemployment rate data.

    Strip and lower column names, enforce types, rename columns.

    Args:
        df: The raw dataframe extracted from BLS website.

    Returns:
        Cleaned version of the national unemployment rates dataframe
        with no columns lost.
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

    return df


def get_national_unemployment_annual_avg(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Clean the raw national unemployment data, get the annual average for each year.

    Args:
        raw_df: The raw dataframe extracted from BLS website.

    Returns:
        Dataframe with the annual average national unemployment rate (and nothing else)
    """
    df = transform_national_unemployment_rates(raw_df)
    # note the rounding bc BLS website specifies 1 sig figure
    df = (
        df.groupby("year")["national_unemployment_rate"]
        .mean()
        .round(1)
        .reset_index()
        .rename(
            columns={"national_unemployment_rate": "avg_national_unemployment_rate"}
        )
    )
    # IRA criteria specifies national unemployment rate of the previous year
    df["applies_to_criteria_year"] = df["year"] + 1
    return df


def transform_lau_rates(df: pd.DataFrame) -> pd.DataFrame:
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
    and geo ID column.
    """
    df = raw_df.copy()
    df.columns = df.columns.str.strip().str.lower()
    # only keep records for county and MSA
    df = df[(df.area_code.str[:2] == "CN") | (df.area_code.str[:2] == "MT")]
    df["geographic_level"] = np.where(
        df.area_code.str[:2] == "CN", "county", "metropolitan_stat_area"
    )
    df["state_id_fips"] = df["area_code"].str[2:4]
    df["geoid"] = np.where(
        df["geographic_level"] == "county",
        df["area_code"].str[4:7],
        df["area_code"].str[4:10],
    )
    # construct the local area unemployment series ID
    df["series_id"] = np.where(
        df["geographic_level"] == "county",
        "LAU" + df["area_code"].str[:7],
        "LAU" + df["area_code"].str[:10],
    )
    df["series_id"] = df["series_id"].str.pad(width=18, side="right", fillchar="0")
    df["series_id"] = df["series_id"] + "03"
    return df


def get_local_area_unemployment_rates(
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
    lau_df = transform_lau_rates(raw_lau_df)
    # take an annual average, didn't use M13 here because it is null
    # (footnote code U) when any monthly value is missing (footnote code N)
    # but maybe it's best ot use M13 and not interpolate annual average
    lau_df = lau_df.dropna(subset=["local_area_unemployment_rate"])
    # note the rounding bc BLS website specifies 1 sig figure
    lau_df = (
        lau_df.groupby(by=["series_id", "year"])["local_area_unemployment_rate"]
        .mean()
        .round(1)
        .reset_index()
    )
    lau_df = lau_df.rename(
        columns={"local_area_unemployment_rate": "avg_local_area_unemployment_rate"}
    )
    # join on area information
    area_df = transform_lau_areas(raw_area_df)
    df = lau_df.merge(area_df, on="series_id", how="left")
    df = df[~df.geographic_level.isnull()]
    return df