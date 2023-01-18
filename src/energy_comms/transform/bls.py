"""Transform functions for Bureau of Labor Statistics data for employment criteria."""

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
    df = df.astype(
        {
            "year": "Int64",
            "period": "string",
            "local_area_unemployment_rate": "float",
            "series_id": "string",
        }
    )
    return df
