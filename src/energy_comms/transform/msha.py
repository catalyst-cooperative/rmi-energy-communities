"""Transform functions for the MSHA mine data."""
from typing import Literal

import geopandas
import pandas as pd

import energy_comms


def strip_lower_str_cols(df: pd.DataFrame, str_cols: list[str]) -> pd.DataFrame:
    """Make string columns lower case and strip white space."""
    for col in str_cols:
        df[col] = df[col].str.strip().str.lower()
    return df


def transform(
    raw_df: pd.DataFrame, census_geometry: Literal["state", "county", "tract"] = "tract"
) -> pd.DataFrame:
    """Standardize columns, filter for IRA coal mine criteria, join to census geometry."""
    df = raw_df.copy()
    df.columns = df.columns.str.lower()
    df["current_status_dt"] = pd.to_datetime(df["current_status_dt"].astype("string"))
    df = strip_lower_str_cols(df, ["current_mine_status", "coal_metal_ind"])
    df["current_mine_name"] = (
        df["current_mine_name"].astype("string").str.strip().str.title()
    )
    df["fips_cnty_cd"] = df["fips_cnty_cd"].astype("string").str.rjust(3, "0")
    df = df.dropna(subset=["latitude", "longitude"])
    # apply filters for IRA criteria
    mask = (
        (
            df.current_mine_status.isin(
                ["abandoned and sealed", "abandoned", "nonproducing"]
            )
        )
        & (df.coal_metal_ind == "c")
        & (df.current_status_dt.dt.year >= 2000)
    )
    df = df[mask]
    # impute census tracts of missing lat, lon points?
    # add geometry column to msha data
    df = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.longitude, df.latitude)
    )
    # get intersection of mines with specified census geometry
    df = energy_comms.helpers.get_geometry_intersection(
        df, census_geometry=census_geometry, add_adjacent_geoms=True
    )

    return df
