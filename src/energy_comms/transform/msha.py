"""Transform functions for the MSHA mine data."""
from typing import Literal

import geopandas
import pandas as pd

import energy_comms


def transform(
    raw_df: pd.DataFrame, census_geometry: Literal["state", "county", "tract"] = "tract"
) -> pd.DataFrame:
    """Standardize columns, filter for IRA coal mine criteria, join to census tract."""
    df = raw_df.copy()
    df.columns = df.columns.str.lower()
    df["current_status_dt"] = pd.to_datetime(df["current_status_dt"].astype("string"))
    # apply filters for IRA criteria
    mask = (
        (df.current_mine_status.isin(["Abandoned and Sealed", "Abandoned"]))
        & (df.coal_metal_ind == "C")
        & (df.current_status_dt.dt.year >= 2000)
        & ~(df.longitude.isnull())
        & ~(df.latitude.isnull())
    )
    df = df[mask]
    # add geometry column to msha data
    df = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.longitude, df.latitude, crs=4269)
    )
    # get intersection of mines with specified census geometry
    df = energy_comms.helpers.get_geometry_intersection(
        df, census_geometry=census_geometry
    )
    # find adjacent census geometries to closed mines
    df = energy_comms.helpers.get_adjacent_geometries(
        df,
        fips_column_name=f"{census_geometry}_id_fips",
        census_geometry=census_geometry,
    )

    return df
