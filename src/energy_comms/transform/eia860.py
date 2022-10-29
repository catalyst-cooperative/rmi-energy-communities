"""Transform functions for the EIA 860 generators data."""
from typing import Literal

import geopandas
import pandas as pd

import energy_comms

# not including petcoke as coal generator
COAL_CODES = ["ANT", "BIT", "LIG", "SUB", "SGC", "WC", "RC"]


def transform(
    raw_df: pd.DataFrame, census_geometry: Literal["state", "county", "tract"] = "tract"
) -> pd.DataFrame:
    """Filter for coal plants meeting IRA criteria and join to Census geometry."""
    df = raw_df.copy()
    # apply filters for IRA criteria
    mask = (
        (df.operational_status == "retired")
        & (
            (df.energy_source_code_1.isin(COAL_CODES))
            | (df.energy_source_code_2.isin(COAL_CODES))
        )
        & (df.retirement_date >= "2010-01-01")
        & ~(df.longitude.isnull())
        & ~(df.latitude.isnull())
    )
    df = df[mask]

    # add geometry column to data
    df = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.longitude, df.latitude)
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
