"""Transform functiosn for EPA brownfields data."""
import logging
from typing import Any, Literal

import geopandas
import pandas as pd

import energy_comms

logger = logging.getLogger(__name__)


def transform(
    df: pd.DataFrame,
    census_geometry: Literal["county", "tract"] = "county",
    pudl_settings: dict[Any, Any] | None = None,
) -> pd.DataFrame:
    """Map brownfields to counties, add columns for map integration.

    Arguments:
        df (pd.DataFrame): Raw EPA brownfields dataframe
        census_geometry (str): Which set of Census geometries to read, must be
        "county" or "tract".
        pudl_settings (dict or None): A dictionary of PUDL settings, including
            paths to various resources like the Census DP1 SQLite database. If
            None, the user defaults are used.
    """
    logger.info("Transforming EPA brownfields data.")

    df.columns = df.columns.str.lower().str.replace(" ", "_")
    # assign dtypes
    df = df.astype(
        {"site_name": str, "state": str, "latitude": float, "longitude": float}
    )
    df[:, "program"] = df["program"].str.lower()
    df = df[df.program != "superfund"]
    df = df.rename(columns={"acreage_(acres)": "brownfield_acreage"})
    df.loc[:, "site_name"] = df["site_name"].str.strip().str.title()
    df = df.dropna(subset=["latitude", "longitude"])
    df = energy_comms.helpers.remove_invalid_lat_lon_records(df)
    # we're only keeping site_name, latitude, longitude for final map
    df = df.drop_duplicates(subset=["site_name", "latitude", "longitude"])
    df = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.longitude, df.latitude)
    )
    df = energy_comms.helpers.get_geometry_intersection(
        df, census_geometry=census_geometry, pudl_settings=pudl_settings
    )
    df = df.assign(
        qualifying_area="point",
        qualifying_criteria="brownfield",
    )

    return df
