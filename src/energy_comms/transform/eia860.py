"""Transform functions for the EIA 860 generators data."""

from datetime import datetime
from typing import Any, Literal

import geopandas
import pandas as pd

import energy_comms

# not including petcoke as coal generator
COAL_CODES = ["ANT", "BIT", "LIG", "RC", "SGC", "WC", "SUB"]
COAL_TECHS = ["Conventional Steam Coal", "Coal Integrated Gasification Combined Cycle"]


def transform(
    raw_df: pd.DataFrame,
    census_geometry: Literal["county", "tract"] = "tract",
    get_proposed_retirements: bool = False,
    pudl_settings: dict[Any, Any] | None = None,
) -> pd.DataFrame:
    """Filter for coal plants meeting IRA criteria and join to Census geometry.

    Arguments:
        raw_df (pd.DataFrame): The raw EIA860 dataframe extracted from PUDL
        census_geometry (string): Which Census level geometry to aggregate
            closures to.
        get_proposed_retirements (boolean): Whether to filter for just generators
            that are planned to be retired in the future. Default is False.
        pudl_settings (dict or None): A dictionary of PUDL settings, including
            paths to various resources like the Census DP1 SQLite database. If
            None, the user defaults are used.
    """
    df = raw_df.copy()
    # apply filters for IRA criteria
    coal_mask_2010_to_2013 = (
        (df.report_date >= pd.to_datetime("2010-01-01"))
        & (df.report_date < pd.to_datetime("2014-01-01"))
        & (df.energy_source_code_1.isin(COAL_CODES))
    )
    coal_mask_2014_to_present = (df.report_date >= pd.to_datetime("2014-01-01")) & (
        df.technology_description.isin(COAL_TECHS)
    )
    if not get_proposed_retirements:
        mask = (
            (df.operational_status == "retired")
            & (coal_mask_2010_to_2013 | coal_mask_2014_to_present)
            & ~(df.longitude.isnull())
            & ~(df.latitude.isnull())
        )
    else:
        mask = (
            (df.planned_generator_retirement_date > datetime.now())
            & (coal_mask_2010_to_2013 | coal_mask_2014_to_present)
            & ~(df.longitude.isnull())
            & ~(df.latitude.isnull())
        )
    df = df[mask]
    # drop duplicates: even when retired, the same generator will be
    # reported every year, keep the most recent record
    df = df.sort_values(by=["report_date"], ascending=False).drop_duplicates(
        subset=[
            "plant_id_eia",
            "utility_id_eia",
            "generator_id",
        ]
    )
    # add geometry column to data
    df = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.longitude, df.latitude)
    )
    # get intersection with specified census geometry
    df = energy_comms.helpers.get_geometry_intersection(
        df,
        census_geometry=census_geometry,
        add_adjacent_geoms=True,
        pudl_settings=pudl_settings,
    )
    df = df.assign(
        qualifying_area=f"{census_geometry}",
        qualifying_criteria="coal_plant",
    )
    return df
