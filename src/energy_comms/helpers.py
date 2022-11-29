"""General utility functions that are used in a variety of contexts."""
import logging
from typing import Any, Literal

import geopandas
import pandas as pd

import pudl

logger = logging.getLogger(__name__)


def get_geometry_intersection(
    df: geopandas.GeoDataFrame,
    census_geometry: Literal["state", "county", "tract"],
    pudl_settings: dict[Any, Any] | None = None,
    add_adjacent_geoms: bool = False,
) -> geopandas.GeoDataFrame:
    """Find intersection of records in a dataframe with Census geometries.

    Adds columns to ``df`` giving the Census geometry that each record
    intersects with. Performs a left join, adding the index of the right dataframe,
    a column giving the FIPS code of the geometry, and a column
    giving the name of the geometry to the left dataframe.
    Uses information within the Census DP1 database to set the
    coordinate reference system and to identify the column containing the geometry.
    No other column names or types are altered.

    Args:
        df (geopandas.GeoDataFrame): The dataframe with records to find the intersection of
            geometries with. Must have a Geopandas geometry column.
        census_geometry (str): Which set of Census geometries to read, must be one
            of "state", "county", or "tract".
        pudl_settings (dict or None): A dictionary of PUDL settings, including
            paths to various resources like the Census DP1 SQLite database. If
            None, the user defaults are used.
        add_adjacent_geoms (bool): Whether to add a column with a list of geometries
            at the ``census_geometry`` level that are adjacent to the record

    Returns:
        geopandas.GeoDataFrame
    """
    logger.info("Finding intersecting Census geometries.")
    col_names = {
        "state": {"geoid10": "state_id_fips", "stusps10": "state_name_census"},
        "county": {"geoid10": "county_id_fips", "namelsad10": "county_name_census"},
        "tract": {"geoid10": "tract_id_fips", "namelsad10": "tract_name_census"},
    }
    census_df = pudl.output.censusdp1tract.get_layer(
        layer=census_geometry, pudl_settings=pudl_settings
    )
    if df.crs is None:
        output = df.set_crs(census_df.crs)
    else:
        output = df.to_crs(census_df.crs)
    output = output.sjoin(
        census_df[["geometry"] + list(col_names[census_geometry].keys())],
        how="left",
        predicate="intersects",
    ).rename(columns=col_names[census_geometry])
    output = output.drop(columns=["index_right"])
    # output[f"{census_geometry}_id_fips"] = df["tract_id_fips"].astype("string").str.rjust(11, "0")
    if add_adjacent_geoms:
        output = get_adjacent_geometries(
            output,
            fips_column_name=f"{census_geometry}_id_fips",
            census_geometry=census_geometry,
            pudl_settings=pudl_settings,
            census_df=census_df,
        )

    return output


def get_adjacent_geometries(
    df: geopandas.GeoDataFrame,
    fips_column_name: str = "tract_id_fips",
    census_geometry: Literal["state", "county", "tract"] = "tract",
    pudl_settings: dict[Any, Any] | None = None,
    census_df: pd.DataFrame | None = None,
) -> geopandas.GeoDataFrame:
    """Find the Census geometries adjacent to each record in ``df``.

    Adds a column with a list of all the geometries specified by
    ``census_geometry`` that are adjacent to the geometry of a record
    in ``df``. Should likely be used after ``get_geometry_intersections``
    to get adjacent geometries in coal communities analysis.

    Args:
        df (geopandas.GeoDataFrame): The dataframe with records to find
            the adjacent geometries to. The records should have a geometry
            column at the same resolution as ``census_geometry``. Must have
            a column with the unique FIPS code for each record's geometry,
            column name specified by ``fips_column_name``. The FIPS column is
            converted to a string so as not to lose a leading zero.
        fips_column_name (str): The name of the column which contains the unique
            FIPS code for each record.
        census_geometry (str): Which set of Census geometries to find
            adjacency with, must be one of "state", "county", or "tract".
        pudl_settings (dict or None): A dictionary of PUDL settings, including
            paths to various resources like the Census DP1 SQLite database. If
            None, the user defaults are used.
        census_df (pd.DataFrame): The dataframe of Census geometries. If None
            (the default), this dataframe is generated from
            ``pudl.output.censusdp1tract.get_layer()``
    """
    logger.info("Finding adjacent Census geometries.")
    if census_df is None:
        census_df = pudl.output.censusdp1tract.get_layer(
            layer=census_geometry, pudl_settings=pudl_settings
        )
    idx = df[f"{fips_column_name}"].dropna().astype(str).unique()
    # get a list of adjacent Census geometries to FIPS codes in idx
    adj_geoms_series = (
        census_df.set_index("geoid10")
        .loc[idx]
        .sjoin(
            census_df[["geometry", "geoid10"]],
            how="left",
            predicate="touches",
        )
        .rename(columns={"geoid10": "adjacent_id_fips"})
        .groupby("geoid10")["adjacent_id_fips"]
        .apply(list)
    )
    # join the list of adjacent FIPS ids onto the MSHA dataframe
    output = df.join(adj_geoms_series, on=fips_column_name)
    return output
