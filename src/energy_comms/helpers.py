"""General utility functions that are used in a variety of contexts."""
import logging
from typing import Any, Literal

import geopandas
import numpy as np
import pandas as pd

import pudl

logger = logging.getLogger(__name__)


def get_geometry_intersection(
    gdf: geopandas.GeoDataFrame,
    census_geometry: Literal["state", "county", "tract"],
    pudl_settings: dict[Any, Any] | None = None,
    add_adjacent_geoms: bool = False,
    census_gdf: geopandas.GeoDataFrame | None = None,
) -> geopandas.GeoDataFrame:
    """Find intersection of records in a dataframe with Census geometries.

    Adds columns to ``gdf`` giving the Census geometry that each record
    intersects with. Performs a left join, adding the index of the right dataframe,
    a column giving the FIPS code of the geometry, and a column
    giving the name of the geometry to the left dataframe.
    Uses information within the Census DP1 database to set the
    coordinate reference system and to identify the column containing the geometry.
    No other column names or types are altered.

    Args:
        gdf (geopandas.GeoDataFrame): The dataframe with records to find the intersection of
            geometries with. Must have a Geopandas geometry column.
        census_geometry (str): Which set of Census geometries to read, must be one
            of "state", "county", or "tract".
        pudl_settings (dict or None): A dictionary of PUDL settings, including
            paths to various resources like the Census DP1 SQLite database. If
            None, the user defaults are used.
        add_adjacent_geoms (bool): Whether to add a column with a list of geometries
            at the ``census_geometry`` level that are adjacent to the record
        census_gdf (geopandas.GeoDataFrame): A dataframe of Census geometries containing columns
            for geometry, geoid10 (FIPS code), and name. If None (the default),
            this dataframe is generated from ``pudl.output.censusdp1tract.get_layer()``

    Returns:
        geopandas.GeoDataFrame: GeoDataFrame with columns added for FIPS ID at the
            ``census_geometry`` level and the name of the geometry e.g. Census Tract 58
            or Springfield County. If ``add_adjacent_geoms`` is True the column
            ``adjacent_id_fips`` is added, giving a list of geometries adjacent to the record.
    """
    logger.info("Finding intersecting Census geometries.")
    output = gdf.copy()
    col_names = {
        "state": {"geoid10": "state_id_fips", "stusps10": "state_name_census"},
        "county": {"geoid10": "county_id_fips", "namelsad10": "county_name_census"},
        "tract": {"geoid10": "tract_id_fips", "namelsad10": "tract_name_census"},
    }
    if census_gdf is None:
        census_gdf = pudl.output.censusdp1tract.get_layer(
            layer=census_geometry, pudl_settings=pudl_settings
        )
    if output.crs is None:
        output = output.set_crs(census_gdf.crs)
    elif output.crs != census_gdf.crs:
        logger.info(
            f"Converting geodataframe CRS {output.crs} to match Census geodataframe CRS {census_gdf.crs}"
        )
        output = output.to_crs(census_gdf.crs)
    output = output.sjoin(
        census_gdf[["geometry"] + list(col_names[census_geometry].keys())],
        how="left",
        predicate="intersects",
    ).rename(columns=col_names[census_geometry])
    output = output.drop(columns=["index_right"])
    if add_adjacent_geoms:
        output = get_adjacent_geometries(
            output,
            fips_column_name=f"{census_geometry}_id_fips",
            census_geometry=census_geometry,
            pudl_settings=pudl_settings,
            census_gdf=census_gdf,
        )

    return output


def get_adjacent_geometries(
    gdf: geopandas.GeoDataFrame,
    fips_column_name: str = "tract_id_fips",
    census_geometry: Literal["state", "county", "tract"] = "tract",
    pudl_settings: dict[Any, Any] | None = None,
    census_gdf: geopandas.GeoDataFrame | None = None,
) -> geopandas.GeoDataFrame:
    """Find the Census geometries adjacent to each record in ``gdf``.

    Adds a column, ``adjacent_id_fips`` with a list of all the geometries
    specified by ``census_geometry`` that are adjacent to the geometry
    of a record in ``gdf``. Should likely be used after
    ``get_geometry_intersections`` to get adjacent geometries in coal
    communities analysis.

    Args:
        gdf (geopandas.GeoDataFrame): The dataframe with records to find
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
        census_gdf (geopandas.GeoDataFrame): The dataframe of Census geometries. If None
            (the default), this dataframe is generated from
            ``pudl.output.censusdp1tract.get_layer()``

    """
    logger.info("Finding adjacent Census geometries.")
    if census_gdf is None:
        census_gdf = pudl.output.censusdp1tract.get_layer(
            layer=census_geometry, pudl_settings=pudl_settings
        )
    idx = gdf[f"{fips_column_name}"].dropna().astype(str).unique()
    # get a list of adjacent Census geometries to FIPS codes in idx
    adj_geoms_series = (
        census_gdf.set_index("geoid10")
        .loc[idx]
        .sjoin(
            census_gdf[["geometry", "geoid10"]],
            how="left",
            predicate="touches",
        )
        .rename(columns={"geoid10": "adjacent_id_fips"})
        .groupby("geoid10")["adjacent_id_fips"]
        .apply(list)
    )
    # join the list of adjacent FIPS ids onto the MSHA dataframe
    output = gdf.join(adj_geoms_series, on=fips_column_name)
    return output


def remove_invalid_lat_lon_records(
    df: pd.DataFrame, latitude_col: str = "latitude", longitude_col: str = "longitude"
) -> pd.DataFrame:
    """Filter out records that don't have valid lat or lon values."""
    lat_filter = ((df[latitude_col] >= -90) & (df[latitude_col] <= 90)) | df[
        latitude_col
    ].isnull()
    df = df[lat_filter]
    lon_filter = ((df[longitude_col] >= -180) & (df[longitude_col] <= 180)) | df[
        longitude_col
    ].isnull()
    df = df[lon_filter]
    return df


def add_bls_qcew_geo_cols(qcew_df: pd.DataFrame) -> pd.DataFrame:
    """Using BLS area codes, make a column indicating the geographic level of the record.

    Geographic levels are counties, states, MSA, etc. In this case, since QCEW data
    is filtered for county and MSA, only those geographic levels are found.

    Args:
        qcew_df: Dataframe to add geographic levels to. Probably the QCEW transformed data.
    """
    df = qcew_df.copy()
    df["geographic_level"] = np.where(
        df["area_title"].str.contains("Statewide"), "state", pd.NA
    )
    df["geographic_level"] = np.where(
        df["area_title"].str.contains("Parish|City|Borough|County"),
        "county",
        df["geographic_level"],
    )
    df["geographic_level"] = np.where(
        df["area_title"].str.contains("MSA"),
        "metropolitan_stat_area",
        df["geographic_level"],
    )
    df["geographic_level"] = np.where(
        df["area_title"].str.contains("MicroSA"),
        "micropolitan_stat_area",
        df["geographic_level"],
    )
    df["geographic_level"] = np.where(
        df["area_title"].str.contains("(Combined)"),
        "aggregated_stat_area",
        df["geographic_level"],
    )
    df["geographic_level"] = np.where(
        df["area_title"].str.contains("TOTAL"), "nationwide", df["geographic_level"]
    )
    df["geographic_level"] = np.where(
        df["area_title"].str.contains("Unknown"),
        "undefined",
        df["geographic_level"],
    )

    # add geoid column
    df["geoid"] = df["area_fips"]
    # take out C in MSA records to add extra 0
    df["geoid"] = df["geoid"].str.replace("C", "")
    # for MSAs, make geoid to match census crosswalk
    df["geoid"] = np.where(
        df["geographic_level"] == "metropolitan_stat_area",
        df["geoid"] + "0",
        df["geoid"],
    )
    return df
