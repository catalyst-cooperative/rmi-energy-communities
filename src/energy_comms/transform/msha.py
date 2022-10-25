"""Transform functions for the MSHA mine data."""
import geopandas
import pandas as pd

import pudl


def transform(raw_df: pd.DataFrame) -> pd.DataFrame:
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
    # get census tract geometries
    tract_df = pudl.output.censusdp1tract.get_layer(layer="tract")
    # add geometry column to msha data
    df = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.longitude, df.latitude, crs=4269)
    )
    df = df.sjoin(
        tract_df[["geometry", "geoid10", "namelsad10"]],
        how="left",
        predicate="intersects",
    )
    # TODO: get adjoining Census tracts
    # do a join or use .touches to get list of all touching tracts for each record
    # something like the below
    """
    adjacent_tracts = tract_df[["geometry", "geoid10", "namelsad10"]].sjoin(
        tract_df[["geometry", "geoid10"]], how="left", predicate="touches"
    )
    adjacent_tracts = adjacent_tracts.join(
        adjacent_tracts.groupby("geoid10_left")["geoid10_right"]
        .apply(list)
        .rename("adjacent_tracts"),
        on="geoid10",
        rsuffix="touching",
    )
    """

    return df
