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
    ).rename(columns={"namelsad10": "tract_name_census", "geoid10": "tract_id_fips"})
    # get a list of adjacent Census tracts
    idx = df.index_right.dropna().astype(int).unique()
    adj_tracts_series = (
        tract_df.iloc[idx]
        .sjoin(tract_df[["geometry", "geoid10"]], how="left", predicate="touches")
        .groupby("geoid10_left")["geoid10_right"]
        .apply(list)
        .rename("adjacent_tract_id_fips")
    )
    # join the list of adjacent tracts FIPS ids onto the MSHA dataframe
    df = df.join(adj_tracts_series, on="tract_id_fips")

    return df
