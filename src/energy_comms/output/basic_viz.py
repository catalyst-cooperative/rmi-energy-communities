"""Functions to create basic visualizations of criteria."""

import matplotlib.pyplot as plt
import pandas as pd

import pudl


def make_coal_comm_map(
    df: pd.DataFrame, census_geometry: str = "tract", path: str | None = None
) -> None:
    """Map the Census geometries of coal communities.

    Arguments:
        df (pd.Dataframe): Either the closed coal generators or MSHA
            closed mines dataframe outputted from the transform step
        census_geometry (str): The Census geometry layer level that ``df``
            is joined with.
        path (str): Path to save the plot to
    """
    # temporary: drop AK and HI
    df = df[(df.state != "AK") & (df.state != "HI")]
    # get FIPS codes for closed tracts and adjacent tracts
    main_geos = df.drop_duplicates(subset=[f"{census_geometry}_id_fips"])
    adj_geos = df.explode("adjacent_id_fips").drop_duplicates(
        subset=["adjacent_id_fips"]
    )
    # get the Census geometries and join them on by FIPS
    census_geos_df = pudl.output.censusdp1tract.get_layer(
        layer=census_geometry
    ).set_index("geoid10")[["geometry"]]
    main_geos = main_geos.join(
        census_geos_df,
        on=f"{census_geometry}_id_fips",
        rsuffix="_census",
    ).set_geometry("geometry_census")
    adj_geos = adj_geos.join(
        census_geos_df,
        on="adjacent_id_fips",
        rsuffix="_census",
    ).set_geometry("geometry_census")
    # plot Census tracts over state geometries
    state_geos_df = pudl.output.censusdp1tract.get_layer(layer="state")
    # temporary: drop PR, HI, AK
    base = state_geos_df.drop([34, 7, 17]).plot(color="lightgrey", edgecolor="dimgrey")
    adj_and_base = adj_geos.plot(ax=base, color="coral")
    main_geos.plot(ax=adj_and_base, color="darkred")
    if path:
        plt.savefig(path)
