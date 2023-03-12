"""Functions to create basic visualizations of criteria."""

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px

import pudl


# dataframe for plotly map
def create_geometries_df(
    df: pd.DataFrame, census_geometry: str = "tract"
) -> pd.DataFrame:
    """Create geometries to map qualifying geometries.

    Arguments:
        df (pd.DataFrame): Either the closed coal generators or MSHA
            closed mines dataframe outputted from the transform step, or
            the combined dataframe of generators and mines. Must have columns
            ``adjacent_id_fips`` with a list of adjacent fips codes and
            ``tract_id_fips`` or ``county_id_fips`` with the primary fips code
            for each record.
        census_geometry (str): The Census geometry layer level that ``df``
            is joined with.
    """
    census_geos_df = pudl.output.censusdp1tract.get_layer(
        layer=census_geometry
    ).set_index("geoid10")[["geometry"]]
    adj_fips = df.explode("adjacent_id_fips")[["adjacent_id_fips"]].drop_duplicates()
    adj_fips["primary_or_adj"] = "adjacent"
    adj_fips = adj_fips.rename(columns={"adjacent_id_fips": "fips_id"})

    prim_fips = df[[f"{census_geometry}_id_fips"]].drop_duplicates()
    prim_fips["primary_or_adj"] = "primary"
    prim_fips = prim_fips.rename(columns={f"{census_geometry}_id_fips": "fips_id"})

    full_fips = (
        pd.concat([prim_fips, adj_fips])
        .sort_values(by="primary_or_adj", ascending=False)
        .drop_duplicates(subset=["fips_id"])
        .set_index("fips_id")
    )

    geoms = census_geos_df.join(full_fips)
    geoms = geoms[~(geoms.primary_or_adj.isnull())]
    return geoms


def combine_gen_and_mine_geoms(
    mine_geoms: pd.DataFrame, gen_geoms: pd.DataFrame
) -> pd.DataFrame:
    """Combine generator and mine closure geometry dataframes.

    Combine the outputs of ``create_geometries_df`` for the mine and generator
    data respectively into one dataframe that's ready to be mapped with either
    ``make_plotly_map`` or ``make_matplotlib_map``.
    """
    gen_df = gen_geoms.copy()
    mine_df = mine_geoms.copy()
    gen_df.loc[:, "primary_or_adj"] = gen_df.primary_or_adj.astype("str") + "_gen"
    mine_df.loc[:, "primary_or_adj"] = mine_df.primary_or_adj.astype("str") + "_mine"
    gen_df = gen_df.reset_index()
    mine_df = mine_df.reset_index()
    full_df = (
        pd.concat([gen_df, mine_df])
        .sort_values(by="primary_or_adj", ascending=False)
        .drop_duplicates(subset=["geoid10"])
        .set_index("geoid10")
    )
    return full_df


def make_plotly_map(geoms: pd.DataFrame, output_filename: str) -> None:
    """Create plotly map of qualifying areas.

    Arguments:
        geoms (gpd.GeoDataFrame): Geodataframe with a geometry column
            and primary_or_adj column indicating whether the geometry is a
            primary geometry with a closed mine or plant or an adjacent geometry.
            Likely the output of `create_geometries_df`.
        output_filename (str): path name to output the map to as an HTML
    """
    fig = px.choropleth(
        geoms,
        geojson=geoms.geometry,
        color="primary_or_adj",
        locations=geoms.index,
        title="Qualifying Areas",
    )

    fig.update_geos(fitbounds="locations", scope="usa", showsubunits=True)
    fig.write_html(output_filename)


def make_matplotlib_map(
    geoms: pd.DataFrame,
    color_column: str = "qualifying_criteria",
    output_filename: str | None = None,
    only_conus: bool = False,
) -> None:
    """Create matplotlib map of qualifying areas.

    Arguments:
        geoms (gpd.GeoDataFrame): Geodataframe with a geometry column
            and the ``color_column`` column indicating what color the geometry
            should be.
        color_column (str): The name of the column to use to color code the
            geometries on the map.
        output_filename (str): path name to output the map to as an HTML
        only_conus (bool): if True, drop the AK, HI, PR state outline to make the
            CONUS easier to see
    """
    state_geos_df = pudl.output.censusdp1tract.get_layer(layer="state")
    if only_conus:
        state_geos_df = state_geos_df.drop([34, 7, 17])
    base = state_geos_df.plot(color="lightgrey", edgecolor="dimgrey")
    geoms.plot(ax=base, column=color_column, legend=True)
    # set limits here because AK state outlines creates a really wide window
    if only_conus:
        plt.xlim([-130, -65])
        plt.ylim([20, 60])
    else:
        plt.xlim([-180, -65])
    if output_filename:
        plt.savefig(output_filename)
    else:
        plt.show()
