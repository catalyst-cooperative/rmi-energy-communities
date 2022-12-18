"""Transform functiosn for EPA brownfields data."""
import logging
from typing import Any, Literal

import geopandas
import pandas as pd

import energy_comms

logger = logging.getLogger(__name__)


def transform(
    raw_df: pd.DataFrame,
    census_geometry: Literal["state", "county", "tract"] = "county",
    pudl_settings: dict[Any, Any] | None = None,
) -> pd.DataFrame:
    """Map zip codes to counties, prepare dataframe for map integration."""
    logger.info("Transforming EPA brownfields data.")
    # from HUD: https://www.huduser.gov/portal/datasets/usps_crosswalk.html

    df = raw_df.copy()
    # make lower case and replace spaces
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    # assign dtypes
    df = df.astype(
        {"site_name": str, "state": str, "latitude": float, "longitude": float}
    )
    # needed for eventual merge with other criteria
    df["area_title"] = df["site_name"].str.strip().str.title()
    df = df.dropna(subset=["latitude", "longitude"])
    df = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.longitude, df.latitude)
    )
    df = energy_comms.helpers.get_geometry_intersection(
        df, census_geometry=census_geometry, pudl_settings=pudl_settings
    )

    """
    # read in zip code cross walk from HUD

    zip_code_crosswalk = pd.read_excel(
        "https://www.huduser.gov/portal/datasets/usps/ZIP_COUNTY_122021.xlsx",
        dtype={"zip": str, "county": str},
    )

    zip_crosswalk = dict(zip(zip_code_crosswalk["zip"], zip_code_crosswalk["county"]))

    # fill in criteria for patio and map crosswalk
    """

    df = df.assign(
        qualifying_area="site",
        criteria="brownfield",
    )

    return df
