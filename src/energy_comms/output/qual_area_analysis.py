"""Perform analyses on the output dataframe of qualifying areas."""
import logging

import pandas as pd

import energy_comms

logger = logging.getLogger(__name__)


def get_brownfield_acreage_agg() -> pd.Series:
    """Get the total acreage of brownfields per county.

    Returns:
        A pandas series with each record being the sum of brownfield
        acreage in a county, with county FIPS as the index.
    """
    epa_raw = energy_comms.extract.epa.extract(update=False)
    epa_df = energy_comms.transform.epa.transform(epa_raw)
    acreage = epa_df.groupby("county_id_fips")["acreage"].sum().round(2)
    return acreage


def county_agg(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate all qualifying areas to get stats for each county.

    Arguments:
        df: A dataframe of all qualifying areas, with the coal and
        brownfields criteria at the tract level. Likely the output of
        ``energy_comms.coordinate.get_all_qualifying_areas()``
    """
    # get the number of brownfields and brownfield acreage per county
    out = (
        df[~(df.county_id_fips.isnull())]
        .drop_duplicates(subset=["county_id_fips"])
        .set_index("county_id_fips")[["county_name", "state_name"]]
    )
    brownfield_nums = (
        df[df.qualifying_criteria == "brownfield"]
        .groupby("county_id_fips")
        .size()
        .rename("num_brownfields")
        .to_frame()
    )
    out = out.merge(brownfield_nums, how="left", left_index=True, right_index=True)
    brownfield_acreage = get_brownfield_acreage_agg()
    out = out.merge(brownfield_acreage, how="left", left_index=True, right_index=True)

    # get the number of coal qualifying tracts
    coal_criterias = [
        "coalmine",
        "coal_plant",
        "coal_mine_adjacent_tract",
        "coal_plant_adjacent_tract",
    ]
    coal_df = df[df.qualifying_criteria.isin(coal_criterias)].drop_duplicates(
        subset=["county_id_fips", "tract_id_fips"], keep="first"
    )
    coal_tracts = (
        coal_df.groupby("county_id_fips")
        .size()
        .rename("num_coal_qualifying_tracts")
        .to_frame()
    )
    out = out.merge(coal_tracts, left_index=True, right_index=True, how="left")

    # get the percentage of area for each county that qualifies via the coal criteria
    # first get the area of each tract
    coal_df["tract_area_meters"] = coal_df.area_geometry.to_crs("EPSG:26914").area
    # now get the area of each county
    coal_counties = coal_df[["county_id_fips"]].drop_duplicates()
    coal_counties = energy_comms.helpers.add_geometry_column(
        df=coal_counties, census_geometry="county"
    ).set_geometry("area_geometry")
    coal_counties["county_area_meters"] = coal_counties.area_geometry.to_crs(
        "EPSG:26914"
    ).area

    areas = coal_df.groupby("county_id_fips").tract_area_meters.sum().to_frame()
    areas = areas.merge(
        coal_counties.set_index("county_id_fips")[["county_area_meters"]],
        how="left",
        left_index=True,
        right_index=True,
    )
    # divide to get the percent of the county area that is qualified
    areas["percent_of_county_coal_qualified"] = (
        areas["tract_area_meters"] / areas["county_area_meters"]
    ).round(2)
    if areas.percent_of_county_coal_qualified.max() > 1:
        fips = list(areas[areas["percent_of_county_coal_qualified"] > 1].index)
        logger.warning(
            f"Uh oh! County FIPS {fips} have percentage of qualifying area greater than 1."
        )
    out = out.merge(
        areas[["percent_of_county_coal_qualified"]],
        how="left",
        left_index=True,
        right_index=True,
    )
    out = out.fillna(0).astype(
        {
            "num_brownfields": int,
            "num_coal_qualifying_tracts": int,
            "percent_of_county_coal_qualified": float,
        }
    )
    # add column for if a county qualifies by the employment criteria
    emp = df[df.qualifying_criteria == "fossil_fuel_employment"]
    emp["qualifies_by_employment_criteria"] = True
    emp = emp.set_index("county_id_fips")

    out = out.merge(
        emp[["qualifies_by_employment_criteria"]],
        left_index=True,
        right_index=True,
        how="left",
    ).fillna(False)

    return out
