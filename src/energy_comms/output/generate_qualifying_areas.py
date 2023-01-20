"""Combine the data sources to find qualifying areas."""

import logging
from typing import Literal

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

FOSSIL_NAICS_CODES = ["2121", "211", "213", "23712", "486", "4247", "22112"]


def fossil_employment_qualifying_areas(
    qcew_df: pd.DataFrame, msa_df: pd.DataFrame
) -> pd.DataFrame:
    """Find qualifying areas that meet the employment criteria.

    This criteria says any area that has (any time 2010 onwards) .17%
    or greater direct employment or 25% or greater local tax revenues
    related to extraction, processing, transport, or storage of coal, oil
    or natural gas.

    Args:
        qcew_df: Dataframe of the transformed QCEW data.
        msa_df: Dataframe of the MSA area code information.

    Returns:
        Dataframe with areas that qualify under this criteria.
    """
    df = qcew_df.copy()
    # get data for total employees in an area
    total_employment_df = df[(df.industry_code == "10") & (df.own_code == 0)]
    total_employment_df = total_employment_df.rename(
        columns={"annual_avg_emplvl": "total_employees"}
    )
    # get data for fossil fuel employees in an area
    fossil_employment_df = df.loc[df["industry_code"].isin(FOSSIL_NAICS_CODES)]
    fossil_employment_df = (
        fossil_employment_df.groupby(["area_fips", "year"])["annual_avg_emplvl"]
        .sum()
        .reset_index()
    )
    fossil_employment_df = fossil_employment_df.rename(
        columns={"annual_avg_emplvl": "fossil_employees"}
    )
    full_df = total_employment_df.merge(
        fossil_employment_df,
        on=["area_fips", "year"],
        how="outer",
        indicator=True,
    )
    if "right_only" in full_df._merge.unique():
        logger.warning(
            "Area found in fossil employment dataframe that's not in total employment dataframe."
        )
    full_df = full_df.fillna({"fossil_employees": 0})
    # drop rows with no employees so we don't divide by 0
    full_df = full_df[full_df.total_employees != 0]
    # Get percentage of fossil fuel employment
    full_df["percent_fossil_employment"] = (
        full_df.fossil_employees / full_df.total_employees * 100
    )
    # area qualifies if fossil fuel employment is greater than .17%
    full_df["meets_fossil_employment_threshold"] = np.where(
        full_df["percent_fossil_employment"] > 0.17, 1, 0
    )
    # merge in state, county, and MSA name information
    full_df = full_df.merge(msa_df, on="geoid", how="left")
    full_df = full_df.rename(columns={"county_id_fips": "county_fips_three_digit"})
    # construct the full county FIPS code
    full_df["county_id_fips"] = np.where(
        full_df["geographic_level"] == "county",
        full_df["geoid"],
        full_df["state_id_fips"] + full_df["county_fips_three_digit"],
    )

    return full_df


def unemployment_rate_qualifying_areas(
    national_unemployment_df: pd.DataFrame, lau_unemployment_df: pd.DataFrame
) -> pd.DataFrame:
    """Find qualifying areas that meet the unemployment rate criteria.

    Qualifying areas have an unemployment rate at or above the national average
    unemployment rate for the previous year.

    Args:
        national_unemployment_df: Transformed dataframe of national unemployment rates
            from the CPS data. The result of
            ``energy_comms.transform.bls.transform_national_unemployment_rates()``
        lau_unemployment_df: Transformed dataframe of local area unemployment rates. The
            result of ``energy_comms.transform.bls.transform_local_area_unemployment_rates()``

    Returns:
        Dataframe with areas that qualify under this criteria.
    """
    full_df = lau_unemployment_df.merge(
        national_unemployment_df,
        left_on="year",
        right_on="applies_to_criteria_year",
        how="left",
    )
    full_df["meets_unemployment_threshold"] = np.where(
        full_df["local_area_unemployment_rate"]
        >= full_df["national_unemployment_rate"],
        1,
        0,
    )

    return full_df


def employment_criteria_qualifying_areas(
    fossil_employment_df: pd.DataFrame, unemployment_df: pd.DataFrame
) -> pd.DataFrame:
    """Combine employment criteria dataframes to find all qualifying areas.

    The dataframes from each criteria are merged on the `geoid` column.
    The `geoid` is all numeric and five digits. It is the full county FIPS code for
    counties and nonmetropolitan statistical areas and is a five digit numeric MSA code
    for metropolitan statistical areas.

    Args:
        fossil_employment_df: Qualifying areas for fossil employment criteria.
            Result of ``fossil_employment_qualifying_areas``.
        unemployment_df: Qualifying areas for unemployment criteria. Result of
            ``unemployment_rate_qualifying_areas``.
    """
    fossil_employment_df = fossil_employment_df[
        fossil_employment_df["meets_fossil_employment_threshold"] == 1
    ]
    unemployment_df = unemployment_df[
        unemployment_df["meets_unemployment_threshold"] == 1
    ]
    df = fossil_employment_df.merge(
        unemployment_df,
        on=["geoid", "year"],
        how="left",
    )
    df = df.fillna({"meets_unemployment_threshold": 0})
    qualifying_areas = df[
        (df["meets_fossil_employment_threshold"] == 1)
        & (df["meets_unemployment_threshold"] == 1)
    ]
    qualifying_areas = qualifying_areas.drop_duplicates(subset=["geoid"])
    qualifying_areas["qualifying_criteria"] = "fossil_fuel_employment"
    qualifying_areas["qualifying_area"] = "MSA or non-MSA"
    return qualifying_areas


def _explode_adjacent_id_fips(
    df: pd.DataFrame,
    census_geometry: Literal["state", "county", "tract"] = "tract",
    closure_type: str = "coalmine",
) -> pd.DataFrame:
    adj_records = pd.DataFrame()
    adj_records[f"{census_geometry}_id_fips"] = df.adjacent_id_fips.explode()
    adj_records["qualifying_area"] = f"{census_geometry}"
    adj_records["criteria"] = f"{closure_type}_adjacent_tract"
    adj_records = adj_records.drop_duplicates()
    return adj_records


def coal_plant_mine_criteria_qualifying_areas(
    msha_df: pd.DataFrame,
    eia_df: pd.DataFrame,
    census_geometry: Literal["state", "county", "tract"] = "tract",
) -> pd.DataFrame:
    """Combine MSHA coal mines and EIA coal plants to find all qualifying areas.

    Explode the ``adjacent_id_fips`` column into separate records of qualifying areas.

    Args:
        msha_df: The transformed MSHA data.
        eia_df: The transformed EIA data.
        census_geometry: The Census geometry level of qualifying areas. Must
            be one of "state", "county", or "tract".
    """
    cols = [
        f"{census_geometry}_id_fips",
        f"{census_geometry}_name_census",
        "latitude",
        "longitude",
        "geometry",
        "qualifying_area",
        "qualifying_criteria",
        "adjacent_id_fips",
    ]
    msha_df = msha_df[cols]
    eia_df = eia_df[cols]

    adj_msha = _explode_adjacent_id_fips(
        msha_df, census_geometry=census_geometry, closure_type="coalmine"
    )
    adj_eia = _explode_adjacent_id_fips(
        eia_df, census_geometry=census_geometry, closure_type="coal_plant"
    )

    return pd.concat([msha_df, eia_df, adj_msha, adj_eia])
