"""Combine the data sources to find qualifying areas."""

import logging
from typing import Literal

import numpy as np
import pandas as pd

import energy_comms

logger = logging.getLogger(__name__)

FOSSIL_NAICS_CODES = ["2121", "211", "213", "23712", "486", "4247", "22112"]

# in New England states the MSA codes don't map perfectly from LAU to QCEW
LAU_TO_QCEW_MSA_CODE_CORRECTIONS = {
    "C7195": "C1486",
    "C7345": "C2554",
    "C7645": "C3598",
    "C7075": "C1262",
    "C7465": "C3034",
    "C7675": "C3886",
    "C7090": "C1270",
    "C7165": "C1446",
    "C7660": "C3834",
    "C7810": "C4414",
    "C7960": "C4934",
    "C7495": "C3170",
    "C7720": "C3930",
    "C7240": "C1554",
    "C7570": "C3530",
    "C7285": "C1486",
    "C7870": "C3530",
    "C7555": "C3930",
    "C7450": "C4934",
    "C7305": "C1446",
    "C7690": "C1446",
}


def fossil_employment_qualifying_areas(
    qcew_df: pd.DataFrame, msa_df: pd.DataFrame
) -> pd.DataFrame:
    """Find qualifying areas that meet the employment criteria.

    This criteria says any metropolitan statistical area or nonmetropolitan
    statistical area that has (any time 2010 onwards) .17%
    or greater direct employment or 25% or greater local tax revenues
    related to extraction, processing, transport, or storage of coal, oil
    or natural gas. The Quarterly Census of Employment and Wages data doesn't
    have data on the nonmetropolitan statistical area level, so we are just
    looking at MSAs. We are also ignoring the tax revenues provision, as there isn't
    a dataset of comprehensive tax revenues at a national level.

    Args:
        qcew_df: Dataframe of the transformed QCEW data containing
            employment data for MSAs and nonMSAs.
        msa_df: Dataframe of the MSA to county crosswalk.

    Returns:
        Dataframe with column to indicate whether a county qualifies under this criteria.
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
        fossil_employment_df.groupby(["msa_code", "year"])["annual_avg_emplvl"]
        .sum()
        .reset_index()
    )
    fossil_employment_df = fossil_employment_df.rename(
        columns={"annual_avg_emplvl": "fossil_employees"}
    )
    full_df = total_employment_df.merge(
        fossil_employment_df,
        on=["msa_code", "year"],
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
    # and create a record for each county in an MSA
    full_df = full_df.merge(msa_df, on="msa_code", how="left")
    full_df["geoid"] = full_df["county_id_fips"]

    return full_df


def unemployment_rate_qualifying_areas(
    national_unemployment_df: pd.DataFrame,
    lau_unemployment_df: pd.DataFrame,
    msa_df: pd.DataFrame,
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
        msa_df: Dataframe of the MSA to county crosswalk.

    Returns:
        Dataframe with column to indicate whether a county qualifies under this criteria.
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

    # fix MSA codes that are different in the QCEW MSA to county crosswalk
    full_df = full_df.replace({"msa_code": LAU_TO_QCEW_MSA_CODE_CORRECTIONS})
    full_df = full_df.merge(msa_df, on="msa_code", how="left")
    full_df["geoid"] = full_df["county_id_fips"]
    if len(full_df[full_df.geoid.isnull()]) != 0:
        bad_codes = (
            full_df[full_df.geoid.isnull()]
            .drop_duplicates(subset=["msa_code"])["msa_code"]
            .unique()
        )
        logger.warning(
            f"In the LAU data the MSA codes {bad_codes} don't have records "
            "in the MSA to county crosswalk. These MSAs likely have "
            "different codes in the crosswalk."
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
        unemployment_df[["geoid", "year", "meets_unemployment_threshold"]],
        on=["geoid", "year"],
        how="left",
    )
    df = df[~(df.meets_unemployment_threshold.isnull())]
    df = df.drop_duplicates(subset=["geoid"])
    df = energy_comms.helpers.add_state_info(df, "county_id_fips")
    df["qualifying_criteria"] = "fossil_fuel_employment"
    df["qualifying_area"] = "MSA"
    df = df.rename(columns={"county_title": "county_name"})
    df = df[
        [
            "county_name",
            "county_id_fips",
            "state_id_fips",
            "state_abbr",
            "state_name",
            "geoid",
            "qualifying_criteria",
            "qualifying_area",
        ]
    ]
    return df


# TODO: merge on names of the geometry
def _explode_adjacent_id_fips(
    df: pd.DataFrame,
    census_geometry: Literal["county", "tract"] = "tract",
    closure_type: str = "coalmine",
) -> pd.DataFrame:
    adj_records = pd.DataFrame()
    adj_records[f"{census_geometry}_id_fips"] = df.adjacent_id_fips.explode()
    adj_records["qualifying_area"] = f"{census_geometry}"
    adj_records["qualifying_criteria"] = f"{closure_type}_adjacent_{census_geometry}"
    adj_records = adj_records.drop_duplicates()
    return adj_records


def coal_criteria_qualifying_areas(
    msha_df: pd.DataFrame,
    eia_df: pd.DataFrame,
    census_geometry: Literal["county", "tract"] = "tract",
) -> pd.DataFrame:
    """Combine MSHA coal mines and EIA coal plants to find all qualifying areas.

    Explode the ``adjacent_id_fips`` column into separate records of qualifying areas.

    Args:
        msha_df: The transformed MSHA data.
        eia_df: The transformed EIA data.
        census_geometry: The Census geometry level of qualifying areas. Must
            be "county" or "tract".
    """
    msha_df = msha_df.rename(
        columns={
            "current_mine_name": "site_name",
        }
    )
    eia_df = eia_df.rename(
        columns={
            "plant_name_eia": "site_name",
        }
    )
    adj_msha = _explode_adjacent_id_fips(
        msha_df, census_geometry=census_geometry, closure_type="coalmine"
    )
    adj_eia = _explode_adjacent_id_fips(
        eia_df, census_geometry=census_geometry, closure_type="coal_plant"
    )
    # add on records for all areas adjacent to a qualifying area
    df = pd.concat([msha_df, eia_df, adj_msha, adj_eia])
    df["geoid"] = df[f"{census_geometry}_id_fips"]
    if census_geometry == "tract":
        df["county_id_fips"] = df["tract_id_fips"].str[:5]
    df = energy_comms.helpers.add_state_info(df, "county_id_fips")
    cols = [
        f"{census_geometry}_name",
        "county_id_fips",
        "state_id_fips",
        "state_abbr",
        "state_name",
        "geoid",
        "site_name",
        "qualifying_criteria",
        "qualifying_area",
        "latitude",
        "longitude",
        "geometry",
    ]
    if census_geometry == "tract":
        cols = ["tract_id_fips"] + cols
    msha_df = msha_df[cols]
    eia_df = eia_df[cols]
    return df
