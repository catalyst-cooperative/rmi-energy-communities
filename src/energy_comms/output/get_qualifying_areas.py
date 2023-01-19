"""Combine the data sources to find qualifying areas."""

import logging

import numpy as np
import pandas as pd

import energy_comms

logger = logging.getLogger(__name__)

FOSSIL_NAICS_CODES = ["2121", "211", "213", "23712", "486", "4247", "22112"]


def get_fossil_employment_qualifying_areas(
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
    total_employment_df = energy_comms.helpers.add_bls_qcew_geo_cols(
        total_employment_df
    )
    total_employment_df = total_employment_df.rename(
        columns={"annual_avg_emplvl": "total_employees"}
    )
    # get data for fossil fuel employees in an area
    fossil_employment_df = df.loc[df["industry_code"].isin(FOSSIL_NAICS_CODES)]
    fossil_employment_df = energy_comms.helpers.add_bls_qcew_geo_cols(
        fossil_employment_df
    )
    fossil_employment_df = (
        fossil_employment_df.groupby(
            ["area_fips", "area_title", "geographic_level", "year"]
        )["annual_avg_emplvl"]
        .sum()
        .reset_index()
    )
    fossil_employment_df = fossil_employment_df.rename(
        columns={"annual_avg_emplvl": "fossil_employees"}
    )
    full_df = total_employment_df.merge(
        fossil_employment_df,
        on=["area_fips", "area_title", "geographic_level", "year"],
        how="outer",
        indicator=True,
    )
    if "right_only" in full_df._merge.unique():
        logger.warning(
            "Area found in fossil employment dataframe that's not in total employment dataframe."
        )
    full_df = full_df.fillna({"fossil_employees": 0})
    # Get percentage of fossil fuel employment
    full_df["percent_fossil_employment"] = (
        full_df.fossil_employees / full_df.total_employees * 100
    )
    # area qualifies if fossil fuel employment is greater than .17%
    full_df["meets_fossil_employment_threshold"] = np.where(
        full_df["percent_fossil_employment"] > 0.17, 1, 0
    )
    full_df = full_df.merge(msa_df, on="geoid", how="inner")
    full_df["full_county_id_fips"] = np.where(
        full_df["geographic_level"] == "county",
        full_df["geoid"],
        full_df["state_id_fips"] + full_df["county_id_fips"],
    )

    return full_df


def get_unemployment_rate_qualifying_areas(
    national_unemployment_df: pd.DataFrame, lau_unemployment_df: pd.DataFrame
) -> pd.DataFrame:
    """Find qualifying areas that meet the unemployment rate criteria.

    Qualifying areas have an unemployment rate at or above the national average
    unemployment rate for the previous year.

    Args:
        national_unemployment_df: Transformed dataframe of national unemployment rates
            from the CPS data. The result of
            ``energy_comms.transform.bls.get_national_unemployment_annual_avg()``
        lau_unemployment_df: Transformed dataframe of local area unemployment rates. The
            result of ``energy_comms.transform.bls.get_local_area_unemployment_rates()``

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
    full_df["full_county_id_fips"] = np.where(
        full_df["geographic_level"] == "county",
        full_df["state_id_fips"] + full_df["geoid"],
        full_df["geoid"].str[0:5],
    )
    return full_df


def get_employment_criteria_qualifying_areas(
    fossil_employment_df: pd.DataFrame, unemployment_df: pd.DataFrame
) -> pd.DataFrame:
    """Combine employment criteria dataframes to find all qualifying areas."""
    # TODO: correct? merge on just county_id_fips even if geographic_level doesn't match?
    df = fossil_employment_df.merge(
        unemployment_df,
        on=["full_county_id_fips"],
        how="outer",
    )
    qualifying_areas = df[
        (df["meets_fossil_employment_threshold"] == 1)
        & (df["meets_unemployment_threshold"] == 1)
    ]
    qualifying_areas = qualifying_areas.drop_duplicates(subset=["full_county_id_fips"])
    qualifying_areas = qualifying_areas[
        [
            "full_county_id_fips",
            "area_title",
            "area_text",
            "state",
            "msa_name",
            "percent_fossil_employment",
            "local_area_unemployment_rate",
            "national_unemployment_rate",
        ]
    ]
    qualifying_areas["qualifying_criteria"] = "fossil_fuel_employment"
    qualifying_areas["qualifying_area"] = "MSA or non-MSA"
    return qualifying_areas
