"""Combine the data sources to find qualifying areas."""

import logging

import numpy as np
import pandas as pd

import energy_comms

logger = logging.getLogger(__name__)

FOSSIL_NAICS_CODES = ["2121", "211", "213", "23712", "486", "4247", "22112"]


def get_fossil_employment_qualifying_areas(qcew_df: pd.DataFrame) -> pd.DataFrame:
    """Find qualifying areas that meet the employment criteria.

    This criteria says any area that has (any time 2010 onwards) .17%
    or greater direct employment or 25% or greater local tax revenues
    related to extraction, processing, transport, or storage of coal, oil
    or natural gas.

    Args:
        qcew_data: Dataframe of the transformed QCEW data.

    Returns:
        Dataframe with areas that qualify under this criteria.
    """
    # get data for total employees in an area
    total_employment_df = qcew_df[
        (qcew_df.industry_code == "10") & (qcew_df.own_code == 0)
    ]
    total_employment_df = energy_comms.helpers.add_bls_qcew_geographic_level(
        total_employment_df
    )
    total_employment_df = total_employment_df.rename(
        columns={"annual_avg_emplvl": "total_employees"}
    )
    # get data for fossil fuel employees in an area
    fossil_employment_df = qcew_df.loc[
        qcew_df["industry_code"].isin(FOSSIL_NAICS_CODES)
    ]
    fossil_employment_df = energy_comms.helpers.add_bls_qcew_geographic_level(
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

    # TODO: geoid thing - go MSA to county

    return full_df


def get_unemployment_rate_qualifying_areas(
    national_unemployment_df: pd.DataFrame, lau_unemployment_df: pd.DataFrame
) -> pd.DataFrame:
    """Find qualifying areas that meet the unemployment rate criteria.

    Qualifying areas have an unemployment rate at or above the national average
    unemployment rate for the previous year.

    Args:
        national_unemployment_df: Transformed dataframe of national unemployment rates
            from the CPS data.
        lau_unemployment_df: Transformed dataframe of local area unemployment rates.

    Returns:
        Dataframe with areas that qualify under this criteria.
    """
    # get final unemployment data
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
