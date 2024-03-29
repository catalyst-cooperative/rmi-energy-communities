"""Combine the data sources to find qualifying areas."""

import logging
from typing import Any, Literal

import numpy as np
import pandas as pd

import energy_comms

logger = logging.getLogger(__name__)


def _get_percentage_fossil_employees(df: pd.DataFrame) -> pd.DataFrame:
    total_employment_df = df[(df.industry_code == "10") & (df.own_code == 0)]
    total_employment_df = (
        total_employment_df.groupby(["msa_code", "year"])["annual_avg_emplvl"]
        .sum()
        .reset_index()
    )
    total_employment_df = total_employment_df.rename(
        columns={"annual_avg_emplvl": "total_employees"}
    )
    # get data for fossil fuel employees in an area
    # for the sake of the NAICS analysis, filter out records with 0 employment
    fossil_employment_df = df.loc[
        df["industry_code"].isin(energy_comms.transform.bls.FOSSIL_NAICS_CODES)
    ]
    # rename column, useful for looking at what NAICS code record is from
    fossil_employment_df = fossil_employment_df.rename(
        columns={"industry_code": "naics_code"}
    )
    fossil_employment_df = (
        fossil_employment_df.groupby(["msa_code", "year"])
        .agg({"annual_avg_emplvl": "sum", "naics_code": lambda x: list(set(x))})
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
    # Get percentage of fossil fuel employment
    full_df["percent_fossil_employment"] = (
        full_df.fossil_employees / full_df.total_employees
    )
    # area qualifies if fossil fuel employment is greater than .17%
    full_df["meets_fossil_employment_threshold"] = np.where(
        full_df["percent_fossil_employment"] > 0.0017, 1, 0
    )
    return full_df


def fossil_employment_qualifying_areas(
    qcew_df: pd.DataFrame,
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
            a record for the counties within MSAs and non-MSAs giving the
            employment statistics for each NAICS code and ownership code.

    Returns:
        Dataframe with column to indicate whether a county qualifies under this criteria.
    """
    qual_df = _get_percentage_fossil_employees(qcew_df)
    df = qcew_df.drop_duplicates(subset=["msa_code", "county_id_fips", "year"]).merge(
        qual_df, how="left", on=["msa_code", "year"]
    )

    df["geoid"] = df["county_id_fips"]

    return df


def unemployment_rate_qualifying_areas(
    national_unemployment_df: pd.DataFrame,
    lau_df: pd.DataFrame,
) -> pd.DataFrame:
    """Find qualifying areas that meet the unemployment rate criteria.

    Qualifying areas have an unemployment rate at or above the national average
    unemployment rate for the previous year.

    Args:
        national_unemployment_df: Transformed dataframe of national unemployment rates
            from the CPS data. The result of
            ``energy_comms.transform.bls.transform_national_unemployment_rates()``
        lau_df: Transformed dataframe of local area unemployment rates where each
            record represents a county within an MSA or non-MSA. The dataframe returned
            from ``energy_comms.transform.bls.transform_local_area_unemployment_rates()``

    Returns:
        Dataframe with column to indicate whether a county qualifies under this criteria.
    """
    # merge on the national unemployment rates
    full_df = lau_df.merge(
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
    fossil_employment_df: pd.DataFrame,
    unemployment_df: pd.DataFrame,
    pudl_settings: dict[Any, Any] | None = None,
    census_state_df: pd.DataFrame = None,
    census_county_df: pd.DataFrame = None,
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
        pudl_settings: Default is None. Used for adding geometry column onto dataframe.
            A dictionary of PUDL settings, including paths to various resources like the
            Census DP1 SQLite database. If None, the user defaults are used.
        census_state_df: Dataframe of state data. If None (the default), this dataframe
            is generated from ``pudl.output.censusdp1tract.get_layer(layer="state")``.
            This parameter is mostly used for testing purposes. Must have columns
            ``state_id_fips``, ``state_name``, ``state_abbr``
        census_county_df: Dataframe of county info. If None (the default), this dataframe
            is generated from ``pudl.output.censusdp1tract.get_layer(layer="county")``.
            This parameter is mostly used for testing purposes. Must have columns
            ``county_id_fips``, ``county_name``
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
    df = energy_comms.helpers.add_area_info(
        df,
        fips_col="county_id_fips",
        add_state=True,
        add_county=True,
        state_df=census_state_df,
        county_df=census_county_df,
    ).pipe(
        energy_comms.helpers.add_geometry_column,
        census_geometry="county",
        pudl_settings=pudl_settings,
        census_gdf=census_county_df,
    )
    df["qualifying_criteria"] = "fossil_fuel_employment"
    df["qualifying_area"] = "MSA or non-MSA"
    df = df.rename(columns={"area_title": "site_name"})
    df = df[
        [
            "county_name",
            "county_id_fips",
            # "naics_code",
            "state_id_fips",
            "state_abbr",
            "state_name",
            "geoid",
            "site_name",
            "qualifying_criteria",
            "qualifying_area",
            "area_geometry",
        ]
    ]
    return df


def _explode_adjacent_id_fips(
    df: pd.DataFrame,
    census_geometry: Literal["county", "tract"] = "tract",
    closure_type: str = "coalmine",
) -> pd.DataFrame:
    adj_records = pd.DataFrame()
    adj_records[f"{census_geometry}_id_fips"] = df.adjacent_id_fips.explode()
    adj_records = energy_comms.helpers.add_geometry_column(
        adj_records,
        census_geometry=census_geometry,
        add_name=True,
    )
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
    # create records for all areas adjacent to a qualifying area
    adj_msha = _explode_adjacent_id_fips(
        msha_df, census_geometry=census_geometry, closure_type="coal_mine"
    )
    adj_eia = _explode_adjacent_id_fips(
        eia_df, census_geometry=census_geometry, closure_type="coal_plant"
    )
    df = pd.concat([msha_df, eia_df, adj_msha, adj_eia]).to_crs("EPSG:4269")
    df["geoid"] = df[f"{census_geometry}_id_fips"]
    if census_geometry == "tract":
        df = energy_comms.helpers.add_area_info(
            df, fips_col="tract_id_fips", add_state=True, add_county=True
        )
    else:
        df = energy_comms.helpers.add_area_info(
            df, fips_col="county_id_fips", add_state=True, add_county=False
        )
    cols = [
        "county_name",
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
        "site_geometry",
        "area_geometry",
    ]
    if census_geometry == "tract":
        cols = ["tract_name", "tract_id_fips"] + cols
    df = df[cols]

    return df
