"""Coordinate calling functions to generate the end dataframes for each of the IRA criteria."""

from typing import Literal

import pandas as pd

import energy_comms
from energy_comms.extract.bls import QCEW_YEARS


def get_coal_criteria_qualifying_areas(
    census_geometry: Literal["county", "tract"] = "tract"
) -> pd.DataFrame:
    """Get dataframe of qualifying areas under the closed coal mine or plant criteria.

    Args:
        census_geometry: The Census geometry level of qualifying areas. Must
            be one of "county", or "tract".
    """
    msha_raw_df = energy_comms.extract.msha.extract()
    msha_df = energy_comms.transform.msha.transform(
        msha_raw_df, census_geometry=census_geometry
    )
    eia_raw_df = energy_comms.extract.eia860.extract()
    eia_df = energy_comms.transform.eia860.transform(
        eia_raw_df, census_geometry=census_geometry
    )
    df = energy_comms.generate_qualifying_areas.coal_criteria_qualifying_areas(
        msha_df=msha_df, eia_df=eia_df, census_geometry=census_geometry
    )
    return df


def get_brownfields_criteria_qualifying_areas(
    census_geometry: Literal["county", "tract"] = "tract"
) -> pd.DataFrame:
    """Get dataframe of qualifying areas under the brownfields criteria.

    Args:
        census_geometry: The Census geometry level of qualifying areas. Must
            be one of "county", or "tract".
    """
    epa_raw_df = energy_comms.extract.epa.extract()
    epa_df = energy_comms.transform.epa.transform(
        epa_raw_df, census_geometry=census_geometry
    )
    if census_geometry == "tract":
        epa_df = energy_comms.helpers.add_area_info(
            epa_df, fips_col="tract_id_fips", add_state=True, add_county=True
        )
    else:
        epa_df = energy_comms.helpers.add_area_info(
            epa_df, fips_col="county_id_fips", add_state=True, add_county=False
        )
    epa_df["geoid"] = epa_df[f"{census_geometry}_id_fips"]
    cols = [
        "county_name",
        "county_id_fips",
        "state_id_fips",
        "state_abbr",
        "state_name",
        "geoid",
        "qualifying_criteria",
        "qualifying_area",
        "site_name",
        "latitude",
        "longitude",
        "site_geometry",
        "area_geometry",
    ]
    if census_geometry == "tract":
        cols = ["tract_name", "tract_id_fips"] + cols
    epa_df = epa_df[cols]
    return epa_df


def get_employment_criteria_qualifying_areas(update: bool = False) -> pd.DataFrame:
    """Get dataframe of qualifying areas under the employment criteria.

    Args:
        update: Default is False. Whether to pull fresh downloads (True)
            of all the data or use ``energy_comms.DATA_DIR`` inputs (False).
    """
    # start with fossil fuel employment criteria
    energy_comms.extract.bls.download_qcew_data(update=update)
    msa_county_raw_df = energy_comms.extract.bls.extract_msa_county_crosswalk()
    msa_to_county_df = energy_comms.transform.bls.transform_msa_county_crosswalk(
        msa_county_raw_df
    )
    non_msa_to_county_raw_df = (
        energy_comms.extract.bls.extract_nonmsa_county_crosswalk()
    )
    non_msa_to_county_df = energy_comms.transform.bls.transform_nonmsa_county_crosswalk(
        non_msa_to_county_raw_df, msa_to_county_df
    )
    # do one year at a time so the concatenated dataframe isn't as big
    fossil_employment_df = pd.DataFrame()
    for year in QCEW_YEARS:
        year_df = energy_comms.extract.bls.extract_qcew_data(years=[year])
        if year_df.empty:
            continue
        year_msa_df, year_nonmsa_df = energy_comms.transform.bls.transform_qcew_data(
            year_df, non_msa_county_crosswalk=non_msa_to_county_df
        )
        year_df = (
            energy_comms.generate_qualifying_areas.fossil_employment_qualifying_areas(
                qcew_msa_df=year_msa_df,
                qcew_non_msa_county_df=year_nonmsa_df,
                msa_to_county=msa_to_county_df,
            )
        )
        fossil_employment_df = pd.concat([fossil_employment_df, year_df])

    # now do unemployment criteria
    cps_raw_df = energy_comms.extract.bls.extract_national_unemployment_rates()
    lau_raw_df = energy_comms.extract.bls.extract_lau_rates(update=update)
    cps_df = energy_comms.transform.bls.transform_national_unemployment_rates(
        cps_raw_df
    )
    lau_df = energy_comms.transform.bls.transform_local_area_unemployment_rates(
        raw_lau_df=lau_raw_df,
        non_msa_county_crosswalk=non_msa_to_county_df,
        msa_county_crosswalk=msa_to_county_df,
    )
    unemployment_df = (
        energy_comms.generate_qualifying_areas.unemployment_rate_qualifying_areas(
            national_unemployment_df=cps_df, lau_df=lau_df
        )
    )

    # bring these dataframes together to get full employment criteria
    df = energy_comms.generate_qualifying_areas.employment_criteria_qualifying_areas(
        fossil_employment_df=fossil_employment_df, unemployment_df=unemployment_df
    )

    return df


def get_all_qualifying_areas(
    coal_census_geometry: Literal["county", "tract"] = "tract",
    brownfields_census_geometry: Literal["county", "tract"] = "tract",
    update_employment: bool = False,
) -> pd.DataFrame:
    """Get dataframe of all qualifying areas from all criteria."""
    coal_df = get_coal_criteria_qualifying_areas(census_geometry=coal_census_geometry)
    brownfields_df = get_brownfields_criteria_qualifying_areas(
        census_geometry=brownfields_census_geometry
    )
    employment_df = get_employment_criteria_qualifying_areas(update=update_employment)
    full_df = pd.concat(
        [coal_df, brownfields_df, employment_df], ignore_index=True
    ).set_crs("EPSG:4269")
    return full_df
