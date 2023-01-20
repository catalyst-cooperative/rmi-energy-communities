"""Coordinate calling functions to generate the end dataframes for each of the IRA criteria."""

from typing import Literal

import pandas as pd

import energy_comms
from energy_comms.extract.bls import QCEW_YEARS


def get_coal_criteria_qualifying_areas(
    census_geometry: Literal["state", "county", "tract"] = "tract"
) -> pd.DataFrame:
    """Get dataframe of qualifying areas under the closed coal mine or plant criteria.

    Args:
        census_geometry: The Census geometry level of qualifying areas. Must
            be one of "state", "county", or "tract".
    """
    msha_raw_df = energy_comms.extract.msha.extract()
    msha_df = energy_comms.transform.msha.transform(
        msha_raw_df, census_geometry=census_geometry
    )
    eia_raw_df = energy_comms.extract.eia860.extract()
    eia_df = energy_comms.transform.eia860.transform(
        eia_raw_df, census_geometry=census_geometry
    )
    df = energy_comms.output.generate_qualifying_areas.coal_plant_mine_criteria_qualifying_areas(
        msha_df=msha_df, eia_df=eia_df, census_geometry=census_geometry
    )
    return df


def get_brownfields_criteria_qualifying_areas(
    census_geometry: Literal["state", "county", "tract"] = "tract"
) -> pd.DataFrame:
    """Get dataframe of qualifying areas under the brownfields criteria.

    Args:
        census_geometry: The Census geometry level of qualifying areas. Must
            be one of "state", "county", or "tract".
    """
    epa_raw_df = energy_comms.extract.epa.extract()
    epa_df = energy_comms.transform.epa.transform(
        epa_raw_df, census_geometry=census_geometry
    )
    cols = [
        f"{census_geometry}_id_fips",
        f"{census_geometry}_name_census",
        "latitude",
        "longitude",
        "geometry",
        "qualifying_area",
        "qualifying_criteria",
    ]
    df = epa_df[cols]
    return df


def get_employment_criteria_qualifying_areas(update: bool = False) -> pd.DataFrame:
    """Get dataframe of qualifying areas under the employment criteria.

    Args:
        update: Default is False. Whether to pull fresh downloads (True)
            of all the data or use ``energy_comms.DATA_DIR`` inputs (False).
    """
    # start with fossil fuel employment criteria
    energy_comms.extract.bls.download_qcew_data(update=update)
    msa_raw_df = energy_comms.extract.bls.extract_msa_codes()
    msa_df = energy_comms.transform.bls.transform_msa_codes(msa_raw_df)
    # do one year at a time so the concatenated dataframe isn't as big
    fossil_employment_df = pd.DataFrame()
    for year in QCEW_YEARS:
        year_df = energy_comms.extract.bls.extract_qcew_data(years=[year])
        year_df = energy_comms.transform.bls.transform_qcew_data(year_df)
        year_df = energy_comms.output.generate_qualifying_areas.fossil_employment_qualifying_areas(
            year_df, msa_df
        )
        fossil_employment_df = pd.concat([fossil_employment_df, year_df])

    # now do unemployment criteria
    cps_raw_df = energy_comms.extract.bls.extract_national_unemployment_rates()
    lau_raw_df = energy_comms.extract.bls.extract_lau_rates(update=update)
    lau_area_raw_df = energy_comms.extract.bls.extract_lau_area_table(update=update)
    cps_df = energy_comms.transform.bls.transform_national_unemployment_rates(
        cps_raw_df
    )
    lau_df = energy_comms.transform.bls.transform_local_area_unemployment_rates(
        lau_raw_df, lau_area_raw_df
    )
    unemployment_df = energy_comms.output.generate_qualifying_areas.unemployment_rate_qualifying_areas(
        cps_df, lau_df
    )

    # bring these dataframes together to get full employment criteria
    df = energy_comms.output.generate_qualifying_areas.employment_criteria_qualifying_areas(
        fossil_employment_df=fossil_employment_df, unemployment_df=unemployment_df
    )
    # TODO: fix this instead of renaming
    df = df.rename(columns={"full_county_id_fips": "county_id_fips"}).reset_index()
    return df


def get_all_qualifying_areas(
    coal_census_geometry: Literal["state", "county", "tract"] = "tract",
    brownfields_census_geometry: Literal["state", "county", "tract"] = "tract",
    update_employment: bool = False,
) -> pd.DataFrame:
    """Get dataframe of all qualifying areas from all criteria."""
    coal_df = get_coal_criteria_qualifying_areas(census_geometry=coal_census_geometry)
    brownfields_df = get_brownfields_criteria_qualifying_areas(
        census_geometry=brownfields_census_geometry
    )
    employment_df = get_employment_criteria_qualifying_areas(update=update_employment)
    full_df = pd.concat([coal_df, brownfields_df, employment_df], ignore_index=True)
    return full_df
