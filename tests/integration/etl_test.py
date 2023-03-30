"""Test functionality of the ETL for the energy communities criteria."""
from __future__ import annotations

import logging
from typing import Any, Literal

import pytest
import sqlalchemy as sa

import energy_comms

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "table_name",
    [
        "fuel_ferc1",
        "ownership_eia860",
        "plants_entity_eia",
        "fuel_receipts_costs_eia923",
        "utilities_pudl",
    ],
)
def test_pudl_engine(pudl_engine_fixture: dict[Any, Any], table_name: str) -> None:
    """Get pudl_engine and do basic inspection."""
    assert isinstance(pudl_engine_fixture, sa.engine.Engine)  # nosec: B101
    insp = sa.inspect(pudl_engine_fixture)
    if table_name not in insp.get_table_names():
        raise AssertionError(f"{table_name} not in PUDL DB.")


@pytest.mark.parametrize(
    "census_res",
    [("tract"), ("county")],
)
def test_msha_etl(
    census_res: Literal["county", "tract"],
    pudl_settings_fixture: dict[Any, Any] | None,
) -> None:
    """Verify that we can ETL the MSHA data."""
    raw_df = energy_comms.extract.msha.extract()
    if raw_df.empty:
        raise AssertionError("MSHA extract returned empty dataframe.")
    logger.info(f"Running transform at {census_res} level.")
    df = energy_comms.transform.msha.transform(
        raw_df, census_geometry=census_res, pudl_settings=pudl_settings_fixture
    )
    if df.empty:
        raise AssertionError("MSHA transform returned empty dataframe.")
    if "adjacent_id_fips" not in df.columns:
        raise AssertionError("adjacent_id_fips not in transformed MSHA dataframe.")


@pytest.mark.parametrize(
    "census_res",
    [("tract"), ("county")],
)
def test_eia860_etl(
    pudl_engine_fixture: sa.engine.Engine,
    pudl_settings_fixture: dict[Any, Any] | None,
    census_res: Literal["county", "tract"],
) -> None:
    """Verify that we can ETL the EIA 860 data."""
    raw_df = energy_comms.extract.eia860.extract(pudl_engine=pudl_engine_fixture)
    if raw_df.empty:
        raise AssertionError("EIA 860 extract returned empty dataframe.")
    logger.info(f"Running transform at {census_res} level.")
    df = energy_comms.transform.eia860.transform(
        raw_df, census_geometry=census_res, pudl_settings=pudl_settings_fixture
    )
    if df.empty:
        raise AssertionError("EIA 860 transform returned empty dataframe.")
    if "adjacent_id_fips" not in df.columns:
        raise AssertionError("adjacent_id_fips not in transformed EIA 860 dataframe.")


def test_epa_etl(pudl_settings_fixture: dict[Any, Any] | None) -> None:
    """Verify that we can ETL the EPA brownfields data."""
    raw_df = energy_comms.extract.epa.extract()
    if raw_df.empty:
        raise AssertionError("EPA extract returned empty dataframe.")
    logger.info("Running EPA transform.")
    df = energy_comms.transform.epa.transform(
        raw_df, pudl_settings=pudl_settings_fixture
    )
    if df.empty:
        raise AssertionError("EPA transform returned empty dataframe.")
    if not df[(df.county_id_fips.str.len() != 5) & ~(df.county_id_fips.isnull())].empty:
        raise AssertionError(
            "EPA county ID FIPS column is not all 5 character strings."
        )


def test_bls_etl() -> None:
    """Verify that we can ETL the BLS employment data."""
    # begin with unemployment criteria
    # for speed, only extract 2020-2024 data
    nat_unemployment_df = energy_comms.extract.bls.extract_national_unemployment_rates()
    if nat_unemployment_df.empty:
        raise AssertionError(
            "National unemployment rate extract returned empty dataframe."
        )
    nat_unemployment_df = (
        energy_comms.transform.bls.transform_national_unemployment_rates(
            nat_unemployment_df
        )
    )
    raw_lau_df = energy_comms.extract.bls.extract_lau_data(
        file_list=["la.data.0.CurrentU20-24"], update=True
    )
    if raw_lau_df.empty:
        raise AssertionError(
            "Local unemployment data extract returned empty dataframe."
        )
    raw_lau_area_df = energy_comms.extract.bls.extract_lau_area_table(update=True)
    if raw_lau_area_df.empty:
        raise AssertionError(
            "Local unemployment data areas extract returned empty dataframe."
        )
    lau_area_df = energy_comms.transform.bls.transform_lau_areas(raw_df=raw_lau_area_df)
    non_msa_df = energy_comms.extract.bls.extract_nonmsa_area_defs()
    non_msa_df = energy_comms.transform.bls.transform_nonmsa_area_defs(non_msa_df)
    (
        lau_msa_df,
        lau_non_msa_df,
    ) = energy_comms.transform.bls.transform_local_area_unemployment_rates(
        raw_lau_df=raw_lau_df, area_df=lau_area_df, non_msa_df=non_msa_df
    )
    msa_county_crosswalk = energy_comms.extract.bls.extract_msa_county_crosswalk()
    if msa_county_crosswalk.empty:
        raise AssertionError(
            "MSA to county crosswalk extract returned empty dataframe."
        )
    msa_county_crosswalk = energy_comms.transform.bls.transform_msa_county_crosswalk(
        msa_county_crosswalk
    )
    unemployment_df = (
        energy_comms.generate_qualifying_areas.unemployment_rate_qualifying_areas(
            national_unemployment_df=nat_unemployment_df,
            lau_msa_df=lau_msa_df,
            lau_non_msa_county_df=lau_non_msa_df,
            msa_to_county=msa_county_crosswalk,
        )
    )
    if unemployment_df.empty:
        raise AssertionError("Unemployment criteria function returned empty dataframe.")
    if unemployment_df.geoid.isnull().values.any():
        raise AssertionError(
            "Unemployment criteria dataframe contains null values in geoid column."
        )

    # now do fossil fuel employment criteria with 2020 data
    year = 2020
    qcew_df = energy_comms.extract.bls.extract_qcew_data(years=[year], update=True)
    if qcew_df.empty:
        raise AssertionError(f"{year} QCEW data extract returned empty dataframe.")
    qcew_msa_df, qcew_non_msa_df = energy_comms.transform.bls.transform_qcew_data(
        qcew_df, non_msa_df=non_msa_df
    )
    fossil_employment_df = (
        energy_comms.generate_qualifying_areas.fossil_employment_qualifying_areas(
            qcew_msa_df=qcew_msa_df,
            qcew_non_msa_county_df=qcew_non_msa_df,
            msa_to_county=msa_county_crosswalk,
        )
    )
    if fossil_employment_df.empty:
        raise AssertionError(
            "Fossil fuel employment criteria function returned empty dataframe."
        )
    if fossil_employment_df.geoid.isnull().values.any():
        raise AssertionError(
            "Fossil fuel employment criteria dataframe contains null values in geoid column."
        )
