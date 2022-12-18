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
    """Test that the PUDL DB is actually available."""
    insp = sa.inspect(pudl_engine_fixture)
    if table_name not in insp.get_table_names():
        raise AssertionError(f"{table_name} not in PUDL DB.")


@pytest.mark.parametrize(
    "census_res",
    [("tract"), ("county")],
)
def test_msha_etl(
    census_res: Literal["state", "county", "tract"],
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
    census_res: Literal["state", "county", "tract"],
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
