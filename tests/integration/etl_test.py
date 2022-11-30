"""Test functionality of the ETL for the energy communities criteria."""

import logging
from typing import Literal

import pytest

import energy_comms

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "census_res",
    [("tract"), ("county")],
)
def test_msha_etl(
    census_res: Literal["state", "county", "tract"],
    pudl_settings_fixure: dict[str, str] | None,
) -> None:
    """Verify that we can ETL the MSHA data."""
    raw_df = energy_comms.extract.msha.extract()
    if raw_df.empty:
        raise AssertionError("MSHA extract returned empty dataframe.")
    logger.info(f"Running transform at {census_res} level.")
    df = energy_comms.transform.msha.transform(
        raw_df, census_geometry=census_res, pudl_settings=pudl_settings_fixure
    )
    if df.empty:
        raise AssertionError("MSHA transform returned empty dataframe.")
    if "adjacent_id_fips" not in df.columns:
        raise AssertionError("adjacent_id_fips not in transfomred MSA dataframe.")
