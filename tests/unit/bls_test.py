"""Unit tests for the employment criteria ETL modules."""

import logging
import pathlib

import pandas as pd

import energy_comms

logger = logging.getLogger(__name__)


def test_fossil_employment_qualifier(test_dir: pathlib.Path) -> None:
    """Test if fossil employment criteria function selects correct MSAs."""
    qcew_df = pd.read_csv(
        test_dir / "test_inputs/qcew_sample.csv",
        dtype={
            "industry_code": str,
        },
    )
    msa_df = pd.read_csv(
        test_dir / "test_inputs/msa_sample.csv", dtype={"county_id_fips": str}
    )
    fossil_df = (
        energy_comms.generate_qualifying_areas.fossil_employment_qualifying_areas(
            qcew_df=qcew_df, msa_df=msa_df
        )
    )
    fossil_expected = pd.DataFrame(
        {
            "msa_code": "C1018",
            "area_title": "Abilene, TX MSA",
            "year": 2020,
            "industry_code": "10",
            "own_code": 0,
            "total_employees": 67929,
            "fossil_employees": 1220,
            "percent_fossil_employment": 0.018,
            "meets_fossil_employment_threshold": 1,
            "county_id_fips": ["48059", "48253", "48441"],
            "county_title": [
                "Callahan County, Texas",
                "Jones County, Texas",
                "Taylor County, Texas",
            ],
            "geoid": ["48059", "48253", "48441"],
        }
    )
    logger.info(f"{len(fossil_df)}")
    fossil_output = (
        fossil_df[fossil_df.meets_fossil_employment_threshold == 1][
            list(fossil_expected.columns)
        ]
        .round(decimals={"percent_fossil_employment": 4})
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(fossil_expected, fossil_output)
