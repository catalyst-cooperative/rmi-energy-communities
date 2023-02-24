"""Unit tests for the employment criteria ETL modules."""

import logging
import pathlib

import pandas as pd

import energy_comms

logger = logging.getLogger(__name__)


def test_employment_qualifier(test_dir: pathlib.Path) -> None:
    """Test if employment criteria functions selects correct MSAs.

    First test the fossil fuel employment criteria. Then test
    the unemployment criteria. Finally, use the outputs of this to
    test if the employment criteria functions find the correct
    qualifying MSAs.
    """
    # first test fossil employment functions
    qcew_df = pd.read_csv(
        test_dir / "test_inputs/qcew_sample.csv",
        dtype={
            "industry_code": str,
        },
    )
    msa_df = pd.read_csv(
        test_dir / "test_inputs/msa_sample.csv", dtype={"county_id_fips": str}
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
    fossil_output = (
        energy_comms.generate_qualifying_areas.fossil_employment_qualifying_areas(
            qcew_df=qcew_df, msa_df=msa_df
        )
    )
    fossil_output_small = (
        fossil_output[fossil_output.meets_fossil_employment_threshold == 1][
            list(fossil_expected.columns)
        ]
        .round(decimals={"percent_fossil_employment": 4})
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(fossil_expected, fossil_output_small)

    # now test unemployment functions
    national_unemployment_df = pd.DataFrame(
        {
            "real_year": [2018, 2019],
            "national_unemployment_rate": [3.9, 3.7],
            "applies_to_criteria_year": [2019, 2020],
        }
    )
    lau_df = pd.DataFrame(
        {
            "series_id": ["LAUMT481018000000003"] * 2 + ["LAUMT044974000000003"],
            "year": [2019, 2020, 2020],
            "area_title": ["Abilene, TX Metropolitan Statistical Area"] * 2
            + ["Yuma, AZ Metropolitan Statistical Area"],
            "local_area_unemployment_rate": [3.0, 5.6, 17.0],
            "msa_code": ["C1018"] * 2 + ["C4974"],
        }
    )
    unemployment_expected = pd.DataFrame(
        {
            "series_id": ["LAUMT481018000000003"] * 6 + ["LAUMT044974000000003"],
            "year": [2019, 2019, 2019, 2020, 2020, 2020, 2020],
            "local_area_unemployment_rate": [3.0, 3.0, 3.0, 5.6, 5.6, 5.6, 17.0],
            "area_title": ["Abilene, TX Metropolitan Statistical Area"] * 6
            + ["Yuma, AZ Metropolitan Statistical Area"],
            "county_title": [
                "Callahan County, Texas",
                "Jones County, Texas",
                "Taylor County, Texas",
            ]
            * 2
            + ["Yuma County, Arizona"],
            "meets_unemployment_threshold": [0, 0, 0, 1, 1, 1, 1],
            "county_id_fips": ["48059", "48253", "48441"] * 2 + ["04027"],
            "geoid": ["48059", "48253", "48441"] * 2 + ["04027"],
        }
    )
    unemployment_output = (
        energy_comms.generate_qualifying_areas.unemployment_rate_qualifying_areas(
            national_unemployment_df=national_unemployment_df,
            lau_unemployment_df=lau_df,
            msa_df=msa_df,
        )
    )
    pd.testing.assert_frame_equal(
        unemployment_expected, unemployment_output[list(unemployment_expected.columns)]
    )

    # test the combination of the fossil and unemployment outputs for
    # final employment qualifying areas
    employment_expected = pd.DataFrame(
        {
            "county_name": [
                "Callahan County, Texas",
                "Jones County, Texas",
                "Taylor County, Texas",
            ],
            "county_id_fips": ["48059", "48253", "48441"],
            "state_id_fips": "48",
            "state_abbr": "TX",
            "state_name": "Texas",
            "geoid": ["48059", "48253", "48441"],
            "site_name": "Abilene, TX",
            "qualifying_criteria": "fossil_fuel_employment",
            "qualifying_area": "MSA",
        }
    )
    state_df = pd.read_csv(
        test_dir / "test_inputs/texas_census_state_df.csv", dtype=str
    )
    county_df = pd.read_pickle(
        test_dir / "test_inputs/texas_census_counties_gdf.pkl.gz"
    )
    employment_output = (
        energy_comms.generate_qualifying_areas.employment_criteria_qualifying_areas(
            fossil_employment_df=fossil_output,
            unemployment_df=unemployment_output,
            census_county_df=county_df,
            census_state_df=state_df,
        )
    )[list(employment_expected.columns)]
    pd.testing.assert_frame_equal(employment_expected, employment_output)
