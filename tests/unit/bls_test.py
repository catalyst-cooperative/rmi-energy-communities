"""Unit tests for the employment criteria ETL modules."""

import logging
import pathlib

import pandas as pd
import pytest

import energy_comms

logger = logging.getLogger(__name__)


class TestEmploymentQualification:
    """Test if employment criteria functions selects correct MSAs.

    The tests in this class test the employment criteria related
    functions in ``energy_comms.generate_qualifying_areas``.
    """

    @pytest.fixture
    def msa_to_county_df(self, test_dir: pathlib.Path) -> pd.DataFrame:
        """Return the MSA to county crosswalk from test inputs."""
        msa_to_county_df = pd.read_csv(
            test_dir / "test_inputs/msa_sample.csv", dtype={"county_id_fips": str}
        )
        return msa_to_county_df

    @pytest.fixture
    def non_msa_to_county_df(self, test_dir: pathlib.Path) -> pd.DataFrame:
        """Return the non-MSA to county crosswalk from test inputs."""
        non_msa_to_county_df = pd.read_csv(
            test_dir / "test_inputs/non_msa_sample.csv",
            dtype={"county_id_fips": str, "msa_code": str},
        )
        return non_msa_to_county_df

    @pytest.fixture
    def expected_fossil_output(self) -> pd.DataFrame:
        """Return the expected output of the fossil fuel criteria test."""
        msa_fossil_output = pd.DataFrame(
            {
                "msa_code": "C1018",
                "area_title": "Abilene, TX",
                "year": 2020,
                "total_employees": 67929,
                "fossil_employees": 1220,
                "percent_fossil_employment": 0.018,
                "meets_fossil_employment_threshold": 1,
                "county_id_fips": ["48059", "48253", "48441"],
                "geoid": ["48059", "48253", "48441"],
            }
        )
        non_msa_fossil_output = pd.DataFrame(
            {
                "msa_code": "100004",
                "area_title": "Southeast Alabama nonmetropolitan area",
                "year": 2020,
                "total_employees": 22269,
                "fossil_employees": 1700,
                "percent_fossil_employment": 0.0763,
                "meets_fossil_employment_threshold": 1,
                "county_id_fips": ["01005", "01109"],
                "geoid": ["01005", "01109"],
            }
        )
        fossil_expected = pd.concat(
            [msa_fossil_output, non_msa_fossil_output]
        ).reset_index(drop=True)
        return fossil_expected

    @pytest.fixture
    def expected_unemployment_output(self) -> pd.DataFrame:
        """Return the expected output of the unemployment criteria test."""
        unemployment_expected = pd.DataFrame(
            {
                "county_id_fips": ["48059", "48059", "01005", "01109"],
                "msa_code": ["C1018", "C1018", "100004", "100004"],
                "msa_name": [
                    "Abilene, TX",
                    "Abilene, TX",
                    "Southeast Alabama nonmetropolitan area",
                    "Southeast Alabama nonmetropolitan area",
                ],
                "year": [2019, 2020, 2020, 2020],
                "local_area_unemployment_rate": [3.5, 21.0, 4.8, 4.8],
                "meets_unemployment_threshold": [0, 1, 1, 1],
                "geoid": ["48059", "48059", "01005", "01109"],
            }
        ).astype({"year": "Int64"})

        return unemployment_expected

    def test_fossil_fuel_qualifier(
        self,
        test_dir: pathlib.Path,
        expected_fossil_output: pd.DataFrame,
        msa_to_county_df: pd.DataFrame,
        non_msa_to_county_df: pd.DataFrame,
    ) -> None:
        """Test the fossil fuel employment criteria function."""
        raw_qcew_df = pd.read_csv(test_dir / "test_inputs/qcew_raw_sample.csv")
        clean_qcew_df = energy_comms.transform.bls.transform_qcew_data(
            df=raw_qcew_df,
            msa_county_crosswalk=msa_to_county_df,
            non_msa_county_crosswalk=non_msa_to_county_df,
        )
        fossil_output = (
            energy_comms.generate_qualifying_areas.fossil_employment_qualifying_areas(
                qcew_df=clean_qcew_df
            )
        )
        fossil_output_small = (
            fossil_output[fossil_output.meets_fossil_employment_threshold == 1][
                list(expected_fossil_output.columns)
            ]
            .round(decimals={"percent_fossil_employment": 4})
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(expected_fossil_output, fossil_output_small)

    def test_unemployment_qualifier(
        self,
        expected_unemployment_output: pd.DataFrame,
        msa_to_county_df: pd.DataFrame,
        non_msa_to_county_df: pd.DataFrame,
    ) -> None:
        """Test the unemployment rate qualifying function."""
        national_unemployment_df = pd.DataFrame(
            {
                "real_year": [2018, 2019],
                "national_unemployment_rate": [3.9, 3.7],
                "applies_to_criteria_year": [2019, 2020],
            }
        )
        lau_raw_df = pd.DataFrame(
            {
                "series_id": [
                    "LAUCN480590000000004",
                    "LAUCN480590000000004",
                    "LAUCN480590000000006",
                    "LAUCN480590000000006",
                    "LAUCN480590000000004",
                    "LAUCN480590000000004",
                    "LAUCN480590000000006",
                    "LAUCN480590000000006",
                    "LAUCN010050000000004",
                    "LAUCN010050000000004",
                    "LAUCN010050000000006",
                    "LAUCN010050000000006",
                    "LAUCN011090000000004",
                    "LAUCN011090000000004",
                    "LAUCN011090000000006",
                    "LAUCN011090000000006",
                ],
                "year": [2019] * 4 + [2020] * 12,
                "period": ["M01", "M02"] * 8,
                "value": [
                    100.0,
                    150.0,
                    3500,
                    3600,
                    500.0,
                    448.0,
                    2000.0,
                    2500.0,
                    344,
                    675,
                    8680,
                    8636,
                    500,
                    864,
                    15570,
                    15800,
                ],
            }
        )

        lau_actual = energy_comms.transform.bls.transform_local_area_unemployment_rates(
            raw_lau_df=lau_raw_df,
            non_msa_county_crosswalk=non_msa_to_county_df,
            msa_county_crosswalk=msa_to_county_df,
        ).reset_index()
        unemployment_output = (
            energy_comms.generate_qualifying_areas.unemployment_rate_qualifying_areas(
                national_unemployment_df=national_unemployment_df, lau_df=lau_actual
            )
        )
        pd.testing.assert_frame_equal(
            expected_unemployment_output,
            unemployment_output[list(expected_unemployment_output.columns)],
        )

    def test_employment_criteria_qualifier(
        self,
        test_dir: pathlib.Path,
        expected_unemployment_output: pd.DataFrame,
        expected_fossil_output: pd.DataFrame,
    ) -> None:
        """Test the function generating employment qualifying areas.

        Combine the outputs and the fossil and unemployment functions.
        """
        employment_expected = pd.DataFrame(
            {
                "county_name": [
                    "Callahan County",
                    "Barbour County",
                    "Pike County",
                ],
                "county_id_fips": ["48059", "01005", "01109"],
                "state_id_fips": ["48"] + ["01"] * 2,
                "state_abbr": ["TX"] + ["AL"] * 2,
                "state_name": ["Texas"] + ["Alabama"] * 2,
                "geoid": ["48059", "01005", "01109"],
                "site_name": ["Abilene, TX"]
                + ["Southeast Alabama nonmetropolitan area"] * 2,
                "qualifying_criteria": "fossil_fuel_employment",
                "qualifying_area": "MSA or non-MSA",
            }
        )
        state_df = pd.read_csv(
            test_dir / "test_inputs/tx_al_census_state_df.csv", dtype=str
        )
        county_df = pd.read_pickle(
            test_dir / "test_inputs/tx_al_census_counties_gdf.pkl.gz"
        )  # nosec

        # currently testing fewer counties in unemployment test
        county_id_fips_list = expected_unemployment_output.county_id_fips.unique()
        fossil_employment_df = expected_fossil_output[
            expected_fossil_output.county_id_fips.isin(county_id_fips_list)
        ]
        employment_output = (
            energy_comms.generate_qualifying_areas.employment_criteria_qualifying_areas(
                fossil_employment_df=fossil_employment_df,
                unemployment_df=expected_unemployment_output,
                census_county_df=county_df,
                census_state_df=state_df,
            )
        )[list(employment_expected.columns)]
        pd.testing.assert_frame_equal(employment_expected, employment_output)
