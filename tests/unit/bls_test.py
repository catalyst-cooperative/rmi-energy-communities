"""Unit tests for the employment criteria ETL modules."""

import logging
import pathlib

import pandas as pd

import energy_comms

logger = logging.getLogger(__name__)


class TestEmploymentQualification:
    """Test if employment criteria functions selects correct MSAs.

    The tests in this class test the employment criteria related
    functions in ``energy_comms.generate_qualifying_areas``.
    """

    msa_to_county_df = None
    fossil_output = None
    unemployment_output = None

    def _get_msa_to_county_df(self, test_dir: pathlib.Path) -> pd.DataFrame:
        if self.msa_to_county_df is None:
            self.msa_to_county_df = pd.read_csv(
                test_dir / "test_inputs/msa_sample.csv", dtype={"county_id_fips": str}
            )
        return self.msa_to_county_df

    def test_fossil_fuel_qualifier(self, test_dir: pathlib.Path) -> None:
        """Test the fossil fuel employment criteria function."""
        qcew_msa_df = pd.read_csv(
            test_dir / "test_inputs/qcew_msa_sample.csv",
            dtype={
                "industry_code": str,
            },
        )
        qcew_nonmsa_df = pd.read_csv(
            test_dir / "test_inputs/qcew_nonmsa_sample.csv",
            dtype={"msa_code": str, "industry_code": str, "county_id_fips": str},
        )
        msa_fossil_output = pd.DataFrame(
            {
                "msa_code": "C1018",
                "area_title": "Abilene, TX MSA",
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
        self.fossil_output = (
            energy_comms.generate_qualifying_areas.fossil_employment_qualifying_areas(
                qcew_msa_df=qcew_msa_df,
                qcew_non_msa_county_df=qcew_nonmsa_df,
                msa_to_county=self._get_msa_to_county_df(test_dir=test_dir),
            )
        )
        fossil_output_small = (
            self.fossil_output[
                self.fossil_output.meets_fossil_employment_threshold == 1
            ][list(fossil_expected.columns)]
            .round(decimals={"percent_fossil_employment": 4})
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(fossil_expected, fossil_output_small)

    def test_unemployment_qualifier(self, test_dir: pathlib.Path) -> None:
        """Test the unemployment rate qualifiying function."""
        national_unemployment_df = pd.DataFrame(
            {
                "real_year": [2018, 2019],
                "national_unemployment_rate": [3.9, 3.7],
                "applies_to_criteria_year": [2019, 2020],
            }
        )
        lau_msa_df = pd.DataFrame(
            {
                "series_id": ["LAUMT481018000000003"] * 2 + ["LAUMT044974000000003"],
                "year": [2019, 2020, 2020],
                "msa_name": ["Abilene, TX Metropolitan Statistical Area"] * 2
                + ["Yuma, AZ Metropolitan Statistical Area"],
                "local_area_unemployment_rate": [3.0, 5.6, 17.0],
                "msa_code": ["C1018"] * 2 + ["C4974"],
            }
        )
        lau_non_msa_df = pd.DataFrame(
            {
                "series_id": ["LAUCN010050000000003"] * 2
                + ["LAUCN011090000000003"] * 2,
                "year": [2019, 2020, 2019, 2020],
                "msa_name": "Southeast Alabama nonmetropolitan area",
                "msa_code": "100004",
                "total_unemployment": [344.7, 675.9, 536.7, 864.8],
                "total_labor_force": [8636.2, 8680.2, 15570.0, 15840.6],
                "county_id_fips": ["01005"] * 2 + ["01109"] * 2,
            }
        )
        unemployment_expected = pd.DataFrame(
            {
                "series_id": ["LAUMT481018000000003"] * 6
                + ["LAUMT044974000000003"]
                + ["LAUCN010050000000003"] * 2
                + ["LAUCN011090000000003"] * 2,
                "year": [
                    2019,
                    2019,
                    2019,
                    2020,
                    2020,
                    2020,
                    2020,
                    2019,
                    2020,
                    2019,
                    2020,
                ],
                "local_area_unemployment_rate": [
                    3.0,
                    3.0,
                    3.0,
                    5.6,
                    5.6,
                    5.6,
                    17.0,
                    3.6,
                    6.2,
                    3.6,
                    6.2,
                ],
                "msa_name": ["Abilene, TX Metropolitan Statistical Area"] * 6
                + ["Yuma, AZ Metropolitan Statistical Area"]
                + ["Southeast Alabama nonmetropolitan area"] * 4,
                "meets_unemployment_threshold": [0, 0, 0, 1, 1, 1, 1, 0, 1, 0, 1],
                "county_id_fips": ["48059", "48253", "48441"] * 2
                + ["04027"]
                + ["01005"] * 2
                + ["01109"] * 2,
                "geoid": ["48059", "48253", "48441"] * 2
                + ["04027"]
                + ["01005"] * 2
                + ["01109"] * 2,
            }
        )
        self.unemployment_output = (
            energy_comms.generate_qualifying_areas.unemployment_rate_qualifying_areas(
                national_unemployment_df=national_unemployment_df,
                lau_msa_df=lau_msa_df,
                lau_non_msa_county_df=lau_non_msa_df,
                msa_to_county=self._get_msa_to_county_df(test_dir=test_dir),
            )
        )
        pd.testing.assert_frame_equal(
            unemployment_expected,
            self.unemployment_output[list(unemployment_expected.columns)],
        )

    def test_employment_criteria_qualifier(self, test_dir: pathlib.Path) -> None:
        """Test the function generating employment qualifying areas.

        Combine the outputs and the fossil and unemployment functions.
        """
        employment_expected = pd.DataFrame(
            {
                "county_name": [
                    "Callahan County",
                    "Jones County",
                    "Taylor County",
                    "Barbour County",
                    "Pike County",
                ],
                "county_id_fips": ["48059", "48253", "48441", "01005", "01109"],
                "state_id_fips": ["48"] * 3 + ["01"] * 2,
                "state_abbr": ["TX"] * 3 + ["AL"] * 2,
                "state_name": ["Texas"] * 3 + ["Alabama"] * 2,
                "geoid": ["48059", "48253", "48441", "01005", "01109"],
                "site_name": ["Abilene, TX MSA"] * 3
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
        if self.fossil_output is None:
            logger.info(
                "Fossil fuel qualifying areas dataframe isn't populated, running test_fossil_fuel_qualifier() to generate it."
            )
            self.test_fossil_fuel_qualifier(test_dir=test_dir)
        if self.unemployment_output is None:
            logger.info(
                "Unemployment qualifying areas dataframe isn't populated, running test_unemployment_qualifier() to generate it."
            )
            self.test_unemployment_qualifier(test_dir=test_dir)
        employment_output = (
            energy_comms.generate_qualifying_areas.employment_criteria_qualifying_areas(
                fossil_employment_df=self.fossil_output,
                unemployment_df=self.unemployment_output,
                census_county_df=county_df,
                census_state_df=state_df,
            )
        )[list(employment_expected.columns)]
        logger.info(employment_expected.columns)
        logger.info(employment_output.columns)
        pd.testing.assert_frame_equal(employment_expected, employment_output)
