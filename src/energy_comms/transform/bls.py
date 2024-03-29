"""Transform functions for Bureau of Labor Statistics data for employment criteria."""
import logging
import math

import pandas as pd

logger = logging.getLogger(__name__)

FOSSIL_NAICS_CODES = [
    "211",
    "2121",
    "213111",
    "213112",
    "213113",
    "32411",
    "4861",
    "4862",
]

OLD_FOSSIL_NAICS_CODES = ["2121", "211", "213", "23712", "486", "4247", "22112"]


def transform_national_unemployment_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the raw national unemployment rate data and get annual avg for each year.

    Strip and lower column names, enforce types, rename columns. Groupby year
    and get the annual average unemployment rate.

    Args:
        df: The raw dataframe extracted from BLS website.

    Returns:
        Dataframe with the annual average national unemployment rate.
    """
    df.columns = df.columns.str.strip().str.lower()
    df["series_id"] = df["series_id"].str.strip()
    df = (
        df.rename(
            columns={"periodname": "month", "value": "national_unemployment_rate"}
        )
        .dropna(subset=["year"])
        .astype(
            {
                "year": "Int64",
                "period": "string",
                "month": "string",
                "national_unemployment_rate": "float",
                "series_id": "string",
            }
        )
        .sort_values(by=["year", "period"])
    )
    # other values like averages could be included with other period values
    df = df[(df.period >= "M01") & (df.period <= "M12")]
    # now take the annual average
    # note the rounding bc BLS website specifies 1 sig figure
    df = df.groupby("year")["national_unemployment_rate"].mean().round(1).reset_index()
    # IRA criteria specifies national unemployment rate of the previous year
    df["applies_to_criteria_year"] = df["year"] + 1
    df = df.rename(columns={"year": "real_year"})

    return df


def transform_lau_areas(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Transform local areas dataframe.

    Note: this table is no longer needed. Keeping this function
    in case the LAU area table provides helpful context later.

    Construct the BLS series ID for records that refer to a county
    or metropolitan statistical area. Add state FIPS ID column.
    For more information on these BLS series IDs
    see https://www.bls.gov/help/hlpforma.htm#LA

    This table gives the area name for the LAU data.
    """
    df = raw_df.copy()
    df.columns = df.columns.str.strip().str.lower()
    df = df.astype(
        {"area_type_code": "string", "area_code": "string", "area_text": "string"}
    )
    # only keep records for MSAs and counties
    df = df[df.area_code.str[:2].isin(["MT", "CN"])]
    df["state_id_fips"] = df["area_code"].str[2:4]
    # construct the local area unemployment series ID
    df["series_id"] = "LAU" + df["area_code"].str[:10]
    df["series_id"] = df["series_id"].str.pad(width=18, side="right", fillchar="0")
    # construct the MSA code for the MSA to county crosswalk
    df.loc[df.area_code.str[:2] == "MT", "msa_code"] = "C" + df["area_code"].str[4:8]
    df.loc[df.area_code.str[:2] == "CN", "county_id_fips"] = df["area_code"].str[2:7]

    return df


def transform_local_area_unemployment_rates(
    raw_lau_df: pd.DataFrame,
    non_msa_county_crosswalk: pd.DataFrame,
    msa_county_crosswalk: pd.DataFrame,
) -> pd.DataFrame:
    """Get the annual average local unemployment rate and area information.

    Clean the LAU data, filter area data down to counties, merge on area
    information and MSA/non-MSA information, aggregate county LAU data within
    statistical areas and calculate the annual average unemployment.


    For a format description of the series IDs used in the LAU data, see
    https://www.bls.gov/help/hlpforma.htm#LA

    Args:
        raw_lau_df: The raw local area unemployment data.
        non_msa_county_crosswalk: Crosswalk between non-MSA regions and the
            counties within.
        msa_county_crosswalk: Crosswalk between MSA regions and the counties within.

    Returns:
        lau_df: Dataframe giving the annual average unemployment rate for each
            county and year, and the MSA or non-MSA it is contained in.
    """
    lau_df = raw_lau_df.copy()
    lau_df.columns = lau_df.columns.str.strip().str.lower()
    lau_df["series_id"] = lau_df["series_id"].str.strip()
    # convert to float and make invalid values null
    lau_df["value"] = pd.to_numeric(lau_df["value"], errors="coerce")
    lau_df = lau_df.astype(
        {
            "year": "Int64",
            "period": "string",
            "series_id": "string",
        }
    )
    # filter out M13 (annual average) values
    lau_df = lau_df[(lau_df.period >= "M01") & (lau_df.period <= "M12")]
    # filter for just counties
    lau_df = lau_df[lau_df.series_id.str[3:5].isin(["CN"])]
    # get the unemployment total (04) and labor force total (06) stats
    lau_df = lau_df[lau_df.series_id.str[-2:].isin(["04", "06"])]
    lau_df = lau_df.dropna(subset=["value"])
    # take an annual average, didn't use M13 here because it is null
    # (footnote code U) when any monthly value is missing (footnote code N)
    # note the rounding bc BLS website specifies 1 sig figure
    lau_df = (
        lau_df.groupby(by=["series_id", "year"])["value"].mean().round(1).reset_index()
    )
    labor_force = lau_df[lau_df.series_id.str[-2:] == "06"][
        ["series_id", "year", "value"]
    ]
    labor_force = labor_force.rename(columns={"value": "total_labor_force"})
    # remake series_id to be mergeable with the unemployment numbers series ID
    labor_force["series_id"] = labor_force["series_id"].str[:-2] + "04"
    unemployment_df = lau_df[lau_df.series_id.str[-2:] == "04"]
    unemployment_df = unemployment_df.rename(columns={"value": "total_unemployment"})
    lau_county_df = unemployment_df.merge(
        labor_force, how="left", on=["series_id", "year"]
    )
    lau_county_df["county_id_fips"] = lau_county_df.series_id.str[5:10]

    # merge onto non-MSAs to get LAU data for the counties within a non-MSA
    lau_non_msa_df = non_msa_county_crosswalk[
        ["msa_code", "msa_name", "county_id_fips"]
    ].merge(lau_county_df, how="left", on="county_id_fips", indicator=True)
    # merge onto MSAs to get LAU data for the counties within an MSA
    lau_msa_df = msa_county_crosswalk[["msa_code", "msa_name", "county_id_fips"]].merge(
        lau_county_df, how="left", on="county_id_fips", indicator=True
    )
    lau_df = pd.concat([lau_msa_df, lau_non_msa_df])

    # there should be no overlapping counties between MSAs and non-MSAs
    if len(lau_df[lau_df.duplicated(subset=["county_id_fips", "year"])]) > 0:
        raise AssertionError(
            "Duplicate county FIPS codes in combined MSA and non-MSA LAU county dataframe."
        )
    if len(lau_df[lau_df._merge == "left_only"]) > 0:
        logger.warning(
            "There are counties within MSAs or non-MSAs which don't have any LAU unemployment data."
        )
    # filter out counties that aren't in the LAU data
    lau_df = lau_df[lau_df._merge == "both"]

    # divide the unemployment total by the total labor force to get unemployment rate
    unemployment_rates = lau_df.groupby(["msa_code", "year"])[
        ["total_unemployment", "total_labor_force"]
    ].sum()
    unemployment_rates["local_area_unemployment_rate"] = (
        unemployment_rates["total_unemployment"]
        / unemployment_rates["total_labor_force"]
        # round down to three decimals and make percent, not simplifying expression for clarity
    ).apply(lambda x: math.floor(x * 1000) / 1000) * 100
    lau_df = lau_df.merge(
        unemployment_rates[["local_area_unemployment_rate"]],
        how="left",
        left_on=["msa_code", "year"],
        right_index=True,
    )

    return lau_df


def transform_nonmsa_county_crosswalk(
    df: pd.DataFrame, msa_county_crosswalk: pd.DataFrame
) -> pd.DataFrame:
    """Transform dataframe of non-MSA data linked to the counties within.

    Clean column names, enforce string types, pad FIPS codes with 0s,
    filter for nonmetropolitan statistical areas. Drop counties that are
    already in an MSA in the MSA to county crosswalk. Per Treasury/IRS
    guidance, counties in non-MSAs can have no part in an MSA as well.

    Args:
        df: Raw dataframe of non-MSA codes and names.
        msa_county_crosswalk: Transformed crosswalk from MSA to counties.
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    col_rename_dict = {
        "fips_code": "state_id_fips",
        "county_code": "county_id_fips",
        "township_code": "township_id_fips",
    }
    # msa names and codes column have names like may_2021_msa_code
    # make this name independent of date of data publishing
    col_rename_dict.update(
        {
            df.columns[df.columns.str.contains("msa_name")][0]: "msa_name",
            df.columns[df.columns.str.contains("msa_code")][0]: "msa_code",
            df.columns[df.columns.str.contains("county_name")][0]: "county_name",
        }
    )
    # all columns are string type
    df = df.rename(columns=col_rename_dict).astype("string")
    # filter for non-MSAs
    df = df[df.msa_name.str.contains("nonmetropolitan")]
    # construct FIPS codes
    df["state_id_fips"] = df["state_id_fips"].str.zfill(2)
    df["county_id_fips"] = df["state_id_fips"] + df["county_id_fips"].str.zfill(3)
    df["township_id_fips"] = df["township_id_fips"].str.zfill(3)

    counties_in_msas = msa_county_crosswalk.county_id_fips.unique()
    df = df[~df.county_id_fips.isin(counties_in_msas)]

    # there are some New England counties where the county is split
    # between two different non-MSAs. However the QCEW data isn't finer
    # granularity than county, so for now, drop duplicate counties
    # to get one non-MSA per county
    # TODO: keep the non-MSA that the majority of the county is part of
    df = df.drop_duplicates(subset="county_id_fips")

    return df


def transform_msa_county_crosswalk(df: pd.DataFrame) -> pd.DataFrame:
    """Transform MSA to county crosswalk so it can connected to QCEW data."""
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df = df.rename(
        columns={
            "county_code": "county_id_fips",
            "msa_title": "msa_name",
            "county_title": "county_name",
        }
    )
    df = df.astype(str)
    df = df[df.msa_type == "Metro"]
    df["county_id_fips"] = df.county_id_fips.str.zfill(5)
    return df


def transform_qcew_data(
    df: pd.DataFrame,
    msa_county_crosswalk: pd.DataFrame,
    non_msa_county_crosswalk: pd.DataFrame,
    fossil_naics_codes: list[str] = FOSSIL_NAICS_CODES,
) -> pd.DataFrame:
    """Transform the QCEW data.

    This function takes the QCEW data as input, cleans it, filters for industry
    codes within the fossil fuel industry or representing a total sum of employees,
    and then separate it into two dataframes.

    The first returned dataframe contains a record
    for each MSA. The second returned dataframe contains a record for each
    county within a nonmetropolitan statistical area.

    Args:
        df: Dataframe of raw QCEW data containing records for MSAs as well
            as records for counties.
        msa_county_crosswalk: A dataframe of all counties within MSAs.
        non_msa_county_crosswalk: A dataframe of all counties within nonmetropolitan
            statistical areas.
        fossil_naics_codes: A list of fossil NAICS codes to include records from. Used
            for comparing results from different groupings of NAICS codes. Default is
            FOSSIL_NAICS_CODES.

    Returns:
        full_df: Dataframe of the transformed QCEW data containing
            a record for the counties within MSAs and non-MSAs giving the
            employment statistics for each NAICS code and ownership code.
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df = df.astype(
        {
            "area_fips": "string",
            "area_title": "string",
            "industry_code": "string",
            "own_code": "Int64",
        }
    )
    df["area_fips"] = df["area_fips"].str.zfill(5)
    # filter for records representing totals or fossil fuel industry records
    df = df[
        df["industry_code"].isin(["10"] + fossil_naics_codes)
        & (df["annual_avg_emplvl"] != 0)
    ]

    # get just the county records
    df = df[~(df["area_title"].str.contains("MSA|CSA", regex=True))]
    df = df.rename(columns={"area_fips": "county_id_fips"})
    # merge on MSA information
    msa_df = df.merge(
        msa_county_crosswalk,
        how="inner",
        on="county_id_fips",
    )
    # merge on non MSA information
    non_msa_df = df.merge(
        non_msa_county_crosswalk,
        how="inner",
        on="county_id_fips",
    )
    full_df = pd.concat([msa_df, non_msa_df])
    county_len_diff = len(df.county_id_fips.unique()) - len(
        full_df.county_id_fips.unique()
    )
    if county_len_diff > 10:
        logger.warning(
            f"There are {county_len_diff} counties in the QCEW data that are not part of the MSA or non-MSA crosswalk."
        )
    # rename to standardize columns
    full_df = full_df.drop(columns=["area_title"]).rename(
        columns={"msa_name": "area_title"}
    )
    full_df = full_df[
        [
            "msa_code",
            "county_id_fips",
            "area_title",
            "year",
            "industry_code",
            "own_code",
            "annual_avg_emplvl",
        ]
    ]

    return full_df
