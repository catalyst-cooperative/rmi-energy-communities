"""Transform functions for Bureau of Labor Statistics data for employment criteria."""
import logging

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
    raw_lau_df: pd.DataFrame, area_df: pd.DataFrame, non_msa_df: pd.DataFrame
) -> pd.DataFrame:
    """Get the annual average local unemployment rate and area information.

    Clean the LAU data, filter area data down to counties and metropolitan
    statistical areas, calculate annual average rate, merge on area information,
    aggregate county LAU data within nonmetropolitan statistical areas.

    Args:
        raw_lau_df: The raw local area unemployment data.
        area_df: The transformed local area unemployment area information.
        non_msa_df: Dataframe of non-MSA codes and names.

    Returns:
        lau_msa_df: Dataframe giving the annual average unemployment rate for
            metropolitan statistical areas
        lau_non_msa_df: Dataframe giving the annual average unemployment rate
            for counties within nonmetropolitan statistical areas.
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
    # filter for just MSAs and counties
    lau_df = lau_df[lau_df.series_id.str[3:5].isin(["MT", "CN"])]
    # get the unemployment rate, unemployment total, and labor force total stats
    lau_df = lau_df[lau_df.series_id.str[-2:].isin(["03", "04", "06"])]
    lau_df = lau_df.dropna(subset=["value"])
    # take an annual average, didn't use M13 here because it is null
    # (footnote code U) when any monthly value is missing (footnote code N)
    # note the rounding bc BLS website specifies 1 sig figure
    lau_df = (
        lau_df.groupby(by=["series_id", "year"])["value"].mean().round(1).reset_index()
    )
    # get just the MSA records
    lau_msa_df = lau_df[lau_df.series_id.str[3:5] == "MT"]
    # filter for unemployent rate statistics (last two digits is measure_code)
    lau_msa_df = lau_msa_df[lau_msa_df.series_id.str[-2:] == "03"]
    lau_msa_df = lau_msa_df.rename(columns={"value": "local_area_unemployment_rate"})
    # merge on area information
    msa_area_df = area_df.assign(series_id=lambda x: x.series_id + "03")
    lau_msa_df = lau_msa_df.merge(msa_area_df, on="series_id", how="left")
    lau_msa_df = lau_msa_df.drop(columns=["county_id_fips"])
    lau_msa_df = lau_msa_df.rename(columns={"area_text": "msa_name"})

    # now handle nonMSAs, first get county records
    lau_non_msa_df = lau_df[lau_df.series_id.str[3:5] == "CN"]
    # get the records that have a series_id ending in 06 (the labor force stats)
    labor_force = lau_non_msa_df[lau_non_msa_df.series_id.str[-2:] == "06"][
        ["series_id", "year", "value"]
    ]
    labor_force = labor_force.rename(columns={"value": "total_labor_force"})
    # remake series_id to be mergeable with the unemployment numbers series ID
    labor_force["series_id"] = labor_force["series_id"].str[:-2] + "04"
    lau_non_msa_df = lau_non_msa_df[lau_non_msa_df.series_id.str[-2:] == "04"]
    lau_non_msa_df = lau_non_msa_df.rename(columns={"value": "total_unemployment"})
    lau_non_msa_df = lau_non_msa_df.merge(
        labor_force, how="left", on=["series_id", "year"]
    )
    # merge on area information
    non_msa_area_df = area_df.assign(series_id=lambda x: x.series_id + "04")
    lau_non_msa_df = lau_non_msa_df.merge(non_msa_area_df, on="series_id", how="left")
    lau_non_msa_df = lau_non_msa_df.drop(columns=["msa_code"])
    # merge on nonMSA information for counties within nonMSAs
    lau_non_msa_df = lau_non_msa_df.merge(non_msa_df, how="inner", on="county_id_fips")

    return lau_msa_df, lau_non_msa_df


def transform_nonmsa_area_defs(
    df: pd.DataFrame, msa_county_crosswalk: pd.DataFrame
) -> pd.DataFrame:
    """Transform dataframe of non-MSA codes and names.

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
    df = df.rename(columns={"county_code": "county_id_fips"})
    df = df.astype(str)
    df = df[df.msa_type == "Metro"]
    df["county_id_fips"] = df.county_id_fips.str.zfill(5)
    return df


def transform_qcew_data(df: pd.DataFrame, non_msa_df: pd.DataFrame) -> pd.DataFrame:
    """Transform the QCEW data.

    This function takes the QCeW data as input, cleans it, filters for industry
    codes within the fossil fuel industry or representing a total sum of employees,
    and then separate it into two dataframes.

    The first returned dataframe contains a record
    for each MSA. The second returned dataframe contains a record for each
    county within a nonmetropolitan statistical area.

    Args:
        df: Dataframe of raw QCEW data containing records for MSAs as well
            as records for counties.
        non_msa_df: A dataframe of all counties within nonmetropolitan
            statistical areas.

    Returns:
        qcew_msa_df: QCEW data with a record for each metropolitan statistical
            area.
        qcew_non_msa_df: QCEW data with a record for each county within
            a nonmetropolitan statistical area.
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
    df = df[df.industry_code.isin(["10"] + FOSSIL_NAICS_CODES)]

    # get the MSA records
    qcew_msa_df = df[df["area_title"].str.contains("MSA")]
    qcew_msa_df = qcew_msa_df.rename(columns={"area_fips": "msa_code"})
    msa_cols = [
        "msa_code",
        "area_title",
        "year",
        "industry_code",
        "own_code",
        "annual_avg_emplvl",
    ]
    qcew_msa_df = qcew_msa_df[msa_cols]

    # get the county records
    qcew_non_msa_df = df[~(df["area_title"].str.contains("MSA"))]
    # merge on non MSA information
    qcew_non_msa_df = qcew_non_msa_df.merge(
        non_msa_df, how="inner", left_on="area_fips", right_on="county_id_fips"
    )
    # rename to standardize columns
    qcew_non_msa_df = qcew_non_msa_df.rename(
        columns={"area_title": "county_title", "msa_name": "area_title"}
    )

    non_msa_cols = msa_cols + ["county_id_fips"]
    qcew_non_msa_df = qcew_non_msa_df[non_msa_cols]

    return qcew_msa_df, qcew_non_msa_df
