import pandas as pd


def transform(df):

    # from HUD: https://www.huduser.gov/portal/datasets/usps_crosswalk.html

    # make lower case and replace spaces
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # assign dtypes
    df = df.astype(
        {"site_name": str, "state": str, "latitude": float, "longitude": float}
    )

    # only keep columns we want

    df = df[["site_name", "zip_code", "latitude", "longitude", "state"]]
    # read in zip code cross walk from HUD

    zip_code_crosswalk = pd.read_excel(
        "https://www.huduser.gov/portal/datasets/usps/ZIP_COUNTY_122021.xlsx",
        dtype={"zip": str, "county": str},
    )

    zip_crosswalk = dict(zip(zip_code_crosswalk["zip"], zip_code_crosswalk["county"]))

    # fill in criteria for patio and map crosswalk

    df = df.assign(
        fips_county=df.zip.map(zip_crosswalk),
        qualifying_area="site",
        criteria="brownfield",
    )

    # not needed for final output

    df = df.drop(columns=["zip", "state"])

    # needed for eventual merge with other criteria
    df = df.rename(columns={"site_name": "area_title"})

    return df
