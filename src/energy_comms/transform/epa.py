import pandas as pd


def transform(df):

    # from HUD: https://www.huduser.gov/portal/datasets/usps_crosswalk.html

    zip_code_crosswalk = pd.read_excel(
        "/Users/mcastillo/Documents/GitHub/rmi-energy-communities/src/energy_comms/extract/ZIP_COUNTY_122021.xlsx",
        dtype={"zip": str, "county": str},
    )

    zip_crosswalk = dict(zip(zip_code_crosswalk["zip"], zip_code_crosswalk["county"]))

    df = df.rename(columns={"Site Name": "site_name", "Zip Code": "zip"})

    df.columns = df.columns.str.lower()

    df["qualifying_area"] = "site"

    df["criteria"] = "brownfield"

    df["fips_county"] = df["zip"].map(zip_crosswalk)

    df = df.drop(columns="zip")

    return df
