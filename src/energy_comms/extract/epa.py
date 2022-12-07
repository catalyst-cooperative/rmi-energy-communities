"""Extract brownfields sites from EPA source."""
import logging

import pandas as pd

logger = logging.getLogger(__name__)

# from link
# https://www.epa.gov/re-powering/how-identify-sites#looking
SOURCE_URL = "https://www.epa.gov/system/files/documents/2022-04/re-powering-screening-dataset-2022.xlsx"


def extract() -> pd.DataFrame:
    """Download brownfields site data from EPA source."""
    logger.info("Extracting EPA brownfields data.")
    # download area titles and convert to pandas dataframe
    sites_sheet_name = "re-powering sites"
    sheet_idx = None

    # read in sheet
    xl = pd.ExcelFile(SOURCE_URL)

    # clean up sheet to lower and remove trailing white space (if present)
    # of sheet names to identify correct one
    for i, name in enumerate([sn.strip().lower() for sn in xl.sheet_names]):
        if name == sites_sheet_name:
            sheet_idx = i

    # with cleaned list of sheet names, pick the one you want (re-powering sites)
    if sheet_idx is not None:
        df = pd.read_excel(
            SOURCE_URL, sheet_name=sheet_idx, dtype={"Zip Code": "string"}
        )
    else:
        raise AssertionError(
            f"The {sites_sheet_name} sheet is not present in the EPA spreadsheet."
        )

    return df
