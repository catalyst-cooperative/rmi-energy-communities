"""Extract brownfields sites from EPA source."""
import logging

import pandas as pd

import energy_comms

logger = logging.getLogger(__name__)

# from link
SOURCE_URL = "https://www.epa.gov/system/files/documents/2022-04/re-powering-screening-dataset-2022.xlsx"


def extract(update: bool = True) -> pd.DataFrame:
    """Download brownfields site data from EPA source.

    If update is True, get a fresh download of data and write the pickled
    dataframe to the ``energy_comms.DATA_INPUTS`` directory so it
    can be read in faster later.

    See https://www.epa.gov/re-powering/how-identify-sites#looking
    for details on data.

    Args:
        update: Whether to download fresh data from the ``SOURCE_URL``.
            Default is True. If False, data will be read in from the
            pickled dataframe in the data inputs directory.
    """
    logger.info("Extracting EPA brownfields data.")
    data_dir = energy_comms.DATA_INPUTS / "epa"
    data_dir.mkdir(parents=True, exist_ok=True)
    pickle_file_path = data_dir / (SOURCE_URL.split("/")[-1] + ".pkl.gz")

    if not (pickle_file_path.exists()) or update:
        sites_sheet_name = "re-powering sites"
        sheet_idx = None
        # read in sheet
        xl = pd.ExcelFile(SOURCE_URL)

        # clean up sheet to lower and remove trailing white space (if present)
        # of sheet names to identify correct one
        for i, name in enumerate([sn.strip().lower() for sn in xl.sheet_names]):
            if name == sites_sheet_name:
                sheet_idx = i

        # pick the sheet we want (re-powering sites)
        if sheet_idx is not None:
            df = pd.read_excel(
                SOURCE_URL, sheet_name=sheet_idx, dtype={"Zip Code": "string"}
            )
        else:
            raise AssertionError(
                f"The {sites_sheet_name} sheet is not present in the EPA spreadsheet."
            )
        # pickle dataframe so we don't need to read from excel every time
        df.to_pickle(pickle_file_path, compression="gzip")
    else:
        df = pd.read_pickle(pickle_file_path)

    return df
