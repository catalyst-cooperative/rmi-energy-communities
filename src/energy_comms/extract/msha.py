"""Extract MSHA mines data and load into pandas dataframe."""
import io
import logging
import pathlib
import zipfile

import pandas as pd
import requests
from requests.models import HTTPError

logger = logging.getLogger(__name__)

SOURCE_URL = "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip"
METADATA_URL = (
    "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines_Definition_File.txt"
)


def get_metadata(filepath: pathlib.Path) -> None:
    """Download and save mines metadata from MSHA site."""
    m = requests.get(METADATA_URL)
    if m.status_code != 200:
        raise HTTPError(
            f"Bad response from Mine Data Retrieval System metadata. Status code: {m.status_code}"
        )
    with open(METADATA_URL, "w") as f:
        f.write(m.text)


def extract() -> pd.DataFrame:
    """Download mines zip file from MSHA site."""
    logger.info("Retrieving MSHA data.")

    r = requests.get(SOURCE_URL)
    if r.status_code != 200:
        raise HTTPError(
            f"Bad response from Mine Data Retrieval System. Status code: {r.status_code}"
        )
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        info = z.infolist()
        if len(info) != 1:
            raise ValueError(
                f"ZIP file from mines URL contains {len(info)} files, should only have 1."
            )
        if info[0].filename != "Mines.txt":
            raise AssertionError(f"Filename is {info[0].filename}, expected Mines.txt")
        return pd.read_csv(z.open(info[0]), sep="|", encoding="latin_1")
