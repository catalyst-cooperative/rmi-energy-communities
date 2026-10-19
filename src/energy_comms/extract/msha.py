"""Extract MSHA mines data and load into pandas dataframe."""

import io
import logging
import zipfile

import pandas as pd
import requests
from requests.models import HTTPError

import energy_comms

logger = logging.getLogger(__name__)

SOURCE_URL = "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip"
METADATA_URL = (
    "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines_Definition_File.txt"
)


def get_metadata(filename: str = "metadata.txt") -> None:
    """Download and save mines metadata from MSHA site.

    Args:
        filename: Name for the downloaded metadata file.
    """
    m = requests.get(METADATA_URL, timeout=10)
    if m.status_code != 200:
        raise HTTPError(
            f"Bad response from Mine Data Retrieval System metadata. Status code: {m.status_code}"
        )
    msha_input_dir = energy_comms.DATA_INPUTS / "msha"
    msha_input_dir.mkdir(parents=True, exist_ok=True)
    with open(msha_input_dir / filename, "w") as f:
        logger.info(f"Writing metadata to {msha_input_dir}")
        f.write(m.text)


def extract() -> pd.DataFrame:
    """Download mines zip file from MSHA site."""
    logger.info("Retrieving MSHA data.")

    r = requests.get(SOURCE_URL, timeout=10)
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
