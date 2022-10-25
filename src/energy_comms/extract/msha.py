"""Extract MSHA mines data and load into pandas dataframe."""
import io
import pathlib
import zipfile

import pandas as pd
import requests
from requests.models import HTTPError

import energy_comms

SOURCE_URL = "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip"


def get_page() -> None:
    """Download mines zip file from MSHA site."""
    # TODO: get metadata file as well
    r = requests.get(SOURCE_URL)
    if r.status_code != 200:
        raise HTTPError(
            f"Bad response from Mine Data Retrieval System. Status code: {r.status_code}"
        )
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(energy_comms.INPUTS_DIR / "msha")


def extract(
    txt_file_path: pathlib.Path = energy_comms.INPUTS_DIR / "msha/Mines.txt",
) -> pd.DataFrame:
    """Extract MSHA mines data from downloaded txt file."""
    # use different way to handle errors?
    out = pd.read_csv(txt_file_path, sep="|", header=0, encoding_errors="replace")
    return out
