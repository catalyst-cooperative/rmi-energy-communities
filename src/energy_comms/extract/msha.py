"""Extract MSHA mines data and load into pandas dataframe."""
import io
import pathlib
import zipfile

import pandas as pd
import requests
from requests.models import HTTPError

import energy_comms

SOURCE_URL = "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip"
METADATA_URL = (
    "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines_Definition_File.txt"
)


def get_page() -> None:
    """Download mines zip file and metadata from MSHA site."""
    # maybe move this get page to an integration test
    # read in direclty from url in extract function
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
        if info[0].filename == "Mines.txt":
            raise AssertionError(f"Filename is {info[0].filename}, expected Mines.txt")
        z.extractall(energy_comms.INPUTS_DIR / "msha")

    m = requests.get(METADATA_URL)
    if m.status_code != 200:
        raise HTTPError(
            f"Bad response from Mine Data Retrieval System metadata. Status code: {m.status_code}"
        )
    with open(energy_comms.INPUTS_DIR / "msha/metadata.txt", "w") as f:
        f.write(m.text)


def extract(
    txt_file_path: pathlib.Path = energy_comms.INPUTS_DIR / "msha/Mines.txt",
    fresh_download: bool = True,
) -> pd.DataFrame:
    """Extract MSHA mines data from downloaded txt file."""
    if fresh_download or not pathlib.Path(txt_file_path).is_file():
        get_page()
    out = pd.read_csv(txt_file_path, sep="|", encoding="latin_1")
    return out
