"""PyTest configuration module. Defines useful fixtures, command line args."""
import logging
import os
from pathlib import Path
from typing import Any

import pytest

import pudl

logger = logging.getLogger(__name__)


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add package-specific command line options to pytest.

    This is slightly magical -- pytest has a hook that will run this function
    automatically, adding any options defined here to the internal pytest options that
    already exist.
    """
    parser.addoption(
        "--sandbox",
        action="store_true",
        default=False,
        help="Flag to indicate that the tests should use a sandbox.",
    )


@pytest.fixture(scope="session")
def pudl_settings_fixture() -> Any:  # noqa: C901
    """Determine some settings for the test session.

    * On a user machine, it should use their existing PUDL_DIR.
    * In CI, it should use PUDL_DIR=$HOME/pudl-work containing the
      downloaded PUDL DB.
    """
    logger.info("setting up the pudl_settings_fixture")

    # In CI we want a hard-coded path for input caching purposes:
    if os.environ.get("GITHUB_ACTIONS", False):
        pudl_out = Path(os.environ["HOME"]) / "pudl-work"
        pudl_in = pudl_out
    # Otherwise, default to the user's existing datastore:
    else:
        try:
            defaults = pudl.workspace.setup.get_defaults()
        except FileNotFoundError as err:
            logger.critical("Could not identify PUDL_IN / PUDL_OUT.")
            raise err
        pudl_out = defaults["pudl_out"]
        pudl_in = defaults["pudl_in"]

    # Set these environment variables for future reference...
    logger.info("Using PUDL_IN=%s", pudl_in)
    os.environ["PUDL_IN"] = str(pudl_in)
    logger.info("Using PUDL_OUT=%s", pudl_out)
    os.environ["PUDL_OUT"] = str(pudl_out)

    # Build all the pudl_settings paths:
    pudl_settings = pudl.workspace.setup.derive_paths(
        pudl_in=pudl_in, pudl_out=pudl_out
    )
    # Set up the pudl workspace:
    pudl.workspace.setup.init(pudl_in=pudl_in, pudl_out=pudl_out)

    logger.info("pudl_settings being used: %s", pudl_settings)
    return pudl_settings


@pytest.fixture(scope="session")
def test_dir() -> Path:
    """Return the path to the top-level directory containing the tests."""
    return Path(__file__).parent
