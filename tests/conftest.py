"""PyTest configuration module. Defines useful fixtures, command line args."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa

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
def pudl_env(pudl_input_output_dirs: dict[Any, Any]) -> None:
    """Set PUDL_OUTPUT/PUDL_INPUT/DAGSTER_HOME environment variables."""
    pudl.workspace.setup.get_defaults(**pudl_input_output_dirs)

    logger.info(f"PUDL_OUTPUT path: {os.environ['PUDL_OUTPUT']}")
    logger.info(f"PUDL_INPUT path: {os.environ['PUDL_INPUT']}")


@pytest.fixture(scope="session")
def pudl_input_output_dirs(pudl_output_tmpdir) -> dict[Any, Any]:  # type: ignore
    """Determine where the PUDL input/output dirs should be."""
    input_override = None
    output_override = None

    if os.environ.get("GITHUB_ACTIONS", False):
        # hard-code input dir for CI caching
        input_override = Path(os.environ["HOME"]) / "pudl-work"
        output_override = pudl_output_tmpdir

    return {"input_dir": input_override, "output_dir": output_override}


@pytest.fixture(scope="session")
def pudl_tmpdir(tmp_path_factory):  # type: ignore
    """Base temporary directory for all other tmp dirs."""
    tmpdir = tmp_path_factory.mktemp("pudl")
    return tmpdir


@pytest.fixture(scope="session")
def pudl_output_tmpdir(pudl_tmpdir):  # type: ignore
    """Temporary directory for PUDL outputs."""
    tmpdir = pudl_tmpdir / "output"
    tmpdir.mkdir()
    return tmpdir


@pytest.fixture(scope="session", name="pudl_settings_fixture")
def pudl_settings_dict(request, pudl_input_output_dirs):  # type: ignore
    """Determine some settings (mostly paths) for the test session."""
    logger.info("setting up the pudl_settings_fixture")
    pudl_settings = pudl.workspace.setup.get_defaults(**pudl_input_output_dirs)
    pudl.workspace.setup.init(pudl_settings)

    pudl_settings["sandbox"] = request.config.getoption("--sandbox")

    pretty_settings = json.dumps(
        {str(k): str(v) for k, v in pudl_settings.items()}, indent=2
    )
    logger.info(f"pudl_settings being used: {pretty_settings}")
    return pudl_settings


@pytest.fixture(scope="session")
def pudl_engine_fixture(pudl_settings_fixture: dict[Any, Any]) -> sa.engine.Engine:
    """Grab a connection to the PUDL Database.

    If we are using the test database, we initialize the PUDL DB from scratch.
    If we're using the live database, then we just make a conneciton to it.
    """
    logger.info("setting up the pudl_engine fixture")
    engine = sa.create_engine(pudl_settings_fixture["pudl_db"])
    logger.info("PUDL Engine: %s", engine)
    return engine


@pytest.fixture(scope="session")
def test_dir() -> Path:
    """Return the path to the top-level directory containing the tests."""
    return Path(__file__).parent
