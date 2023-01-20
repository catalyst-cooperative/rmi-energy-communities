"""Test the PUDL console scripts from within PyTest."""

import pkg_resources
import pytest

# Obtain a list of all deployed entry point scripts to test:
ENTRY_POINTS = [
    ep.name
    for ep in pkg_resources.iter_entry_points("console_scripts")
    if ep.module_name.startswith("energy_comms")
]


@pytest.mark.parametrize("ep", ENTRY_POINTS)
@pytest.mark.script_launch_mode("inprocess")
def test_energy_comms_scripts(script_runner, ep: str) -> None:  # type: ignore
    """Run each deployed console script with --help as a basic test.

    The script_runner fixture is provided by the pytest-console-scripts plugin.
    """
    ret = script_runner.run(ep, "--help", print_result=False)
    assert ret.success  # nosec: B101


@pytest.mark.parametrize(
    "ca,ba",
    [
        pytest.param("area", "state", marks=pytest.mark.xfail),
        pytest.param("tract", "area", marks=pytest.mark.xfail),
    ],
)
@pytest.mark.script_launch_mode("inprocess")
def test_script_args(script_runner, ca: str, ba: str) -> None:  # type: ignore
    """Try running the script with bad inputs for coal and brownfield geometry."""
    ret = script_runner.run("get_all_qualifying_areas", "-ca", ca, "-ba", ba)
    assert ret.success  # nosec: B101
