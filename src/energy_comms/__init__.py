"""Identify energy communities as defined by the Inflation Reduction Act."""

import logging

import pkg_resources

# In order for the package modules to be available when you import the package,
# they need to be imported here somehow. Not sure if this is best practice though.
import energy_comms.cli
import energy_comms.dummy  # noqa: F401
import energy_comms.extract.eia860
import energy_comms.extract.msha
import energy_comms.helpers
import energy_comms.transform.eia860
import energy_comms.transform.msha  # noqa: F401

__author__ = "Catalyst Cooperative"
__contact__ = "pudl@catalyst.coop"
__maintainer__ = "Catalyst Cooperative"
__license__ = "MIT License"
__maintainer_email__ = "pudl@catalyst.coop"
__version__ = pkg_resources.get_distribution("catalystcoop.energy_comms").version
__docformat__ = "restructuredtext en"
__description__ = (
    "Identifying energy communities as defined by the Inflation Reduction Act."
)
__long_description__ = """
This repository is a collaboration between RMI and Catalyst Cooperative to
identify and map the areas identified as energy communities by the
Inflation Reduction Act. These communities are eligible for tax incentives.
"""
__projecturl__ = "https://github.com/catalyst-cooperative/energy_comms"
__downloadurl__ = "https://github.com/catalyst-cooperative/energy_comms"

# Create a root logger for use anywhere within the package.
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
