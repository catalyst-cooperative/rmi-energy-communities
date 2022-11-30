"""Modules transforming the raw data and making it ready for use.

Each module in this subpackage transforms the tabular data associated with a single data
source. This process begins with a dictionary of "raw" :class:`pandas.DataFrame` objects
produced by the corresponding data source specific routines from the
:mod:`energy_comms.extract` subpackage, and ends with a dictionary of
:class:`pandas.DataFrame` objects that are cleaned and ready to be used in mapping the
IRA energy communities criteria.
"""

from energy_comms.transform import eia860, msha  # noqa: F401
