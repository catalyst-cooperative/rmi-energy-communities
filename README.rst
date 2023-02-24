
.. readme-intro

.. image:: https://github.com/catalyst-cooperative/rmi-energy-communities/workflows/tox-pytest/badge.svg
   :target: https://github.com/catalyst-cooperative/rmi-energy-communities/actions?query=workflow%3Atox-pytest
   :alt: Tox-PyTest Status

.. image:: https://img.shields.io/codecov/c/github/catalyst-cooperative/rmi-energy-communities?style=flat&logo=codecov
   :target: https://codecov.io/gh/catalyst-cooperative/rmi-energy-communities
   :alt: Codecov Test Coverage

.. image:: https://img.shields.io/pypi/pyversions/catalystcoop.cheshire?style=flat&logo=python
   :target: https://pypi.org/project/catalystcoop.cheshire/
   :alt: Supported Python Versions

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black>
   :alt: Any color you want, so long as it's black.

This repository is a collaboration between RMI and Catalyst Cooperative
to identify energy communities as defined by the Inflation Reduction Act.

Installation
============
To install the software in this repository, clone it to your computer using git.
If you're authenticating using SSH:

.. code:: bash

   git clone git@github.com:catalyst-cooperative/rmi-energy-communities.git

Or if you're authenticating via HTTPS:

.. code:: bash

   git clone https://github.com/catalyst-cooperative/rmi-energy-communities.git

Then in the top level directory of the repository, create a ``conda`` environment
based on the ``environment.yml`` file that is stored in the repo:

.. code:: bash

   conda env create --name energy_comms --file environment.yml

Note that the software in this repository depends on
`the dev branch <https://github.com/catalyst-cooperative/pudl/tree/dev>`__ of the
`main PUDL repository <https://github.com/catalyst-cooperative/pudl>`__,
and the ``setup.py`` in this repository indicates that it should be installed
directly from GitHub. This can be a bit slow, as ``pip`` (which in this case is
running inside of a ``conda`` environment) clones the entire history of the
repository containing the package being installed. How long it takes will depend on
the speed of your network connection. It might take ~5 minutes.

To generate the outputs in this repo you will need a pre-processed data archive for
PUDL to access. When you extract that archive you will need to tell PUDL where to
find it by putting the path to
that data in a file called ``.pudl.yml`` in your home directory.
For more instructions on setting up PUDL with the necessary data and settings
to generate the outputs in this repo, you can
`read the PUDL docs <https://catalystcoop-pudl.readthedocs.io/en/latest/dev/dev_setup.html>`__.

Generating Outputs
==================

To generate output dataframes of the qualifying energy community areas you can run
the following commands from your command line once the ``energy_comms`` package is
installed. There will be the county FIPS and name for every record, so for an
MSA that qualifies under the employment criteria, there will be a record for each
county in that MSA. If a Census tract qualifies under the coal criteria, the
``county_id_fips`` and ``county_name`` column will contains the FIPS and name for
the county that the tract is contained in. For more information about what these
qualifying criteria mean, see the below section on IRA criteria.

**Command Line Arguments**:

- ``--coal_area``: 
- ``--brownfields_area``:
- ``--update_employment_data``:
- ``--output_filepath``:

Note that for the coal closure and brownfields criteria you can specify the
census geometry area that qualifies for the criteria e.g. tract, county, state.

To save a pickled dataframe of all areas that qualify as an energy community and the
criteria they qualify by, you can run:

.. code:: bash

   get_all_qualifying_areas --coal_area tract --brownfields_area tract  --output_filepath all_qualifying_areas.pkl

To get just the states, counties, or census tracts that qualify under the coal
closures community criteria run:

.. code:: bash

   get_coal_qualifying_areas --coal_area tract --output_filepath coal_qualifying_areas.pkl

To get the states, counties, or census tract that qualify under the
brownfields criteria run:

.. code:: bash

   get_brownfields_qualifying_areas --brownfields_area tract

To get the metropolitan statistical areas and non-metropolitan statistical
areas that qualify under the employment criteria run:

.. code:: bash

   get_employment_qualifying_areas --update_employment_data True


Inflation Reduction Act Energy Communities Criteria
===================================================

The IRA defines an energy community as an area that qualifies by at least
one of the following outlined criteria:

1. A Brownfield Site
2. A metropolitan statistical area or non-metropolitan statistical area which
   meets both of these requirements:
   - has at any time after Dec. 31 2009, had .17% or greater direct employment
   or 25% or greater local tax revenues related to the fossil fuel industry
   (extraction, processing, transport, storage of coal, oil, natural gas)
   - has an unemployment rate at or above the national average unemployment rate
   for the previous year.
3. A census tract in which after Dec 31, 1999 a coal mine has closed, or after
   after Dec 31, 2009 a coal-fired electric generating unit has been retired. Or
   a census tract that is directly adjoining an aforementioned census tract.


Development
===========

To run the pre-commit hooks before you commit code run:

.. code:: bash

   pre-commit install

Thank You
=========

A big thank you to Resources for the Future for generously sharing with
us their own work and insight on identifing energy communities. You can view their
report on IRA energy communities
`here <https://www.resources.org/common-resources/what-is-an-energy-community/>`__.
