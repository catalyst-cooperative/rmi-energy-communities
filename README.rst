
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
To install the software in this repository, clone it to your computer using git. If
you're authenticating using SSH:

.. code:: bash

   git clone git@github.com:catalyst-cooperative/rmi-energy-communities.git

Or if you're authenticating via HTTPS:

.. code:: bash

   git clone https://github.com/catalyst-cooperative/rmi-energy-communities.git

Then in the top level directory of the repository, create a `conda` environment based on
the ``environment.yml`` file that is stored in the repo:

.. code:: bash

   conda env create --name energy_comms --file environment.yml

Note that the software in this repository depends on 
`the dev branch <https://github.com/catalyst-cooperative/pudl/tree/dev>`__ of the 
`main PUDL repository <https://github.com/catalyst-cooperative/pudl>`__, 
and the ``setup.py`` in thisrepository indicates that it should be installed 
directly from GitHub. This can be a bit slow, as ``pip`` (which in this case is 
running inside of a ``conda`` environment) clones the entire history of the 
repository containing the package being installed. How long ittakes will depend on 
the speed of your network connection. It might take ~5 minutes.

Thank You
=========

A big thank you to Resources for the Future for generously sharing with
us their own work and insight on identifing energy communities. You can view their
report on IRA energy communities 
`here <https://www.resources.org/common-resources/what-is-an-energy-community/>`__.
