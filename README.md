![tox-pytest](https://github.com/catalyst-cooperative/rmi-energy-communities/actions/workflows/tox-pytest.yml/badge.svg)
![codecov](https://img.shields.io/codecov/c/github/catalyst-cooperative/rmi-energy-communities)
![code style](https://img.shields.io/badge/code%20style-black-000000.svg)

This repository is a collaboration between RMI and Catalyst Cooperative to identify energy communities
as defined by the Inflation Reduction Act.

## Installation

To install the software in this repository, clone it to your computer using git. If
you're authenticating using SSH:

```sh
git clone git@github.com:catalyst-cooperative/rmi-energy-communities.git
```

Or if you're authenticating via HTTPS:

```sh
git clone https://github.com/catalyst-cooperative/rmi-energy-communities.git
```

Then in the top level directory of the repository, create a `conda` environment based on
the `environment.yml` file that is stored in the repo:

```sh
conda env create --file environment.yml
```

After any changes to the environment specification, you'll need to recreate the conda
environment. The most reliable way to do that is to remove the old environment and
create it from scratch. If you're in the top level `rmi-ferc1-eia` directory and have
the `energy_comms` environment activated, that process would look like this:

```sh
conda deactivate
conda env remove --name energy_comms
conda env create --file environment.yml
conda activate energy_comms
```

**Sources**

A big thank you to Resources for the Future for generously sharing with us their own work and insight on
identifing energy communities.
