[![codecov](https://codecov.io/gh/CWorthy-ocean/C-Star/graph/badge.svg?token=HAPZGL2LWF)](https://codecov.io/gh/CWorthy-ocean/C-Star)
[![Documentation Status](https://readthedocs.org/projects/c-star/badge/?version=latest)](https://c-star.readthedocs.io/en/latest/?badge=latest)

# Installation

## Installation from GitHub

To obtain the latest development version, clone [this repository](https://github.com/CWorthy-ocean/C-Star):

```
git clone https://github.com/CWorthy-ocean/C-Star.git
cd C-Star
``` 

Next, install **one** of the following conda environments, depending on whether you are working on a supported HPC system (environment management by Linux Environment Modules) or a generic machine like a laptop (environment managed by conda):

```
conda env create -f ci/environment_hpc.yml  # conda environment for supported HPC system
# conda env create -f ci/environment.yml  # conda environment for generic machine 
```

Activate the conda environment:
```
conda activate cstar_env
```

Finally, install `C-Star` in the same environment:
```
pip install -e .
``` 

## Run the tests

Before running the tests, you can activate the conda environment created in the previous section:
```
conda activate cstar_env
```

Check the installation of `C-Star` has worked by running the test suite
```
cd C-Star
pytest
```

# Getting Started

To learn how to use `C-Star`, check out the [documentation](https://c-star.readthedocs.io/en/latest/index.html).

# Feedback and contributions

If you find a bug, have a feature suggestion, or any other kind of feedback, please start a Discussion.

We also accept contributions in the form of Pull Requests.

