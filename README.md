[![codecov](https://codecov.io/gh/CWorthy-ocean/C-Star/graph/badge.svg?token=HAPZGL2LWF)](https://codecov.io/gh/CWorthy-ocean/C-Star)
[![Documentation Status](https://readthedocs.org/projects/c-star/badge/?version=latest)](https://c-star.readthedocs.io/en/latest/?badge=latest)

<mark>**DISCLAIMER**: 
* This project is **ALPHA** and will be **EXPERIMENTAL** at least throughout 2024. We do not recommend to use this software before this ALPHA development and test phase is completed.
* Examples in the [documentation](https://c-star.readthedocs.io/en/latest/index.html) currently only work on the Perlmutter machine.
</mark>

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

## License

C-Star is openly available for use and permissively licenced under Apache 2.0. 

   Copyright 2024 [C]Worthy LLC.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
