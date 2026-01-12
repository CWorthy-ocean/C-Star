# Get Started with CSON Forge

## Prerequisites

- Python 3.8 or higher
- Conda or Mamba package manager
- Git

## Setup

1. Create a fork of the repository at https://github.com/CWorthy-ocean/cson-forge 

2. In your terminal, clone your forked repository:
```bash
git clone https://github.com/<YOUR_GITHUB_USERNAME>/cson-forge.git
cd cson-forge
```

3. Create and activate the conda environment:
```bash
conda env create -f environment.yml
conda activate cson-forge
```

4. Install `C-Star`
```bash
./setup_cstar.sh
```

## Verify Installation

To verify that everything is installed correctly, run the following in a Python interactive shell:

```python
import cson_forge
import config
print(f"System detected: {config.system}")
```

## Register for data access

CSON Forge facilitates access to a collection of open datasets required to force regional oceanographic models. 
These data are documented in ROMS Tools [here](https://roms-tools.readthedocs.io/en/latest/datasets.html).

Access to most of the data is facilitated automatically. 
- [Sign up for access](https://help.marine.copernicus.eu/en/articles/4220332-how-to-sign-up-for-copernicus-marine-service) to the Copernicus Marine Service 
- [Sign up for access](https://www.tpxo.net/global) to TPXO data
