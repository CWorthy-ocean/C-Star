```{include} disclaimer.md
```

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

