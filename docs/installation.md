# Installation


## Installation from GitHub

To obtain the latest development version, clone [this repository](https://github.com/CWorthy-ocean/C-Star):


```
git clone https://github.com/CWorthy-ocean/C-Star.git
cd C-Star
``` 

Next, install and activate the following conda environment:

```
conda env create -f ci/environment.yml
conda activate cstar_env
```

Finally, install `C-Star` in the same environment:
```
pip install -e .
``` 

