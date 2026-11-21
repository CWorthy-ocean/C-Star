``cstar blueprint check`` now reports a forge blueprint that ``cstar forge run`` would refuse, such as one with a misspelled ``model_settings`` key or a CDR window that ends before the run, instead of saying it is valid.
- ``cstar forge run`` prints that refusal as an error instead of a Python traceback.
