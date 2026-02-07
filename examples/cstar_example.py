"""
This is a condensed version of the cstar_example_notebook.ipynb designed to be run in an interactive python session.
For more details, see ../README.md or cstar_example_notebook.ipynb
"""

import cstar
import os

roms_marbl_case = cstar.Simulation.from_blueprint(
    blueprint="cstar_blueprint_yaml_test.yaml",
)

## In a python session, execute:
roms_marbl_case.setup()
roms_marbl_case.build()
roms_marbl_case.pre_run()
#roms_marbl_case.run(account_key=os.environ.get('ACCOUNT_KEY')) #substituting your account key on any HPC system
#roms_marbl_case.post_run()
