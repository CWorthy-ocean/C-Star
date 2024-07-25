"""
This is a condensed version of the cstar_example_notebook.ipynb designed to be run in an interactive python session.
For more details, see ../README.md or cstar_example_notebook.ipynb
"""

import cstar_ocean as cstar


roms_marbl_case = cstar.Case.from_blueprint(
    blueprint="cstar_blueprint_roms_marbl_example.yaml",
    caseroot="roms_marbl_example_case/",
)

## In a python session, execute:
# roms_marbl_case.setup()
# roms_marbl_case.build()
# roms_marbl_case.pre_run()
# roms_marbl_case.run(account_key=None) #substituting your account key on any HPC system
# roms_marbl_case.post_run()
