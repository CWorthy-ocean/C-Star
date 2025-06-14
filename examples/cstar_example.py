"""Demonstrate the same behaviors as cstar_example_notebook.ipynb.

This code is designed to be run in an interactive python session.
For more details, see ../README.md or cstar_example_notebook.ipynb
"""

import cstar

roms_marbl_case = cstar.Case.from_blueprint(
    blueprint="cstar_blueprint_yaml_test.yaml",
    caseroot="roms_marbl_example_case/",
    start_date="20120103 12:00:00",
    end_date="20120103 18:00:00",
)

## In a python session, execute:
roms_marbl_case.setup()
roms_marbl_case.build()
roms_marbl_case.pre_run()

"""
To execute on an HPC, substitute your credentials as follows:
roms_marbl_case.run(account_key=os.environ.get('ACCOUNT_KEY'))

The post_run method should be executed when the simulation completes:
roms_marbl_case.post_run()
"""
