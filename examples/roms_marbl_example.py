import os
import cstar_ocean as cstar


roms_marbl_case = cstar.Case.from_blueprint(
    blueprint="cstar_blueprint_roms_marbl_example.yaml",
    caseroot="roms_marbl_example_case/",
)
