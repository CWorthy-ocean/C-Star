from unittest.mock import patch
import cstar_ocean as cstar

roms_marbl_case = cstar.Case.from_blueprint(
    blueprint=(cstar.environment._CSTAR_ROOT) + "examples/cstar_blueprint_roms_marbl_example.yaml",
    caseroot="roms_marbl_example_case/",
)

# patch will automatically respond "y" to any call for input
with patch("builtins.input", return_value="y"):
    roms_marbl_case.setup()
    roms_marbl_case.persist("test_blueprint.yaml")
    roms_marbl_case.build()
    roms_marbl_case.pre_run()
    roms_marbl_case.run()
    roms_marbl_case.post_run()
