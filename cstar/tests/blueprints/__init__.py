# TODO 'base' locations assume you are running pytest from the root directory of the cloned repo, which is fragile

BLUEPRINTS_FOR_TESTING = {
    "ROMS_MARBL": {
        "base": "./cstar/tests/blueprints/roms_marbl_example_template.yaml",
        "input_datasets_location": "https://github.com/CWorthy-ocean/input_datasets_roms_marbl_example/raw/main",
        "additional_code_location": "https://github.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example.git",
    },
    # Blueprint to test roms-tools integration: input_datasets entries point to yaml files
    "cstar_test_with_yaml_datasets": {
        "base": "./cstar/tests/blueprints/cstar_blueprint_with_yaml_datasets_template.yaml",
        "input_datasets_location": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case/raw/main/roms_tools_yaml_files",
        "additional_code_location": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case.git",
    },
    # Blueprint to test existing datasets: input_datasets entries point to netcdf files
    "cstar_test_with_netcdf_datasets": {
        "base": "./cstar/tests/blueprints/cstar_blueprint_with_netcdf_datasets_template.yaml",
        "input_datasets_location": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case/raw/main/input_datasets/ROMS",
        "additional_code_location": "https://github.com/CWorthy-ocean/cstar_blueprint_test_case.git",
    },
}
