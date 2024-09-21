TEST_BLUEPRINTS = {
    "ROMS_MARBL": {
        # TODO this assumes you are running pytest from the root directory of the cloned repo, which is fragile
        "base": "./cstar/tests/blueprints/roms_marbl_example_template.yaml",
        "input_datasets_location": "https://github.com/CWorthy-ocean/input_datasets_roms_marbl_example/raw/main",
        "additional_code_location": "https://github.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example.git",
    },
}
