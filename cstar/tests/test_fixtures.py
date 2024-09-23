import yaml


def test_mock_input_fixture(mock_user_input):
    # Mocked input behavior
    with mock_user_input("yes"):
        assert input("Enter your choice: ") == "yes"


class TestGetBluePrints:
    def test_get_blueprint(self, blueprint_as_dict):
        blueprint_dict = blueprint_as_dict(
            "cstar_test_with_netcdf_datasets", use_local_sources=False
        )

        assert isinstance(blueprint_dict, dict)

        assert "components" in blueprint_dict

        # fetch an input dataset location from the deeply-nested dict
        roms_component = blueprint_dict["components"][1]["component"]
        input_datasets = roms_component["input_datasets"]
        model_grid = input_datasets["model_grid"]
        input_dataset_location = model_grid["files"][0]["location"]
        assert (
            input_dataset_location
            == "https://github.com/CWorthy-ocean/cstar_blueprint_test_case/raw/main/input_datasets/ROMS/roms_grd.nc"
        )

        additional_code_location = roms_component["additional_code"]["location"]
        assert (
            additional_code_location
            == "https://github.com/CWorthy-ocean/cstar_blueprint_test_case.git"
        )

    def test_dict_equivalent_to_path(self, blueprint_as_dict, blueprint_as_path):
        blueprint_dict = blueprint_as_dict("cstar_test_with_netcdf_datasets")
        blueprint_filepath = blueprint_as_path("cstar_test_with_netcdf_datasets")

        with open(blueprint_filepath, "r") as file:
            blueprint_dict_from_file = yaml.safe_load(file)

        # python's assert should automatically check lists/dicts etc. recursively
        assert blueprint_dict_from_file == blueprint_dict
