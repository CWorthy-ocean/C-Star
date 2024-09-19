import yaml


def test_mock_input_fixture(mock_user_input):
    # Mocked input behavior
    with mock_user_input("yes"):
        assert input("Enter your choice: ") == "yes"


class TestGetBluePrints:
    def test_get_blueprint(self, test_blueprint_as_dict):
        blueprint_dict = test_blueprint_as_dict("ROMS_MARBL", local=False)

        assert isinstance(blueprint_dict, dict)

        assert "components" in blueprint_dict

        # fetch an input dataset location from the deeply-nested dict
        roms_component = blueprint_dict["components"][1]["component"]
        input_datasets = roms_component["input_datasets"]
        model_grid = input_datasets["model_grid"]
        location = model_grid["files"][0]["location"]

        assert (
            location
            == "https://github.com/CWorthy-ocean/input_datasets_roms_marbl_example/raw/main/roms_grd.nc"
        )

    def test_dict_equivalent_to_path(
        self, test_blueprint_as_dict, test_blueprint_as_path
    ):
        blueprint_dict = test_blueprint_as_dict("ROMS_MARBL")
        blueprint_filepath = test_blueprint_as_path("ROMS_MARBL")

        with open(blueprint_filepath, "r") as file:
            blueprint_dict_from_file = yaml.safe_load(file)

        # python's assert should automatically check lists/dicts etc. recursively
        assert blueprint_dict_from_file == blueprint_dict
