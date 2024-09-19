def test_mock_input_fixture(mock_user_input):
    # Mocked input behavior
    with mock_user_input("yes"):
        assert input("Enter your choice: ") == "yes"


def test_get_blueprint(test_blueprint_as_dict):
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


def test_dict_equivalent_to_path(): ...
