def test_mock_input_fixture(mock_user_input):
    # Mocked input behavior
    with mock_user_input("yes"):
        assert input("Enter your choice: ") == "yes"
