# Tests

This directory contains the test suite for cson-forge.

## Running Tests

### Run all tests
```bash
pytest
```

### Run specific test file
```bash
pytest tests/test_model.py
```

### Run with coverage
```bash
pytest --cov=workflows --cov-report=html
```

### Run specific test
```bash
pytest tests/test_model.py::TestLoadModelsYaml::test_load_from_real_yaml
```

## Test Structure

- `conftest.py`: Shared fixtures and pytest configuration
- `test_model.py`: Tests for the `model.py` module
- `data/`: Test data files (if needed)

## Continuous Integration

Tests are automatically run on:
- Push to `main` or `develop` branches
- Pull requests to `main` or `develop` branches
- Manual workflow dispatch

See `.github/workflows/tests.yml` for CI configuration.

