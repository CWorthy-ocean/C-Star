"""
Tests for DomainCatalog with a live GitHub-hosted catalog.

These tests require network access and are skipped in offline/CI environments
unless explicitly opted in.  Run with::

    pytest -m network

or exclude them from a normal run with::

    pytest -m "not network"

NOTE: These tests are currently commented out pending GitHub token setup.
"""

# import pytest
# import pandas as pd
#
# from cstar_forge.domain_catalog import DomainCatalog
#
# GITHUB_CATALOG_URL = "https://github.com/CWorthy-ocean/CWorthy-Demo/catalog"
#
# pytestmark = pytest.mark.network
#
#
# @pytest.fixture(scope="module")
# def github_catalog():
#     """DomainCatalog backed by the live CWorthy-Demo GitHub repo."""
#     return DomainCatalog(catalog_root=GITHUB_CATALOG_URL)
#
#
# @pytest.fixture(scope="module")
# def roms_marbl_blueprint_df(github_catalog):
#     """Cached roms_marbl_blueprint_df(stage=None) — avoids repeated GitHub fetches."""
#     return github_catalog.roms_marbl_blueprint_df(stage=None)
#
#
# class TestGitHubCatalogInit:
#     def test_is_not_local(self, github_catalog):
#         assert not github_catalog._is_local
#
#     def test_catalog_root_is_path(self, github_catalog):
#         from pathlib import Path
#         assert isinstance(github_catalog.catalog_root, Path)
#
#     def test_has_machines(self, github_catalog):
#         assert len(github_catalog.machine_names) > 0
#
#     def test_has_models(self, github_catalog):
#         assert len(github_catalog.model_names) > 0
#
#     def test_has_blueprints(self, github_catalog):
#         assert len(github_catalog.roms_marbl_blueprint_names) > 0
#
#
# class TestGitHubCatalogBlueprintDF:
#     def test_returns_dataframe(self, roms_marbl_blueprint_df):
#         assert isinstance(roms_marbl_blueprint_df, pd.DataFrame)
#
#     def test_not_empty(self, roms_marbl_blueprint_df):
#         assert not roms_marbl_blueprint_df.empty
#
#     def test_expected_columns(self, roms_marbl_blueprint_df):
#         for col in ("model_name", "grid_name", "blueprint_name", "stage",
#                     "blueprint_path", "grid_yaml_path"):
#             assert col in roms_marbl_blueprint_df.columns, f"Missing column: {col}"
#
#     def test_model_name_populated(self, roms_marbl_blueprint_df):
#         assert roms_marbl_blueprint_df["model_name"].notna().all()
#
#     def test_grid_name_populated(self, roms_marbl_blueprint_df):
#         assert roms_marbl_blueprint_df["grid_name"].notna().all()
#
#     def test_grid_yaml_is_url_when_present(self, roms_marbl_blueprint_df):
#         non_null = roms_marbl_blueprint_df["grid_yaml_path"].dropna()
#         for val in non_null:
#             assert str(val).startswith("https://"), (
#                 f"grid_yaml_path should be an https URL for a GitHub catalog, got: {val!r}"
#             )
