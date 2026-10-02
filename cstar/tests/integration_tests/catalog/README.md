# Integration-test catalog layer

A test-local catalog layer that sits on top of the bundled one: small domains,
forcing specs that point at the pinned source data, and a minimal output spec.
`${TEST_DATA}` in the YAML is substituted with the pooch cache directory by the
`test_catalog_root` fixture. ModelSpecs come from the bundled layer below.
There is no tides ForcingSpec: `SourceSpec.path` is `str | None`, but TPXO needs a
`{grid, h, u}` mapping, so an explicit-path tidal source cannot be expressed yet.
