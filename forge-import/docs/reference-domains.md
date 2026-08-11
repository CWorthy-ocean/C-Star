# Domains (`DomainSpec/{grid}/Domain.yaml`)

Current domains live in the catalog as one directory per grid, each holding a
`Domain.yaml`. Example (`cstar_forge/catalog/DomainSpec/wio-toy/Domain.yaml`, the
toy domain used in [Getting Started](getting-started.md)):

```{include} ../cstar_forge/catalog/DomainSpec/wio-toy/Domain.yaml
:code: yaml
```

## Legacy nested-domain format

The pre-catalog notebook workflow described parent/child pairs in a single
nested file; it is kept only as historical reference. Note that its
`_child_grid_name`/`_parent_grid_name` keys are read by nothing today — the
current nesting mechanism is a separately selected child `DomainSpec` feeding
`grid_kwargs_child`/`grid_kwargs_parent`/`metadata_child` on
`build_forge_blueprint`.

```{include} ../legacy/workflows/generate-models/templates/domains-nested.yaml
:code: yaml
```
