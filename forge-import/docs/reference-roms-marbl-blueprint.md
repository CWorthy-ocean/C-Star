# ROMS-MARBL blueprint (`RomsMarblBlueprint`)

The forge application emits a ROMS-MARBL blueprint at the end of a
`python -m cstar_forge.run` processing run; it is the handoff to C-Star, which uses
it to build and run the simulation.

Example (a saved legacy-layout blueprint):

`legacy/blueprints/MacOS/cson_roms-marbl_v0.1_test-tiny_1procs/B_cson_roms-marbl_v0.1_test-tiny_1procs.yaml`

```{include} ../legacy/blueprints/MacOS/cson_roms-marbl_v0.1_test-tiny_1procs/B_cson_roms-marbl_v0.1_test-tiny_1procs.yaml
:code: yaml
```
