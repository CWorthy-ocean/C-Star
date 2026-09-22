"""The forge application: execution code that turns a ForgeBlueprint (its blueprint)
into ROMS-MARBL input artifacts. Relocatable into C-Star as an application.
"""

# Register the global PyYAML Enum representer as insurance against a Forge enum reaching
# roms-tools' SafeDumper by any path (imported for its side effect). See the module docstring.
from cstar_forge.forge import _yaml_representers as _yaml_representers
