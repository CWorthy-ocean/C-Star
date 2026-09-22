"""The forge application: execution code that turns a ForgeBlueprint (its blueprint)
into ROMS-MARBL input artifacts.
"""

# Register the global PyYAML Enum representer as insurance against a Forge enum reaching
# roms-tools' SafeDumper by any path (imported for its side effect). See the module docstring.
from cstar.applications.forge import _yaml_representers as _yaml_representers
from cstar.applications.forge import app as app
