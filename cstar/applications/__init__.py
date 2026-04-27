import glob
from os.path import basename, dirname, isfile, join

modules = glob.glob(join(dirname(__file__), "*.py"))
__all__ = [
    basename(f)[:-3]
    for f in modules
    if isfile(f) and not f.endswith("__init__.py") and not f.endswith("utils.py")
]
# import importlib
# for module in [ basename(f)[:-3] for f in modules if isfile(f) and not f.endswith('__init__.py') and not f.endswith('utils.py')]:
#     importlib.import_module(f"cstar.applications.{module}")
