import os, fsspec
from pathlib import Path
import yaml

# import some python package that keeps track of/enforces a hierachical file system?
#  catalog:

class DomainCatalog():
    '''C-Star DomainCatalog class handles the hierachical system of validated/registerd "domains."
    The DomainCatalog is designed to hold, at the root in side a folder called 'catalog', models and
    model domains that together describe "validated" C-Star model solutions, where validated
    indicates model review and reflections (e.g. input considerations/deviations rom C-Star default
    processing, outputs, intended use, uncertainty, caveats) descrbed in catalog metadata.

    The base of a given catalog (self.catalog_root) is intended to be somewhere in the local file system,
    perhaps a git repository, or a web address. The details of the location are handled by the fsspec
    package/filesystem.

    The DomainCatalog can process models, blueprints, and observations present in the catalog (report
    those available, returing yaml-based schema for particular named <model, domain, observation>).

    Returned domains can be combined with models using forge to develop blueprints for new C-Star
    simulations on new machines. Archived blueprints can be run directly if all domain assests and
    the orginal run machine/setup/container are available.

    The DomainCatalog can also "register" new domains, which consists of adding required dmoain metadata,
    linking non-forge default generated files (e.g. groomed grids, or other input files) to persistent
    copyies/storage, and optionally copying the domain an alternate catalog root (e.g. from local storage
    to github or other service)

    The domain catalog should probably abstract a storage_service concept, were writing and reading domain
    assets happen through the service (e.g. local gile systme github...)

    The catalog structure is as follows:

    Catalog/
    ├── Machines/
    │   ├── Anvil.yml #example machine file
    │   └── Derecho.yml #example machine file
    ├── ModelSpec/
    │   ├── ROMS-v0.yml #example model spec file
    │   ├── ROMS-MARBL-v0.yml #example model spec file
    │   ├── ROMS-DyeTracer-v0.yml #example model spec file
    │   └── ROMS-CDR-v0.yml #example model spec file
    ├── DomainSpec/
    │   ├── Pacific-12km/ #example domain spec
    │   │   ├── Domain.yml
    │   │   └── Assets/{*.png, *.pdf}
    │   └── SalishSea-Nested/ #example domain spec
    │       ├── Domain.yml
    │       └── Assets/{*.png, *.pdf}
    ├── Blueprints/
    │   └── Anvil_ROMS-MARBL-v0_Pacific-12km_20220101-20261231/ #example blueprint
    │       ├── Archive.yml              # meta data and pointer to output
    |       |── Build/                     # build directory
    │       └── Assets/{*.png, *.mp4, *.pdf, *.csv}    # plots, movies, skill metrics, reports/papers
    └── Observations/
        ├── glodap.py  # example of a dataset
        └── woa.py   # example of a dataset

    '''

    catalog_root: None | Path | str     # {Path, str, github url}
    cat: None | fsspec.filesystem       # fsspec instance of this catalog's filesystem
    _domains: list[Path] = []           # list of paths to domain spec yaml instances (read-oin yaml files)
    _blueprints: list[Path] = []        # list of paths to blueprint yaml instances (read-oin yaml files)
    _models: list[Path] = []            # list of paths to model yaml instances (read-oin yaml files)

    def __init__(catalog_root=None):

        if catalog_root is None:
            # find directory of this file (__main__?), set catalog root to <this_dir>/calaog
            pass
        else:
            # typcheck for {github url; <Path, str> exists ..}
            self.catalog_root = catalog_root
            self.cat = self.infer_service()

        # scan for required directry structure, if it doesn't exist, create it?
        # While scanning, catalog file list of yamls in catalog/models, catalog/blueprints,
        # catalog/domains
        self._scan_domains()
        self._scan_blueprints()
        self._scan_models()

    def infer_service():
        # find the setup the service that feeds us catalog files
        if not isinstance(self.catalog_root, Path):
            if isinstance(self.catalog_root, str):
                if 'github' in self.catalog_root:
                    return fsspec.filesystem('github', root=self.catalog_root)
                if self.catalog_root.startswith('http'):
                    return fsspec.filesystem('http', root=self.catalog_root)
                else:
                    return fsspec.filesystem('file', root=self.catalog_root)
            raise ValueError(f'Could not infer serice from catalog_root: {self.catalog_root}')
        else:
            return fsspec.filesystem('file', root=catalog_root)

    def _scan_domains():
        '''Add docstring, check is this is valid'''
        self._domains = []
        for domain in self.catalog_root.glob("catalog/domains/*.yml"):
            self._domains.append(Path(domain))

    def _scan_blueprints():
        '''functionality incorporated from catalog.py, providing similar services at the class level'''
        pass  # add exising functionaliy from inputs_data.py recusively return build blueprints, organized by domain name

    def _scan_models():
        '''scan the models yaml files, essentially the same things as _scan_domains()'''
        pass # scan the models.yaml, essential a database

    def register_domain(builder: CstarSpecBuilder):
        ''' Create a new domain by copying information from a CstarSpecBuilder ... somehow this has
        to be called by CstarSpecBuilder, as it will supply the builder paths.

        '''
        pass

    def register_model(model_yaml: Path | str):
        '''register a new model by validatingm, copying a model yaml file to the catalog,
        and rescanning the catalog after adding'''
        pass

    def add_assetto_domain(domain_name: str, asset_name: str, asset_file: any, asset_metadata: dict):
        '''add a file to the assests folder of a domain spec yaml instance, and add the asset
        metadata to the domain spec yaml instance, including a relative path to the asset from the domain spec yaml instance.

        For now, the assest metadata is a dictionary of key-value pairs, with no requirements (may be empty)
        '''
        pass

    def copy_domain(domain_name: str, catalog: DomainCatalog):
        '''copy a domain spec (essentially domain sub-directory copy)'''
        pass

    def copy_model(model_name: str, catalog: DomainCatalog):
        '''copy a model yaml instance/file to a new catalog'''
        pass

    def domain(domain_id: str | int):
        '''return a domain spec yaml instance by name or index'''
        if isinstance(domain_id, str):
            return self._domains[domain_id]
        elif isinstance(domain_id, int):
            return self._domains[domain_id]
        else:
            raise ValueError(f'Invalid domain id: {domain_id}')

    def blueprint(blueprint_id: str | int):
        '''return a blueprint yaml instance by name or index'''
        if isinstance(blueprint_id, str):
            return self._blueprints[blueprint_id]
        elif isinstance(blueprint_id, int):
            return self._blueprints[blueprint_id]
        else:
            raise ValueError(f'Invalid blueprint id: {blueprint_id}')

    def model(model_id: str | int):
        '''return a model yaml instance by name or index'''
        if isinstance(model_id, str):
            return self._models[model_id]
        elif isinstance(model_id, int):
            return self._models[model_id]
        else:
            raise ValueError(f'Invalid model id: {model_id}')
