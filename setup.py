from setuptools import setup

################################################################################

setup(
    name = 'cstar_ocean',
    packages = ['cstar_ocean'],
    version = '0.0.0',
    description = 'Computation Systems for Tracking Ocean Carbon (CSTAR) python package',
    author = 'Dafydd Stephenson, Thomas Nicholas, and others',
    author_email = 'dafydd@cworthy.org, thomas@cworthy.org',
    url = 'https://github.com/CWorthy-ocean/C-Star',
    keywords = ['MCDR','CDR'],
    #include_package_data=True,
    #data_files=[()],
    python_requires = '>=3.10',
    #install_requires = dependency_list,
    #tests_require=[python packages needed for testing],
    license='MIT',
    #classifiers=[list of even more metadata],
    #long_description=<print contents of a README here>,
    #long_description_content_type='text/markdown',
)
    
