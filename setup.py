from setuptools import setup, find_packages

################################################################################

setup(
    name="cstar_ocean",
    packages=find_packages(),  # ['cstar_ocean'],
    version="0.0.0",
    description="Computation Systems for Tracking Ocean Carbon (CSTAR) python package",
    author="Dafydd Stephenson, Thomas Nicholas, Matt Long, and others",
    author_email="dafydd@cworthy.org, tom@cworthy.org",
    url="https://github.com/CWorthy-ocean/C-Star",
    keywords=["MCDR", "CDR"],
    # include_package_data=True,
    # data_files=[()],
    python_requires=">=3.10",
    include_package_data=True,
    # install_requires = dependency_list,
    # setup_requires=['pytest-runner'],
    # tests_require=['pytest'],
    license="GNU",
    # classifiers=[list of even more metadata],
    # long_description=<print contents of a README here>,
    # long_description_content_type='text/markdown',
)
