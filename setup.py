from setuptools import setup,find_packages
from setuptools.command.install import install
import os,sys

################################################################################

class install_with_externals(install):
    '''
    Customise the setuptools default install procedure
    '''
    def run(self):
        install.run(self) # run the default install procedure first

        self.checkout() # run our checkout externals function
        
    def checkout(self):
        # Add manage_externals to the path so we can import manic
        parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'manage_externals'))
        sys.path.insert(0, parent_dir)
        
        import manic
        
        # Unclear how to call manic.checkout without CLI so just passing redundant args:
        ARGS=manic.checkout.commandline_arguments(['--externals','Externals.cfg'])
        
        try:
            manic.checkout.main(ARGS)
        except Exception as error:  # pylint: disable=broad-except
            manic.printlog(str(error))
            if ARGS.backtrace:
                traceback.print_exc()


setup(
    name = 'cstar_ocean',
    packages = find_packages(),#['cstar_ocean'],
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
    #setup_requires=['pytest-runner'],
    #tests_require=['pytest'],
    license='MIT',
    #classifiers=[list of even more metadata],
    #long_description=<print contents of a README here>,
    #long_description_content_type='text/markdown',
    cmdclass={
        'install': install_with_externals,
    },
)
    
