import os
import glob
import tempfile
import subprocess
from abc import ABC, abstractmethod
from typing import List, Union, Optional

from .utils import _calculate_node_distribution,_replace_text_in_file
from .cstar_base_model import BaseModel
from .cstar_input_dataset import InputDataset
from .cstar_additional_code import AdditionalCode

from . import _CSTAR_COMPILER,_CSTAR_SCHEDULER,\
    _CSTAR_SYSTEM,_CSTAR_SYSTEM_MAX_WALLTIME,_CSTAR_SYSTEM_DEFAULT_PARTITION,_CSTAR_SYSTEM_CORES_PER_NODE


class Component(ABC):
    """A model component of this Case, e.g. ROMS as the ocean physics component"""

    def __init__(
        self,
        base_model: BaseModel,
        discretization:  Optional[dict] = None,
        additional_code: Optional[Union[AdditionalCode, List[AdditionalCode]]] = None,
        input_datasets:  Optional[Union[InputDataset, List[InputDataset]]] = None,
    ):
        # Do Type checking here
        self.base_model = base_model
        self.additional_code = additional_code
        self.input_datasets = input_datasets


    def __repr__(self):
        return (f"Component(base_model={self.base_model},"+\
                     f"additional_code={self.additional_code},"+\
                      f"input_datasets={self.input_datasets}")


    @abstractmethod
    def build(self):
        ''' Build the model component'''
        
    @abstractmethod
    def pre_run(self):
        '''Things to do before running with this component'''

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def post_run(self):
        pass
        
class ROMSComponent(Component):

    def __init__(
            self,
        base_model: BaseModel,
        discretization:  Optional[dict] = None,
        additional_code: Optional[Union[AdditionalCode, List[AdditionalCode]]] = None,
        input_datasets:  Optional[Union[InputDataset, List[InputDataset]]] = None,
        nx:        Optional[int] = None,
        ny:        Optional[int] = None,
        n_levels:  Optional[int] = None,
        n_procs_x: Optional[int] = None,
        n_procs_y: Optional[int] = None
        ):
        super().__init__(
            base_model=base_model,
            additional_code=additional_code,
            input_datasets=input_datasets)
        # QUESTION: should all these attrs be passed in as a single "discretization" arg of type dict?
        self.nx=nx
        self.ny=ny
        self.n_levels=n_levels
        self.n_procs_x=n_procs_x
        self.n_procs_y=n_procs_y
        self.exe_path=None

    @property
    def n_procs_tot(self):
        return self.n_procs_x * self.n_procs_y
        
            
    def build(self,local_path):
        ''' We need here to go to the additional code/source mods and run make'''
        builddir=local_path+'/source_mods/ROMS/'
        if os.path.isdir(builddir+'Compile'):
            subprocess.run('make compile_clean',cwd=builddir,shell=True)
        subprocess.run(f'make COMPILER={_CSTAR_COMPILER}',\
                       cwd=builddir,shell=True)

        self.exe_path=builddir+'roms'

    def pre_run(self,local_path):
        ''' Before running ROMS, we need to run partit on all the netcdf files'''

        # Partition input datasets
        if self.input_datasets is not None:
            datasets_to_partition=self.input_datasets if isinstance(self.input_datasets,list) else [self.input_datasets,]

            dspath=local_path+'/input_datasets/ROMS/'
            os.makedirs(dspath+'PARTITIONED',exist_ok=True)
            ndig=len(str(self.n_procs_tot)) # number of digits in n_procs_tot to pad filename strings
            for f in datasets_to_partition:
                fname=os.path.basename(f.source)
                subprocess.run('partit '+str(self.n_procs_x)+' '+str(self.n_procs_y)+' ../'+fname,
                               cwd=dspath+'PARTITIONED',shell=True)

            

        ################################################################################
        ## NOTE: we assume that roms.in is the ONLY entry in additional_code.namelists, hence [0]
        _replace_text_in_file(local_path+'/'+self.additional_code.namelists[0],'INPUT_DIR',local_path+'/input_datasets/ROMS')
        
        ##FIXME: it doesn't make any sense to have the next line in ROMSComponent, does it?
        _replace_text_in_file(local_path+'/'+self.additional_code.namelists[0],'MARBL_NAMELIST_DIR',local_path+'/namelists/MARBL')
        ################################################################################

    def run(self,account_key=None,
                 walltime=_CSTAR_SYSTEM_MAX_WALLTIME,
                 job_name='my_roms_run'):

        run_path=self.additional_code.local_path+'/output/PARTITIONED/'
        #FIXME this only works if additional_code.get() is called in the same session
        os.makedirs(run_path,exist_ok=True)
        if self.exe_path is None:
            #FIXME this only works if build() is called in the same session
            print(f'C-STAR: Unable to find ROMS executable. Run .build() first.')
        else:
            print(self.exe_path)
            match _CSTAR_SYSTEM:
                case "sdsc_expanse":
                    exec_pfx="srun --mpi=pmi2"
                case "nersc_perlmutter":
                    exec_pfx="srun"
                case "ncar_derecho":
                    exec_pfx="mpirun"
                case "osx_arm64":
                    exec_pfx="mpirun"

                #FIXME (probably throughout): self.additional_code /could/ be a list
                # need to figure out which element to use
            roms_exec_cmd=\
                f"{exec_pfx} -n {self.n_procs_tot} {self.exe_path} "+\
                f"{self.additional_code.local_path}/{self.additional_code.namelists[0]}"

            print(roms_exec_cmd)
            
            if _CSTAR_SYSTEM_CORES_PER_NODE is not None:
                nnodes,ncores=_calculate_node_distribution(
                    self.n_procs_tot,_CSTAR_SYSTEM_CORES_PER_NODE)
                    
            match _CSTAR_SCHEDULER:
                
                case "pbs":
                    scheduler_script=f"""PBS -S /bin/bash
                    #PBS -N {job_name}
                    #PBS -o {jobname}.out
                    #PBS -A {account_key}
                    #PBS -l select={nnodes}:ncpus={ncores},walltime={walltime}
                    #PBS -q {_CSTAR_SYSTEM_DEFAULT_PARTITION}
                    #PBS -j oe
                    #PBS -k eod
                    #PBS -V

                    {roms_exec_cmd}
                    """

                    with tempfile.NamedTemporaryFile(mode='w',delete=False,suffix='.pbs') as f:
                        f.write(scheduler_script)
                        subprocess.run(f'qsub {f.name}',shell=True)
                    
                case "slurm":

                    #TODO: export ALL copies env vars, but will need to handle module load
                    
                    scheduler_script=f"""#!/bin/bash
                    #SBATCH --job-name={job_name}
                    #SBATCH --output={outfile}
                    #SBATCH --partition={_CSTAR_SYSTEM_DEFAULT_PARTITION}
                    #SBATCH --nodes={nnodes}
                    #SBATCH --ntasks-per-node={ncores}
                    #SBATCH --acount={account_key}
                    #SBATCH --export=ALL
                    #SBATCH --mail-type=ALL
                    #SBATCH -C=cpu
                    #SBATCH -t={walltime}

                    {roms_exec_cmd}
                    """
                    with tempfile.NamedTemporaryFile(mode='w',delete=False,suffix='.sh') as f:
                        f.write(scheduler_script)
                        subprocess.run(f'sbatch {f.name}',shell=True)

                case None:
                    subprocess.run(roms_exec_cmd,shell=True,cwd=run_path)
                    
    def post_run(self):
        out_path=self.additional_code.local_path+'/output/'
        #run_path='/Users/dafyddstephenson/Code/my_c_star/cstar_ocean/rme_case/output/'
        files = glob.glob(out_path+'PARTITIONED/*.0.nc')
        if not files:
            print('no suitable output found')
        else:
            for f in files:
                print(f)
                subprocess.run('ncjoin '+f[:-4]+'?.nc',cwd=out_path,shell=True)

                    
class MARBLComponent(Component):
    def build(self,local_path):
        print('source code modifications to MARBL are not yet supported')

    def pre_run(self,local_path):
        print('no pre-run actions involving MARBL are currently supported')
    def run(self,local_path):
        print('MARBL must be run in the context of a parent model')
    def post_run(self):
        print('no post-run actions involving MARBL are currently supported')

        
