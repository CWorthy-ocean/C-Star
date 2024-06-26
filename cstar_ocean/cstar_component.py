import os
import glob
import tempfile
import subprocess
from abc import ABC, abstractmethod
from typing import List, Union, Optional

from .utils import _calculate_node_distribution,_replace_text_in_file
from .cstar_base_model import BaseModel,ROMSBaseModel,MARBLBaseModel
from .cstar_input_dataset import InputDataset
from .cstar_additional_code import AdditionalCode

from . import _CSTAR_COMPILER,_CSTAR_SCHEDULER,\
    _CSTAR_SYSTEM,_CSTAR_SYSTEM_MAX_WALLTIME,_CSTAR_SYSTEM_DEFAULT_PARTITION,_CSTAR_SYSTEM_CORES_PER_NODE


class Component(ABC):
    """
    A model component that contributes to a unique Case instance.

    Attributes:
    ----------
    base_model: BaseModel
        An object pointing to the unmodified source code of a model handling an individual
        aspect of the simulation such as biogeochemistry or ocean circulation
    additional_code: AdditionalCode or list of AdditionalCodes
        Additional code contributing to a unique instance of a base model,
        e.g. namelists, source modifications, etc.
    input_datasets: InputDataset or list of InputDatasets
        Any spatiotemporal data needed to run this instance of the base model
        e.g. initial conditions, surface forcing, etc.

    Methods:
    -------
    build()
        Compile any component-specific code on this machine
    pre_run()
        Execute any pre-processing actions necessary to run this component
    run()
        Run this component
    post_run()
        Execute any post-processing actions associated with this component
    """


    def __init__(
        self,
        base_model: BaseModel,
        additional_code: Optional[Union[AdditionalCode, List[AdditionalCode]]] = None,
        input_datasets:  Optional[Union[InputDataset, List[InputDataset]]] = None
    ):

        """
        Initialize a Component object from a base model and any additional_code or input_datasets

        Parameters:
        -----------
        base_model: BaseModel
            An object pointing to the unmodified source code of a model handling an individual
            aspect of the simulation such as biogeochemistry or ocean circulation
        additional_code: AdditionalCode or list of AdditionalCodes
            Additional code contributing to a unique instance of a base model,
            e.g. namelists, source modifications, etc.
        input_datasets: InputDataset or list of InputDatasets
            Any spatiotemporal data needed to run this instance of the base model
            e.g. initial conditions, surface forcing, etc.

        Returns:
        --------
        Component:
            An intialized Component object
        """
        
        # FIXME: do Type checking here
        self.base_model = base_model
        self.additional_code = additional_code
        self.input_datasets = input_datasets


        
    def __str__(self):
        name=self.__class__.__name__
        base_str=f"{name} object "
        base_str= "-"*(len(name)+7)+"\n"+base_str
        base_str+="\n"+"-"*(len(name)+7)
        base_str+="\nBuilt from: "
        
        NAC=len(self.additional_code) if isinstance(self.additional_code,list) else 1 if isinstance(self.additional_code,AdditionalCode) else 0
        NID=len(self.input_datasets)  if isinstance(self.input_datasets ,list) else 1 if isinstance(self.input_datasets,InputDataset   ) else 0        
        base_str+=f"\n{NAC} AdditionalCode repositories"
        base_str+=f"\n{NID} InputDataset objects"

        
        return base_str
    
    def __repr__(self):
        return self.__str__()


    @abstractmethod
    def build(self):
        """
        Compile any Component-specific code on this machine

        This abstract method will be implemented differently by different Component types.
        """
        
    @abstractmethod
    def pre_run(self):
        """
        Execute any pre-processing actions necessary to run this component.

        This abstract method will be implemented differently by different Component types.
        """

    @abstractmethod
    def run(self):
        """
        Run this component

        This abstract method will be implemented differently by different Component types.
        """
        pass

    @abstractmethod
    def post_run(self):
        """
        Execute any pre-processing actions associated with this component.

        This abstract method will be implemented differently by different Component types.
        """
        pass
        
class ROMSComponent(Component):
    """
    An implementation of the Component class for the UCLA Regional Ocean Modeling System

    This subclass has unique attributes concerning parallelization, and ROMS-specific implementations
    of the build(), pre_run(), run(), and post_run() methods.

    Attributes:
    -----------
    base_model: ROMSBaseModel
        An object pointing to the unmodified source code of ROMS at a specific commit
    additional_code: AdditionalCode or list of AdditionalCodes
        Additional code contributing to a unique instance of this ROMS run
        e.g. namelists, source modifications, etc.
    input_datasets: InputDataset or list of InputDatasets
        Any spatiotemporal data needed to run this instance of ROMS
        e.g. initial conditions, surface forcing, etc.
    nx,ny,n_levels: int
        The number of x and y points and vertical levels in the domain associated with this object
    n_procs_x,n_procs_y: int
        The number of processes following the x and y directions, for running in parallel

    Properties:
    -----------
    n_procs_tot: int
        The value of n_procs_x * n_procs_y
    
    Methods:
    --------
    build()
        Compiles any code associated with this configuration of ROMS
    pre_run()
        Performs pre-processing steps, such as partitioning input netcdf datasets into one file per core
    run()
        Runs the executable created by `build()`
    post_run()
        Performs post-processing steps, such as joining output netcdf files that are produced one-per-core
    """
    
    def __init__(
            self,
        base_model: ROMSBaseModel,
        additional_code: Optional[Union[AdditionalCode, List[AdditionalCode]]] = None,
        input_datasets:  Optional[Union[InputDataset, List[InputDataset]]] = None,
        nx:        Optional[int] = None,
        ny:        Optional[int] = None,
        n_levels:  Optional[int] = None,
        n_procs_x: Optional[int] = None,
        n_procs_y: Optional[int] = None
        ):
        
        """
        Initialize a ROMSComponent object from a ROMSBaseModel object, code, input datasets, and discretization information

        Parameters:
        -----------
        base_model: ROMSBaseModel
            An object pointing to the unmodified source code of a model handling an individual
            aspect of the simulation such as biogeochemistry or ocean circulation
        additional_code: AdditionalCode or list of AdditionalCodes
            Additional code contributing to a unique instance of a base model,
            e.g. namelists, source modifications, etc.
        input_datasets: InputDataset or list of InputDatasets
            Any spatiotemporal data needed to run this instance of the base model
            e.g. initial conditions, surface forcing, etc.
        nx,ny,n_levels: int
            The number of x and y points and vertical levels in the domain associated with this object
        n_procs_x,n_procs_y: int
            The number of processes following the x and y directions, for running in parallel
        exe_path:
            The path to the ROMS executable, set when `build()` is called
        
        Returns:
        --------
        ROMSComponent:
            An intialized ROMSComponent object
        """
        
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
        """Total number of processors required by this ROMS configuration"""
        return self.n_procs_x * self.n_procs_y
        
            
    def build(self):
        """
        Compiles any code associated with this configuration of ROMS.

        Compilation occurs in the directory
        `ROMSComponent.additional_code.local_path/source_mods/ROMS`.
        This method sets the ROMSComponent `exe_path` attribute.
        
        """

        local_path=self.additional_code.local_path
        
        builddir=local_path+'/source_mods/ROMS/'
        if os.path.isdir(builddir+'Compile'):
            subprocess.run('make compile_clean',cwd=builddir,shell=True)
        subprocess.run(f'make COMPILER={_CSTAR_COMPILER}',\
                       cwd=builddir,shell=True)

        self.exe_path=builddir+'roms'

    def pre_run(self):
        """
        Performs pre-processing steps associated with this ROMSComponent object.

        This method:
        1. goes through any netcdf files associated with InputDataset objects belonging
           to this ROMSComponent instance and runs `partit`, a ROMS program used to
           partition netcdf files such that there is one file per processor.
           The partitioned files are stored in a subdirectory `PARTITIONED` of
           InputDataset.local_path
        
        2. Replaces the template strings INPUT_DIR and MARBL_NAMELIST_DIR (if present)
           in the roms namelist file (typically `roms.in`) used to run the model with
           the respective paths to input datasets and any MARBL namelists (if this ROMS
           component belongs to a case for which MARBL is also a component).
           The namelist file is sought in
           `ROMSComponent.additional_code.local_path/namelists/ROMS`.
        
        """

        # Partition input datasets
        if self.input_datasets is not None:
            datasets_to_partition=self.input_datasets if isinstance(self.input_datasets,list) else [self.input_datasets,]

            #dspath=local_path+'/input_datasets/ROMS/'
            os.makedirs(dspath+'PARTITIONED',exist_ok=True)
            ndig=len(str(self.n_procs_tot)) # number of digits in n_procs_tot to pad filename strings
            for f in datasets_to_partition:
                dspath=f.local_path
                fname=os.path.basename(f.source)
                subprocess.run('partit '+str(self.n_procs_x)+' '+str(self.n_procs_y)+' ../'+fname,
                               cwd=dspath+'PARTITIONED',shell=True)

            

        ################################################################################
        ## NOTE: we assume that roms.in is the ONLY entry in additional_code.namelists, hence [0]
        _replace_text_in_file(self.additional_code.local_path+'/'+self.additional_code.namelists[0],\
                              'INPUT_DIR',self.additional_code.local_path+'/input_datasets/ROMS')
        
        ##FIXME: it doesn't make any sense to have the next line in ROMSComponent, does it?
        _replace_text_in_file(self.additional_code.local_path+'/'+self.additional_code.namelists[0],\
                              'MARBL_NAMELIST_DIR',self.additional_code.local_path+'/namelists/MARBL')
        ################################################################################

    def run(self,account_key=None,
                 walltime=_CSTAR_SYSTEM_MAX_WALLTIME,
                 job_name='my_roms_run'):

        """
        Runs the executable created by `build()`

        This method creates a temporary file to be submitted to the job scheduler (if any)
        on the calling machine, then submits it. By default the job requests the maximum
        walltime. It calculates the number of nodes and cores-per-node to request based on
        the number of cores required by the job, `ROMSComponent.n_procs_tot`.

        Parameters:
        -----------
        account_key: str, default None
            The users account key on the system
        walltime: str, default _CSTAR_SYSTEM_MAX_WALLTIME
            The requested length of the job, HH:MM:SS
        job_name: str, default 'my_roms_run'
            The name of the job submitted to the scheduler, which also sets the output file name
            `job_name.out`
        """
        
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
        """
        Performs post-processing steps associated with this ROMSComponent object.

        This method goes through any netcdf files produced by the model in
        `additional_code.local_path/output/PARTITIONED` and runs `ncjoin`,
        a ROMS program used to join netcdf files that are produced separately by each processor.
        The joined files are saved in
        `additional_code.local_path/output`
                
        Parameters:
        -----------
        local_path: str
            The path where this ROMS component is being assembled
        """
        
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
    # Inherits its docstring from Component
    
    def build(self,local_path):
        print('source code modifications to MARBL are not yet supported')

    def pre_run(self,local_path):
        print('no pre-run actions involving MARBL are currently supported')
        
    def run(self,local_path):
        print('MARBL must be run in the context of a parent model')
        
    def post_run(self):
        print('no post-run actions involving MARBL are currently supported')

        
