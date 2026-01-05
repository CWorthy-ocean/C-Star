import os
import shutil
import typing as t
from pathlib import Path

ENV_CSTAR_ORCH_OUTDIR: t.Literal["CSTAR_ORCH_OUTDIR"] = "CSTAR_ORCH_OUTDIR"
ENV_CSTAR_ORCH_DELAYS: t.Literal["CSTAR_ORCH_DELAYS"] = "CSTAR_ORCH_DELAYS"
ENV_CSTAR_ORCH_TRX_FREQ: t.Literal["CSTAR_ORCH_TRX_FREQ"] = "CSTAR_ORCH_TRX_FREQ"
ENV_CSTAR_ORCH_TRX_RESET: t.Literal["CSTAR_ORCH_TRX_RESET"] = "CSTAR_ORCH_TRX_RESET"
ENV_CSTAR_ORCH_CLOBBER_WD: t.Literal["CSTAR_CLOBBER_WORKING_DIR"] = (
    "CSTAR_CLOBBER_WORKING_DIR"
)
ENV_CSTAR_ORCH_RESET_NAME: t.Literal["CSTAR_ORCH_RESET_NAME"] = "CSTAR_ORCH_RESET_NAME"

ENV_CSTAR_RUNID: t.Literal["CSTAR_RUNID"] = "CSTAR_RUNID"
ENV_CSTAR_SLURM_ACCOUNT: t.Literal["CSTAR_SLURM_ACCOUNT"] = "CSTAR_SLURM_ACCOUNT"
ENV_CSTAR_SLURM_QUEUE: t.Literal["CSTAR_SLURM_QUEUE"] = "CSTAR_SLURM_QUEUE"


def clear_working_dir(path: Path) -> None:
    """Clear specific paths under the working directory if CSTAR_CLOBBER_WORKING_DIR is set.

    Parameters
    ----------
    path: the working directory to be cleared

    Returns
    -------
    None
    """
    if os.getenv(ENV_CSTAR_ORCH_CLOBBER_WD) == "1":
        print(f"clearing {path}")
        shutil.rmtree(path / "input", ignore_errors=True)
        shutil.rmtree(path / "output", ignore_errors=True)
