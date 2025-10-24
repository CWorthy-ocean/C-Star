import re
import warnings
from pathlib import Path

from cstar.base.utils import _run_cmd


def _clone(source_repo: str, local_path: str | Path) -> None:
    """Clone `source_repo` to `local_path`"""
    _run_cmd(
        f"git clone {source_repo} {local_path}",
        msg_pre=f"Cloning `{source_repo}`",
        msg_post=f"Cloned {source_repo} to {local_path}",
        msg_err=f"Error when cloning repository {source_repo} to {local_path}.",
        raise_on_error=True,
    )


def _checkout(source_repo: str, local_path: str | Path, checkout_target: str) -> None:
    """Checkout the target `checkout_target` for `source_repo` found in `local_path`."""
    _run_cmd(
        f"git -C {local_path} checkout {checkout_target}",
        msg_pre=f"Checking out `{source_repo}` @ `{checkout_target}`",
        msg_post=f"Checked out {checkout_target} in git repository {local_path}",
        msg_err=f"Error when checking out {checkout_target} in git repository {local_path}.",
        raise_on_error=True,
    )


def _clone_and_checkout(
    source_repo: str, local_path: str | Path, checkout_target: str
) -> None:
    """Clone `source_repo` to `local_path` and checkout `checkout_target`."""
    _clone(source_repo, local_path)
    _checkout(source_repo, local_path, checkout_target)


def _check_local_repo_changed_from_remote(
    remote_repo: str, local_repo: str | Path, checkout_target: str
):
    """Returns True if a local repository has changed since being checked out from a remote at a given target"""
    local_repo = Path(local_repo)

    if (not local_repo.exists()) or (not (local_repo / ".git").exists()):
        return True

        try:
            expected_hash = _get_hash_from_checkout_target(
                repo_url=remote_repo, checkout_target=checkout_target
            )

            # 1. Check current HEAD commit hash
            head_hash = _run_cmd(
                cmd="git rev-parse HEAD", cwd=local_repo, raise_on_error=True
            )

            if head_hash != expected_hash:
                return True  # HEAD is not at the expected hash

            # if HEAD is at expected hash, check if dirty:
            status_output = _run_cmd(
                cmd="git diff-index HEAD", cwd=local_repo, raise_on_error=True
            )

            return bool(status_output.strip())  # True if any changes

        except RuntimeError as e:
            print(f"Git error: {e}")
            return True

        return False


def _get_repo_remote(local_path: str | Path) -> str:
    """Take a local repository path string (local_path) and return as a string the
    remote URL.
    """
    return _run_cmd(
        f"git -C {local_path} remote get-url origin",
        msg_pre=f"Retrieving URL for remote in repository `{local_path}`.",
        msg_post=f"Retrieved URL for remote in repository `{local_path}`.",
        msg_err=f"Error retrieving URL for remote in repository {local_path}.",
    )


def _get_repo_head_hash(local_path: str | Path) -> str:
    """Take a local repository path string (local_path) and return as a string the
    commit hash of HEAD.
    """
    return _run_cmd(
        f"git -C {local_path} rev-parse HEAD",
        msg_pre=f"Retrieving commit hash for repository `{local_path}`.",
        msg_post=f"Retrieved commit hash for repository `{local_path}`.",
        msg_err=f"Error retrieving commit hash for repository {local_path}.",
    )


def _get_hash_from_checkout_target(repo_url: str, checkout_target: str) -> str:
    """Take a git checkout target (any `arg` accepted by `git checkout arg`) and return
    a commit hash.

    This method parses the output of `git ls-remote {repo_url}` to create a dictionary
    of refs and hashes, returning the hash corresponding to `checkout_target` or
    raising an error listing available branches and tags if the target is not found.

    Parameters:
    -----------
    repo_url: str
        URL pointing to a git-controlled repository
    checkout_target: str
        Any valid argument that can be supplied to `git checkout`

    Returns:
    --------
    git_hash: str
        A git commit hash associated with the checkout target
    """
    # Get list of targets from git ls-remote
    ls_remote = _run_cmd(
        f"git ls-remote {repo_url}",
        msg_pre=f"Retrieving remote refs for repository `{repo_url}`.",
        msg_post=f"Retrieved remote refs for repository `{repo_url}`.",
        msg_err=f"Error retrieving remote refs for repository {repo_url}.",
    )

    # Process the output into a `reference: hash` dictionary
    ref_dict = {
        ref: has for has, ref in (line.split() for line in ls_remote.splitlines())
    }

    # If the checkout target is a valid hash, return it
    if checkout_target in ref_dict.values():
        return checkout_target

    # Otherwise, see if it is listed as a branch or tag
    for ref, has in ref_dict.items():
        if (
            ref == f"refs/heads/{checkout_target}"
            or ref == f"refs/tags/{checkout_target}"
        ):
            return has

    # Lastly, if NOTA worked, see if the checkout target is a 7 or 40 digit hexadecimal string
    is_potential_hash = bool(re.fullmatch(r"^[0-9a-f]{7}$", checkout_target)) or bool(
        re.fullmatch(r"^[0-9a-f]{40}$", checkout_target)
    )
    if is_potential_hash:
        warnings.warn(
            f"C-STAR: The checkout target {checkout_target} appears to be a commit hash, "
            f"but it is not possible to verify that this hash is a valid checkout target of {repo_url}"
        )

        return checkout_target

    # If the target is still not found, raise an error listing branches and tags
    branches = [
        ref.replace("refs/heads/", "")
        for ref in ref_dict
        if ref.startswith("refs/heads/")
    ]
    tags = [
        ref.replace("refs/tags/", "")
        for ref in ref_dict
        if ref.startswith("refs/tags/")
    ]

    error_message = (
        f"Supplied checkout_target ({checkout_target}) does not appear "
        f"to be a valid reference for this repository ({repo_url}).\n"
    )
    if branches:
        branch_names = "\n".join(f" - {branch}" for branch in sorted(branches))
        error_message += f"Available branches:\n{branch_names}\n"
    if tags:
        tag_names = "\n".join(f" - {tag}" for tag in sorted(tags))
        error_message += f"Available tags:\n{tag_names}\n"

    raise ValueError(error_message.strip())


def git_location_to_raw(
    repo_url: str, checkout_target: str, filename: str, subdir: str = ""
) -> str:
    """Returns a downloadable file address given information about that file in a remote repository.

    Parameters
    ----------
    repo: str
        The repository location
    checkout_target: str
        The tag, branch, or commit hash from which to get the file
    filename: str
        The name of the file (including extension)
     subdir: str, optional
        The subdirectory (from the repository top level) in which to find the file

    Returns
    -------
    str:
        A URL from which to retrieve the raw file directly
    """
    if "http" not in repo_url.lower():
        raise ValueError(
            f"Please provide a HTTP(S) address to the repository, not {repo_url}"
        )

    repo_url = repo_url.removesuffix(".git")

    if "github.com" in repo_url:
        raw_url_base = repo_url.replace("github.com", "raw.githubusercontent.com")
        return f"{raw_url_base}/{checkout_target}/{subdir}/{filename}"
    elif "gitlab.com" in repo_url:
        return f"{repo_url}/-/raw/{checkout_target}/{subdir}/{filename}"
    elif "bitbucket.org" in repo_url:
        return f"{repo_url}/raw/{checkout_target}/{subdir}/{filename}"
    else:
        raise ValueError(
            f"Git service at {repo_url} unsupported. Please use Github, Gitlab, or Bitbucket addresses"
        )
