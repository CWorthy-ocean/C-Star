import os
import re
import subprocess
from math import ceil

def _get_hash_from_checkout_target(repo_url, checkout_target):
    # First check if the checkout target is a 7 or 40 digit hexadecimal string
    is_potential_hash = bool(re.match(r"^[0-9a-f]{7}$", checkout_target)) or bool(
        re.match(r"^[0-9a-f]{40}$", checkout_target)
    )

    # Then try ls-remote to see if there is a match
    # (no match if either invalid target or a valid hash):
    ls_remote = subprocess.run(
        "git ls-remote " + repo_url + " " + checkout_target,
        shell=True,
        capture_output=True,
        text=True,
    ).stdout

    if len(ls_remote) == 0:
        if is_potential_hash:
            # just return the input target assuming a hash, but can't validate
            return checkout_target
        else:
            raise ValueError(
                "supplied checkout_target does not appear "
                + "to be a valid reference for this repository"
            )
    else:
        return ls_remote.split()[0]

def _calculate_node_distribution(n_cores_required,tot_cores_per_node):
    ''' Given the number of cores required for a job and the total number of cores on a node,
    calculate how many nodes to request and how many cores to request on each'''
    n_nodes_to_request=ceil(n_cores_required/tot_cores_per_node)
    cores_to_request_per_node= ceil(
        tot_cores_per_node - ((n_nodes_to_request * tot_cores_per_node) - n_cores_required)/n_nodes_to_request
    )
    
    return n_nodes_to_request,cores_to_request_per_node

def _replace_text_in_file(file_path,old_text,new_text):
    temp_file_path = file_path + '.tmp'
    
    with open(file_path, 'r') as read_file, open(temp_file_path, 'w') as write_file:
        for line in read_file:
            new_line = line.replace(old_text, new_text)
            write_file.write(new_line)
            
    os.remove(file_path)
    os.rename(temp_file_path, file_path)
