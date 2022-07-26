#!/usr/bin/env python3
"""
Copyright 2022 Dustin Black

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import re
import sys
import typing
import tempfile
import yaml
import subprocess
import dataclasses
import fileinput
import os
import shutil
from dataclasses import dataclass
from typing import List
from wolkenwalze_plugin_sdk import plugin
from git import Repo, Git


@dataclass
class SmallfileParams:
    """
    The parameters in this schema will be passed through to the smallfile_cli.py command unchanged
    """
    top: str
    operation: str
    file_size: typing.Optional[str] = dataclasses.field(default=None, metadata={"id": "file-size"})
    #FIXME: wolkenwalze throws an error when using typing.Optional with an int and the key is empty
    threads: typing.Optional[int] = None
    files: typing.Optional[int] = None
    #TODO: Expand supported parameters and determine defaults/requirements


@dataclass
class WorkloadParams:
    """
    This is the data structure for the input parameters of the step defined below.
    """
    #FIXME: Enable boolean types
    cleanup: str
    samples: int
    #TODO: Implement samples? Or do we do this outside of the plugin?
    SmallfileParams: SmallfileParams
    SmallfileRelease: typing.Optional[str] = None


@dataclass
class WorkloadResults:
    """
    This is the output data structure for the success case.
    """
    #TODO
    json: str


@dataclass
class WorkloadError:
    """
    This is the output data structure in the error  case.
    """
    error: str



smallfile_schema = plugin.build_object_schema(SmallfileParams)

# The following is a decorator (starting with @). We add this in front of our function to define the metadata for our step.
@plugin.step(
    id="workload",
    name="smallfile workload",
    description="Run the smallfile workload with the given parameters",
    outputs={"success": WorkloadResults, "error": WorkloadError},
    #TODO: Add step for cache dropping; Or do we handle this in a separate plugin?
)

def smallfile_run(params: WorkloadParams) -> typing.Tuple[str, typing.Union[WorkloadResults, WorkloadError]]:
    """
    The function  is the implementation for the step. It needs the decorator above to make it into a  step. The type
    hints for the params are required.

    :param params:

    :return: the string identifying which output it is, as well the output structure
    """

    smallfile_url = "https://github.com/distributed-system-analysis/smallfile.git"
    smallfile_release = params.SmallfileRelease or "1.1"
    smallfile_dir = tempfile.mkdtemp()
    smallfile_yaml = tempfile.mkstemp()
    smallfile_out = tempfile.mkstemp()

    # Clone the smallfile repo and checkout the correct release
    Repo.clone_from(smallfile_url, smallfile_dir)
    g = Git(smallfile_dir)
    g.checkout(smallfile_release)

    # Copy all parameters from SmallfileParams directly for the smallfile CLI to use via YAML
    with open(smallfile_yaml[1], 'w') as file:
        file.write(yaml.dump(smallfile_schema.serialize(params.SmallfileParams)))

    smallfile_cmd = [
        "{}/smallfile_cli.py".format(smallfile_dir),
        "--yaml-input-file",
        smallfile_yaml[1],
        "--output-json",
        smallfile_out[1],
        "--response-times",
        "y",
    ]

    # Run smallfile
    try:
        print(subprocess.check_output(smallfile_cmd, cwd=smallfile_dir, text=True, stderr=subprocess.STDOUT))
    except subprocess.CalledProcessError as error:
        temp_cleanup(smallfile_yaml, smallfile_dir)
        return "error", WorkloadError("{} failed with return code {}:\n{}".format(error.cmd[0],error.returncode,error.output))

    # Cleanup after run, if enabled
    if params.cleanup:
        print("Test complete; Cleaning up operation files...")
        cleanup_yaml = smallfile_schema.serialize(params.SmallfileParams)
        cleanup_yaml["operation"] = "cleanup"
        smallfile_cleanup_yaml = tempfile.mkstemp()
        smallfile_cleanup_cmd = [
            "{}/smallfile_cli.py".format(smallfile_dir),
            "--yaml-input-file",
            smallfile_cleanup_yaml[1],
        ]
        with open(smallfile_cleanup_yaml[1], 'w') as file:
            file.write(yaml.dump(cleanup_yaml))
        try:
            print(subprocess.check_output(smallfile_cleanup_cmd, cwd=smallfile_dir, text=True, stderr=subprocess.STDOUT))
        except subprocess.CalledProcessError as error:
            temp_cleanup(smallfile_yaml, smallfile_dir)
            temp_cleanup(smallfile_cleanup_yaml)
            return "error", WorkloadError("{} failed with return code {}:\n{}".format(error.cmd[0],error.returncode,error.output))
        temp_cleanup(smallfile_cleanup_yaml)


    with open(smallfile_out[1], 'r') as output:
        smallfile_results = output.read()
    temp_cleanup(smallfile_yaml, smallfile_dir)
    return "success", WorkloadResults(smallfile_results)



def temp_cleanup(file = [], directory = ""):
    # Cleanup our temp files
    print("Cleaning up temporary files...")
    if file:
        os.close(file[0])
        os.remove(file[1])
    if directory:
        shutil.rmtree(directory)



if __name__ == "__main__":
    sys.exit(plugin.run(plugin.build_schema(
        # List your step functions here:
        smallfile_run,
    )))
