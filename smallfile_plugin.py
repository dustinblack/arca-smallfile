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
import subprocess
from dataclasses import dataclass
from typing import List
from wolkenwalze_plugin_sdk import plugin


@dataclass
class SmallfileParams:
    """
    These parameters will be passed through to the smallfile_cli.py command unchanged
    """
    top: str
    operation: str
    threads: int
    file_size: int
    files: int


@dataclass
class WorkloadParams:
    """
    This is the data structure for the input parameters of the step defined below.
    """
    samples: int
    SmallfileParams: SmallfileParams


@dataclass
class WorkloadResults:
    """
    This is the output data structure for the success case.
    """
    #TODO
    stdout: str


@dataclass
class WorkloadError:
    """
    This is the output data structure in the error  case.
    """
    error: str


# The following is a decorator (starting with @). We add this in front of our function to define the metadata for our
# step.
@plugin.step(
    id="workload",
    name="smallfile workload",
    description="Run the smallfile workload with the given parameters",
    outputs={"success": WorkloadResults, "error": WorkloadError},
)

def smallfile_run(params: WorkloadParams) -> typing.Tuple[str, typing.Union[WorkloadResults, WorkloadError]]:
    """
    The function  is the implementation for the step. It needs the decorator above to make it into a  step. The type
    hints for the params are required.

    :param params:

    :return: the string identifying which output it is, as well the output structure
    """

    smallfile_dir = "/home/dblack/git/smallfile"
    smallfile_yaml = tempfile.mkstemp()

    with open(smallfile_yaml[1], 'w') as file:
        file.write(SmallfileParams)

    smallfile_cmd = [
        "./smallfile_cli.py",
        "--yaml-input-file",
        smallfile_yaml[1],
    ]

    try:
        subprocess.check_call(smallfile_cmd, text=True, cwd=smallfile_dir, stderr=subprocess.STDOUT)
        return "stdout", WorkloadResults()
    except subprocess.CalledProcessError as error:
        return "error", WorkloadError("smallfile_cli.py failed with return code %d" % error.returncode)


    #return "error", WorkloadError(
    #    "Cannot kill pod %s in namespace %s, function not implemented" % (
    #        params.pod_name_pattern.pattern,
    #        params.namespace_pattern.pattern
    #    ))


if __name__ == "__main__":
    sys.exit(plugin.run(plugin.build_schema(
        # List your step functions here:
        smallfile_run,
    )))
