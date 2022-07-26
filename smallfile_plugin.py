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
    threads: int
    files: int
    file_size: str = dataclasses.field(metadata={"id": "file-size"})


@dataclass
class WorkloadParams:
    """
    This is the data structure for the input parameters of the step defined below.
    """
    samples: int
    SmallfileParams: SmallfileParams
    SmallfileRelease: typing.Optional[str] = None


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



smallfile_schema = plugin.build_object_schema(SmallfileParams)

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

    smallfile_url = "https://github.com/distributed-system-analysis/smallfile.git"
    smallfile_release = WorkloadParams.SmallfileRelease or "1.1"
    smallfile_dir = tempfile.mkdtemp()
    smallfile_yaml = tempfile.mkstemp()
    #TODO: Clean up tempdir/file

    Repo.clone_from(smallfile_url, smallfile_dir)
    g = Git(smallfile_dir)
    g.checkout(smallfile_release)

    with open(smallfile_yaml[1], 'w') as file:
        file.write(yaml.dump(smallfile_schema.serialize(params.SmallfileParams)))

    smallfile_cmd = [
        "{}/smallfile_cli.py".format(smallfile_dir),
        "--yaml-input-file",
        smallfile_yaml[1],
    ]

    try:
        proc = subprocess.check_output(smallfile_cmd, text=True, cwd=smallfile_dir, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as error:
        return "error", WorkloadError("smallfile_cli.py failed with return code {}".format(error.returncode))

    return "success", WorkloadResults(proc)


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
