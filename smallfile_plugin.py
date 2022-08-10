#!/usr/bin/env python3

import re
import sys
import typing
import tempfile
import yaml
import json
import subprocess
import dataclasses
import fileinput
import os
import shutil
import csv
from dataclasses import dataclass
from typing import List
from arcaflow_plugin_sdk import plugin
from arcaflow_plugin_sdk import schema
#from git import Repo, Git


@dataclass
class SmallfileParams:
    """
    The parameters in this schema will be passed through to the smallfile_cli.py command unchanged
    """
    top: str
    operation: str
    file_size: typing.Optional[int] = dataclasses.field(default=None, metadata={"id": "file-size"})
    threads: typing.Optional[int] = None
    files: typing.Optional[int] = None
    #TODO: Expand supported parameters and determine defaults/requirements

smallfile_schema = plugin.build_object_schema(SmallfileParams)


@dataclass
class WorkloadParams:
    """
    This is the data structure for the input parameters of the step defined below.
    """
    samples: int
    #TODO: Implement samples? Or do we do this outside of the plugin?
    SmallfileParams: SmallfileParams
    SmallfileRelease: typing.Optional[str] = None
    cleanup: typing.Optional[bool] = True


@dataclass
class SmallfileOutputParams:
    host_set: str
    launch_by_daemon: bool
    version: str
    top: str
    operation: str
    files_per_thread: int
    threads: int
    file_size: int
    file_size_distr: int
    files_per_dir: int
    share_dir: str
    hash_to_dir: str
    fsync_after_modify: str
    pause_between_files: float
    auto_pause: bool
    cleanup_delay_usec_per_file: int
    finish_all_requests: str
    stonewall: str
    verify_read: str
    xattr_size: int
    xattr_count: int
    permute_host_dirs: str
    network_sync_dir: str
    min_directories_per_sec: int
    total_hosts: int
    startup_timeout: int
    host_timeout: int
    fname_prefix: typing.Optional[str] = None
    fname_suffix: typing.Optional[str] = None
    
output_params_schema = plugin.build_object_schema(SmallfileOutputParams)


@dataclass
class SmallfileOutputThread:
    elapsed: float
    files: int
    records: int
    filesPerSec: float
    IOPS: float
    MiBps: float
    

@dataclass
class SmallfileOutputResults:
    elapsed: float
    files: int
    records: int
    filesPerSec: float
    IOPS: float
    MiBps: float
    totalhreads: int
    totalDataGB: float
    pctFilesDone: float
    startTime: float
    status: str
    #date: datetime
    #FIXME: Enable datetime data type
    # https://github.com/arcalot/arcaflow-plugin-sdk-python/issues/3
    date: str
    thread: typing.Dict[int, SmallfileOutputThread]

output_results_schema = plugin.build_object_schema(SmallfileOutputResults)


@dataclass
class SmallfileOutputRsptimes:
    host_thread: str
    samples: int
    min: float
    max: float
    mean: float
    pctdev: float
    pctile50: float
    pctile90: float
    pctile95: float
    pctile99: float

output_rsptimes_schema = schema.ListType(
        plugin.build_object_schema(SmallfileOutputRsptimes)
)


@dataclass
class WorkloadResults:
    """
    This is the output data structure for the success case.
    """
    #TODO
    sf_params: SmallfileOutputParams
    sf_results: SmallfileOutputResults
    sf_rsptimes: typing.List[SmallfileOutputRsptimes]


@dataclass
class WorkloadError:
    """
    This is the output data structure in the error  case.
    """
    error: str




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

#    smallfile_url = "https://github.com/distributed-system-analysis/smallfile.git"
#    smallfile_release = params.SmallfileRelease or "1.1"
#    smallfile_dir = tempfile.mkdtemp()
    smallfile_dir = "/plugin/smallfile"
    smallfile_yaml_file = tempfile.mkstemp()
    smallfile_out_file = tempfile.mkstemp()

#    # Clone the smallfile repo and checkout the correct release
#    print("==>> Getting smallfile release {} ...".format(smallfile_release))
#    Repo.clone_from(smallfile_url, smallfile_dir)
#    g = Git(smallfile_dir)
#    g.checkout(smallfile_release)

    # Copy all parameters from SmallfileParams directly for the smallfile CLI to use via YAML
    print("==>> Importing workload parameters ...")
    smallfile_params= smallfile_schema.serialize(params.SmallfileParams)
    with open(smallfile_yaml_file[1], 'w') as file:
        file.write(yaml.dump(smallfile_params))

    smallfile_cmd = [
        "{}/smallfile_cli.py".format(smallfile_dir),
        "--yaml-input-file",
        smallfile_yaml_file[1],
        "--output-json",
        smallfile_out_file[1],
        "--response-times",
        "y",
    ]

    # Run smallfile workload
    print("==>> Running smallfile workload ...")
    try:
        print(subprocess.check_output(smallfile_cmd, cwd=smallfile_dir, text=True, stderr=subprocess.STDOUT))
    except subprocess.CalledProcessError as error:
        temp_cleanup(smallfile_yaml_file, smallfile_dir)
        return "error", WorkloadError("{} failed with return code {}:\n{}".format(error.cmd[0],error.returncode,error.output))

    with open(smallfile_out_file[1], 'r') as output:
        smallfile_results = output.read()

    smallfile_json = json.loads(smallfile_results)
    #debug
    #print(smallfile_results)
    #print(smallfile_json)

    #TODO: Convert results into schema data


    # Post-process output for response times
    print("==>> Collecting response times ...")
    elapsed_time = float(smallfile_json["results"]["elapsed"])
    start_time = smallfile_json["results"]["startTime"]
    rsptime_dir = os.path.join(smallfile_params["top"], "network_shared")
    smallfile_process_cmd = [
        "{}/smallfile_rsptimes_stats.py".format(smallfile_dir),
        "--time-interval",
        str(max(int(elapsed_time / 120.0), 1)),
        "--start-time",
        str(int(start_time)),
        rsptime_dir,
    ]

    try:
        print(subprocess.check_output(smallfile_process_cmd, cwd=smallfile_dir, text=True, stderr=subprocess.STDOUT))
    except subprocess.CalledProcessError as error:
        temp_cleanup(smallfile_out_file, smallfile_dir)
        return "error", WorkloadError("{} failed with return code {}:\n{}".format(error.cmd[0],error.returncode,error.output))

    rsptimes_schema_to_results_map = {
        "host_thread": "host:thread",
        "samples": " samples",
        "min": " min",
        "max": " max",
        "mean": " mean",
        "pctdev": " %dev",
        "pctile50": " 50%ile",
        "pctile90": " 90%ile",
        "pctile95": " 95%ile",
        "pctile99": " 99%ile"
    }

    smallfile_rsptimes = []
    with open("{}/stats-rsptimes.csv".format(rsptime_dir), newline='') as csvfile:
        rsptimes_csv = csv.DictReader(csvfile)
        for row in rsptimes_csv:
            if not re.match("per-", row["host:thread"]) and not re.match("cluster-", row["host:thread"]) and not re.match("time-", row["host:thread"]):
                schema_row = dict(rsptimes_schema_to_results_map)
                for skey, svalue in schema_row.items():
                    for key, value in row.items():
                        if svalue == key:
                            schema_row[skey] = str(value)
                            #if skey == "host_thread":
                            #    schema_row[skey] = str(value)
                            #elif skey == "samples":
                            #    schema_row[skey] = int(value)
                            #else:
                            #    schema_row[skey] = float(value)
                            break
                smallfile_rsptimes.append(schema_row)

    #debug
    #print(smallfile_rsptimes)


    # Cleanup after run, if enabled
    if params.cleanup:
    #if params.cleanup == "True":
        print("==>> Cleaning up operation files ...")
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
            temp_cleanup(smallfile_yaml_file, smallfile_dir)
            temp_cleanup(smallfile_cleanup_yaml)
            return "error", WorkloadError("{} failed with return code {}:\n{}".format(error.cmd[0],error.returncode,error.output))
        temp_cleanup(smallfile_cleanup_yaml)

    temp_cleanup(smallfile_yaml_file, smallfile_dir)
    temp_cleanup(smallfile_out_file)

    print("==>> Workload run complete!")
    return "success", WorkloadResults(output_params_schema.unserialize(smallfile_json["params"]),output_results_schema.unserialize(smallfile_json["results"]),output_rsptimes_schema.unserialize(smallfile_rsptimes))



def temp_cleanup(file = [], directory = ""):
    # Cleanup our temp files
    print("==>> Cleaning up temporary files ...")
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
