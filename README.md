# smallfile workload plugin for Arcaflow

arca-smallfile is a workload plugin of the [smallfile](https://github.com/distributed-system-analysis/smallfile) benchmark tool
using the [Arcaflow python SDK](https://github.com/arcalot/arcaflow-plugin-sdk-python).

Supported smallfile parameters are defined in the `SmallfileParams` schema of the [smallfile_plugin.py](smallfile_plugin.py) file.
You define your test parameters in a YAML file to be passed to the plugin command as shown in [smallfile-example.yaml](smallfile-example.yaml).

## To test:

In order to run the [smallfile plugin](smallfile_plugin.py) run the following steps:

1. Clone this repository
2. Create a `venv` in the current directory with `python3 -m venv $(pwd)/venv`
3. Activate the `venv` by running `source venv/bin/activate`
4. Run `pip install -r requirements.txt`
5. Run `./smallfile_plugin.py -f smallfile-example.yaml`

