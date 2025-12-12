# runcirrus

This is the script that we use to run OpenGoSim's Cirrus reservoir simulator. It does the following things:
- Run cirrus locally, and on the cluster. LSF and OpenPBS are supported for now.
- Can run any version of Cirrus that we have deployed.

## Using
This script operates on Cirrus .in files and enables you to simulate in
parallel. For example, given the "spe1.in" case, you can simply run it with the
following command:

    $ runcirrus spe1.in

This will use all available cores on your local machine, and output the
following files:

    'spe1.out': Text summarising the simulation
    'spe1-mas.dat':
    'spe1.INIT':
    'spe1.SMSPEC':
    'spe1.UNSMRY':

Additionally, runcirrus produces the following files:

    'spe1.LOG': Cirrus' "stdout" standard output
    'spe1.ERR': Cirrus' "stderr" standard error
    'spe1_bsub.LOG': Logs from the workflow manager when using IBM LSF

To utilise the HPC cluster, specify '-q' (aka. '--queue'). In this
configuration, only 1 CPU will be utilised by default. To change this behaviour,
use the '-n' and '-m' options. '-n' is "number of tasks per machine" and '-m' is
"number of machines".

For example, to add a job to the 'bigmem' queue using 2 machines (nodes) and 8
processes per machine for a total of 16 cores, use:

    $ runcirrus -q bigmem -n 8 -m 2 spe1.in

## Building
This project uses Python with [uv](https://docs.astral.sh/uv/).

## Testing
This project uses Pytest. After installing a development version, `pytest` is available. Tests can be run using `pytest tests/`.
