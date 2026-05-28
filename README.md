# runscript

Generic MPI job wrapper for running programs on HPC clusters. Supports local execution, IBM LSF, and OpenPBS.

## Setup

Register a program with its paths:

    $ runscript-configure --progname myprog --executable-path /path/to/myprog --mpirun-path /path/to/mpirun

This creates a `runmyprog` wrapper script alongside `runscript-configure`.

Optional flags:
- `--display-name` — human-readable name (defaults to progname)
- `--launch-template` — custom launch command template
- `--pre-command` — command to run before the main launch

## Using

Run a registered program against an input file:

    $ runmyprog spe1.in

This uses all available cores on the local machine. Output files:

    spe1.LOG: program stdout
    spe1.ERR: program stderr

To submit to the HPC cluster, specify `-q` (`--queue`). Use `-n` (tasks per machine) and `-m` (number of machines) to control parallelism:

    $ runmyprog -q bigmem -n 8 -m 2 spe1.in

## Building
This project uses Python with [uv](https://docs.astral.sh/uv/).

## Testing
This project uses Pytest. After installing a development version, `pytest` is available. Tests can be run using `pytest tests/`.
