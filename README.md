# RunKarsk

MPI wrapper for running software deployed by [Karsk](https://github.com/equinor/karsk) on HPC clusters. Supports local execution, IBM LSF, and OpenPBS.

## Building

This program is written in Rust and uses build-time environment variables to specify how to run the software:

- `KARSKAL_ROOT`: The path to the "karsksal" location, ie. a Karsk config's destination path. Eg: `/opt/karsk/cirrus`.
- `RUNKARSK_EXECUTABLE`: The executable to run inside of an installation's `bin` directory using `mpirun`
- `RUNKARSK_SETUP_COMMAND` (optional): A shell command to run before `mpirun`. Available formatting options are `{case}`, which is replaced with th

### Cirrus

For [Cirrus](https://opengosim.com/cirrus), set the following environment variables, in addition to `KARSKAL_ROOT`:

- `RUNKARSK_EXECUTABLE`: `cirrus`

### PFLOTRAN

For [PFLOTRAN](https://pflotran.org/), set the following environment variables, in addition to `KARSKAL_ROOT`:

- `RUNKARSK_EXECUTABLE`: `pflotran`

### PHAST

For [PHAST](https://www.usgs.gov/software/phast-a-computer-program-simulating-groundwater-flow-solute-transport-and-multicomponent), set the following environment variables, in addition to `KARSKAL_ROOT`:

- `RUNKARSK_EXECUTABLE`: `phast-mpi`
- `RUNKARSK_SETUP_COMMAND`: `phastinput {case} {program_args}`

## Using

All of our programs use a `.in` input file, which can be run like so:

    $ runccs spe1.in

This will run the `stable` version of the wrapped program using its `mpirun` and redirect the STDOUT and STDERR to the respective files, in this case: `spe1.LOG` and `spe1.ERR`.

To submit to the HPC cluster, specify `-q`/`--queue`. Use `-n`/`--num-tasks-per-machine` and `-m`/`--num-machines` to control parallelism. In the following case, the wrapped program will input `spe1.in` and run on 2 hosts using 8 MPI processes per machine for a total of 16 processes on the "bigmem" queue. The correct queue system is automatically detected by determining which of `bsub` (IBM LSF) or `qsub` (PBS) are available.

    $ runccs -q bigmem -n 8 -m 2 spe1.in
