#!/usr/bin/python3.11
"""Wrapper for running Cirrus with MPI in Equinor.


This script operates on Cirrus .in files and enables you to simulate in
parallell. For example, given the "spe1.in" case, you can simply run it with the
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

"""

from __future__ import annotations
import sys
from typing import NoReturn, Any
import os
import argparse
import shutil
import shlex
import subprocess
from pathlib import Path
from dataclasses import dataclass
from runcirrus.logger import logger


SCRIPT = """\
#!/usr/bin/bash
set -e -o pipefail

cd "{outdir}"

arg_mpi_transport=
arg_machinefile=

if [ -n "$LSB_MCPU_HOSTS" ]; then  # LSF
    arg_machinefile="-machinefile $LSB_DJOB_RANKFILE"
elif [ -n "$PBS_NODEFILE" ]; then  # PBS
    arg_machinefile="-machinefile $PBS_NODEFILE"
fi

# Check for possibly non-working RDMA transport
if lsmod | egrep -qw bnxt_re
then
    arg_mpi_transport="-mca btl vader,self,tcp -mca pml ^ucx"
fi

({root}/bin/mpirun $arg_mpi_transport $arg_machinefile {num_tasks} {mpi_args} {telemetry} {root}/bin/cirrus {cirrus_args} -cirrusin "{input_file}" -output_prefix "{outdir}/{case}" | tee "{outdir}/{case}.LOG") 3>&1 1>&2 2>&3 | tee "{outdir}/{case}.ERR"
"""


HAVE_BSUB = shutil.which("bsub") is not None  # IBM LSF
HAVE_QSUB = shutil.which("qsub") is not None  # OpenPBS


def ensure_local_on_hpc(args: Arguments) -> None:
    """
    If we're running on the cluster alrea, override queue to local and set
    num tasks to 1.
    """
    if args.queue != "local" and any(
        x in os.environ for x in ("LSB_DJOB_RANKFILE", "PBS_NODEFILE")
    ):
        args.queue = "local"
        args.num_tasks_per_machine = args.num_tasks_per_machine or 1


def get_max_allowed_cpu(requested: int | None = None) -> int:
    """Determine maximum CPUs available, constrained by environment.

    Precedence: 1) cluster allocation (LSB/PBS) 2) machine CPUs 3) requested value.
    If requested exists, return minimum of its value and any other limits.
    Requested can only reduce other limits, never increase them.
    """
    for env in ("LSB_DJOB_RANKFILE", "PBS_NODEFILE"):
        if (file_ := os.environ.get(env)) is None:
            continue

        hostfile_max = len(Path(file_).read_text().splitlines())
        return min(hostfile_max, requested or hostfile_max)

    machine_max = os.cpu_count() or 1
    return min(machine_max, requested or machine_max)


def get_install_path() -> Path:
    if (path := os.environ.get("CIRRUS_INSTALL_PATH")) is not None:
        return Path(path).expanduser()

    from runcirrus.configure import read_config

    config = read_config()
    if configured_path := config.get("cirrus-install-path"):
        return Path(configured_path).expanduser()

    sys.exit(
        "Cirrus install path not configured. Run:\n"
        "  runcirrus-configure --cirrus-install-path /path/to/cirrus/\n"
        "or set the CIRRUS_INSTALL_PATH environment variable."
    )


class PrintVersionAction(argparse.Action):
    def __call__(self, *_args: Any) -> None:
        install_path = get_install_path()
        sys.exit(
            subprocess.call([str(install_path / "bin" / "cirrus"), "--print-versions"])
        )


@dataclass
class Arguments:
    input: str
    queue: str
    num_tasks_per_machine: int
    num_machines: int
    version: str
    print_job_script: bool
    print_versions: bool
    mpi_args: str
    cirrus_args: str
    output_directory: str | None
    interactive: bool

    telemetry: str | None = None
    bsub_args: str | None = None
    qsub_args: str | None = None
    exclusive: bool | None = None


def parse_args(argv: list[str]) -> Arguments:
    ap = argparse.ArgumentParser(
        prog="runcirrus",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("input", help="Cirrus .in input file")
    ap.add_argument(
        "-q", "--queue", default="local", help="Job queue, or 'local' to run locally"
    )
    ap.add_argument(
        "-n",
        "--num-tasks-per-machine",
        type=int,
        help="Number of tasks/processes per machine",
    )
    ap.add_argument(
        "-m",
        "--num-machines",
        default=1,
        type=int,
        help="Number of machines (nodes)",
    )
    ap.add_argument("-i", "--interactive", action="store_true", help="Run locally")
    ap.add_argument(
        "-v",
        "--version",
        help="Version of Cirrus to use",
    )
    ap.add_argument(
        "-o",
        "--output-directory",
        help="Directory to store the output to",
    )
    ap.add_argument(
        "--cirrus-args",
        help="Additional arguments for Cirrus",
    )
    ap.add_argument(
        "--mpi-args",
        help="Additional arguments for mpirun command",
    )
    ap.add_argument(
        "--telemetry",
        type=str,
        default="",
        help="Program to run between mpirun and Cirrus",
    )
    if HAVE_BSUB:
        ap.add_argument("--bsub-args", help="Additional arguments for bsub command")
    if HAVE_QSUB:
        ap.add_argument("--qsub-args", help="Additional arguments for qsub command")
        ap.add_argument(
            "-e", "--exclusive", help="Exclusive node usage [default: shared]"
        )
    ap.add_argument(
        "--print-job-script",
        action="store_true",
        help="Output job script and exit",
    )
    ap.add_argument(
        "--print-versions",
        action=PrintVersionAction,
        nargs=0,
        help="Output Cirrus versions and exit",
    )
    return Arguments(**vars(ap.parse_args(argv[1:])))


def run(program: str, *args: str) -> NoReturn:
    print(f"{program} {shlex.join(args[:-1])} <SCRIPT>")
    status = subprocess.run([program, *args])
    sys.exit(status.returncode)


def run_local(script: str, args: Arguments) -> NoReturn:
    run(
        "bash",
        "-c",
        script,
    )


def run_bsub(script: str, args: Arguments, input_file: Path) -> NoReturn:
    num_tasks = args.num_machines * args.num_tasks_per_machine

    resources = ["select[rhel >= 8]", "same[type:model]"]
    resources.append(f"span[ptile={args.num_tasks_per_machine}]")
    resource_string = " ".join(resources)

    user_args = shlex.split(args.bsub_args or "")

    script_path = input_file.parent / f"{input_file.stem}.run"
    script_path.write_text(script, encoding="utf-8")

    run(
        "bsub",
        "-q",
        args.queue,
        "-n",
        str(num_tasks),
        "-o",
        f"{input_file.parent}/{input_file.stem}_bsub.LOG",
        "-J",
        f"Cirrus_{input_file.name}",
        "-R",
        resource_string,
        *user_args,
        "--",
        "bash",
        str(script_path),
    )


def run_qsub(script: str, args: Arguments, input_file: Path) -> NoReturn:
    place = "scatter:excl" if args.exclusive else "scatter:shared"

    user_args = shlex.split(args.qsub_args or "")

    script_path = input_file.parent / f"{input_file.stem}.run"
    script_path.write_text(script, encoding="utf-8")

    run(
        "qsub",
        "-q",
        args.queue,
        "-l",
        f"select={args.num_machines}:ncpus={args.num_tasks_per_machine}:mpiprocs={args.num_tasks_per_machine}",
        "-l",
        f"place={place}",
        "-j",
        "oe",
        "-o",
        f"{input_file.parent}/{input_file.stem}_qsub.LOG",
        "-N",
        f"Cirrus_{input_file.name}",
        *user_args,
        "--",
        "/usr/bin/bash",
        "-c",
        script,
    )


def main() -> None:
    argv = []
    for arg in sys.argv:
        if arg == "-nn":
            argv.append("-m")
        elif arg == "-nm":
            argv.append("-n")
        else:
            argv.append(arg)

    args = parse_args(argv)

    input_file = Path(args.input).expanduser().resolve()
    if not input_file.exists():
        sys.exit(f"Cirrus input file '{input_file}' does not exit!")

    if args.interactive:
        args.queue = "local"

    ensure_local_on_hpc(args)

    if args.queue != "local" and args.num_tasks_per_machine is None:
        sys.exit(
            "Must specify -n/--num-tasks-per-machine when running on a non-local queue"
        )

    if args.queue == "local":
        args.num_tasks_per_machine = get_max_allowed_cpu(args.num_tasks_per_machine)

    if args.num_machines > 1 and args.queue == "local":
        sys.exit(
            "Must specify -q/--queue when attempting to run on multiple machines with -m/--num-machines"
        )

    cirrus_args = args.cirrus_args or ""
    if args.version:
        cirrus_args = "-v " + args.version + " " + cirrus_args

    install_path = get_install_path()

    progname = "cirrus"

    num_tasks = args.num_machines * args.num_tasks_per_machine

    if args.output_directory:
        outdir = Path(args.output_directory).expanduser()
    else:
        outdir = Path(args.input).expanduser().parent

    script = SCRIPT.format(
        root=install_path,
        workdir=input_file.parent,
        input_file=input_file,
        case=input_file.stem,
        progname=progname,
        mpi_args=args.mpi_args or "",
        cirrus_args=cirrus_args,
        num_tasks=f"-np {num_tasks}" if num_tasks is not None else "",
        outdir=outdir.resolve(),
        telemetry=args.telemetry,
    )

    logger.info(
        "Start job",
        extra={
            "arg0": sys.argv[0],
            "args.version": str(args.version),
            "args.num_tasks_per_machine": args.num_tasks_per_machine,
            "args.num_machines": args.num_machines,
            "args.queue": args.queue,
            "rootdir": str(install_path),
            "num_tasks": num_tasks,
            "bsub": HAVE_BSUB,
            "qsub": HAVE_QSUB,
        },
    )

    if args.print_job_script:
        print(script)
    elif args.queue == "local":
        run_local(script, args)
    elif HAVE_BSUB:
        run_bsub(script, args, input_file)
    elif HAVE_QSUB:
        run_qsub(script, args, input_file)
    else:
        sys.exit("No supported job scheduler detected on this machine")


if __name__ == "__main__":
    main()
