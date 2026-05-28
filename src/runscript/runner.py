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
from runscript.logger import logger
from runscript.configure import read_config, ProgramConfig

_SCRIPT_HEADER = """\
#!/usr/bin/bash
set -e -o pipefail

cd "{outdir}"

"""

_SCRIPT_MPI_SETUP = """\
arg_mpi_transport=
arg_machinefile=

if [ -n "$LSB_MCPU_HOSTS" ]; then  # LSF
    arg_machinefile="-machinefile $LSB_DJOB_RANKFILE"
elif [ -n "$PBS_NODEFILE" ]; then  # PBS
    arg_machinefile="-machinefile $PBS_NODEFILE"
fi

# Check for possibly non-working RDMA transport
if command -v lsmod >/dev/null 2>&1 && lsmod | egrep -qw bnxt_re
then
    arg_mpi_transport="-mca btl vader,self,tcp -mca pml ^ucx"
fi

"""

_DEFAULT_LAUNCH_COMMAND = "{executable} {program_args}"

_SCRIPT_LAUNCH_MPI = (
    "{mpirun} $arg_mpi_transport $arg_machinefile {num_tasks} {mpi_args} {telemetry}"
    ' {launch_command} 1> >(tee "{outdir}/{case}.LOG")'
    ' 2> >(tee "{outdir}/{case}.ERR" 1>&2)\n'
)

_SCRIPT_LAUNCH_SERIAL = (
    '{telemetry} {launch_command} 1> >(tee "{outdir}/{case}.LOG")'
    ' 2> >(tee "{outdir}/{case}.ERR" 1>&2)\n'
)


HAVE_BSUB = shutil.which("bsub") is not None  # IBM LSF
HAVE_QSUB = shutil.which("qsub") is not None  # OpenPBS


CONFIG: ProgramConfig | None = None


def get_config() -> ProgramConfig:
    global CONFIG
    if CONFIG is None:
        CONFIG = read_config()
    return CONFIG


def ensure_local_on_hpc(args: Arguments) -> None:
    if args.queue != "local" and any(
        x in os.environ for x in ("LSB_DJOB_RANKFILE", "PBS_NODEFILE")
    ):
        args.queue = "local"
        args.num_tasks_per_machine = args.num_tasks_per_machine or 1


def get_max_allowed_cpu(requested: int | None = None) -> int:
    for env in ("LSB_DJOB_RANKFILE", "PBS_NODEFILE"):
        if (file_ := os.environ.get(env)) is None:
            continue

        hostfile_max = len(Path(file_).read_text().splitlines())
        return min(hostfile_max, requested or hostfile_max)

    machine_max = os.cpu_count() or 1
    return min(machine_max, requested or machine_max)


def make_print_version_action() -> type:
    class PrintVersionAction(argparse.Action):
        def __call__(self, *_args: Any) -> None:
            sys.exit(
                subprocess.call([get_config().executable_path, "--print-versions"])
            )

    return PrintVersionAction


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
    program_args: str
    output_directory: str | None
    interactive: bool

    telemetry: str | None = None
    bsub_args: str | None = None
    qsub_args: str | None = None
    exclusive: bool | None = None


def parse_args(argv: list[str]) -> Arguments:
    ap = argparse.ArgumentParser(
        prog=f"run{get_config().progname}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("input", help=f"{get_config().display_name} .in input file")
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
        help=f"Version of {get_config().display_name} to use",
    )
    ap.add_argument(
        "-o",
        "--output-directory",
        help="Directory to store the output to",
    )
    ap.add_argument(
        f"--{get_config().progname}-args",
        dest="program_args",
        help=f"Additional arguments for {get_config().display_name}",
    )
    ap.add_argument(
        "--mpi-args",
        help="Additional arguments for mpirun command",
    )
    ap.add_argument(
        "--telemetry",
        type=str,
        default="",
        help=f"Program to run between mpirun and {get_config().display_name}",
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
        action=make_print_version_action(),
        nargs=0,
        help=f"Output {get_config().display_name} versions and exit",
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
        f"{get_config().progname}_{input_file.name}",
        "-R",
        " ".join(resources),
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
        f"{get_config().progname}_{input_file.name}",
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
        sys.exit(
            f"{get_config().display_name} input file '{input_file}' does not exist!"
        )

    if input_file.is_dir():
        outdir_default = input_file
        case = input_file.name
    else:
        outdir_default = input_file.parent
        case = input_file.stem

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

    program_args = args.program_args or ""
    if args.version:
        program_args = "-v " + args.version + " " + program_args

    num_tasks = args.num_machines * args.num_tasks_per_machine

    if args.output_directory:
        outdir = Path(args.output_directory).expanduser()
    else:
        outdir = outdir_default

    common_fmt = dict(
        executable=get_config().executable_path,
        progname=get_config().progname,
        program_args=program_args,
        input_file=input_file,
        case=case,
        outdir=outdir.resolve(),
        telemetry=args.telemetry or "",
    )

    launch_command = get_config().launch_template.format(**common_fmt)

    pre_command_section = get_config().pre_command.format(**common_fmt) + "\n\n"

    script = (
        _SCRIPT_HEADER.format(**common_fmt)
        + pre_command_section
        + (_SCRIPT_MPI_SETUP + _SCRIPT_LAUNCH_MPI).format(
            **common_fmt,
            launch_command=launch_command,
            mpirun=get_config().mpirun_path,
            num_tasks=f"-np {num_tasks}",
            mpi_args=args.mpi_args or "",
        )
    )

    logger.info(
        "Start job",
        extra={
            "arg0": sys.argv[0],
            "args.version": str(args.version),
            "args.num_tasks_per_machine": args.num_tasks_per_machine,
            "args.num_machines": args.num_machines,
            "args.queue": args.queue,
            "executable": get_config().executable_path,
            "mpirun": get_config().mpirun_path,
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
