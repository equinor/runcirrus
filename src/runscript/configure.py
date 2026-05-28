import dataclasses
import argparse
import json
import sys
from pathlib import Path

CONFIG_PATH = Path(__file__).parent / "_config.json"

_DEFAULT_LAUNCH_COMMAND = "{executable} {program_args}"


@dataclasses.dataclass
class ProgramConfig:
    progname: str
    launch_template: str
    display_name: str
    executable_path: str
    pre_command: str
    mpirun_path: str


def read_config() -> ProgramConfig:
    return ProgramConfig(**json.loads(CONFIG_PATH.read_text()))


def remove_old_symlink(bin_dir: Path) -> None:
    if CONFIG_PATH.exists():
        old_config = ProgramConfig(**json.loads(CONFIG_PATH.read_text()))
        old_wrapper = bin_dir / f"run{old_config.progname}"
        old_wrapper.unlink(missing_ok=True)


def create_symlink(progname: str, bin_dir: Path) -> None:
    wrapper_path = bin_dir / f"run{progname}"
    wrapper_path.unlink(missing_ok=True)
    wrapper_path.symlink_to("runscript")


def main() -> None:
    ap = argparse.ArgumentParser(
        prog="runscript-configure",
        description="Configure runscript installation settings",
    )
    ap.add_argument(
        "--progname",
        metavar="NAME",
        help="Program name; creates a run<name> wrapper script alongside this tool",
        required=True,
    )
    ap.add_argument(
        "--display-name",
        metavar="NAME",
        help="Program name; ",
    )
    ap.add_argument(
        "--executable-path",
        metavar="PATH",
        help="Path to the program binary",
        required=True,
    )
    ap.add_argument(
        "--launch-template",
        metavar="TEMPLATE",
        default=_DEFAULT_LAUNCH_COMMAND,
        help="Command template for launching the program. "
        "Available variables: {executable}, {program_args}, {input_file}, "
        "{outdir}, {case}, {progname}",
    )
    ap.add_argument(
        "--mpirun-path",
        metavar="PATH",
        help="Path to the mpirun or mpiexec executable",
        required=True,
    )
    ap.add_argument(
        "--pre-command",
        metavar="TEMPLATE",
        default="",
        help="Command to run before the main launch (serial). "
        "Available variables: {executable}, {program_args}, {input_file}, "
        "{outdir}, {case}, {progname}",
    )
    args = ap.parse_args()

    if args.display_name is None:
        args.display_name = args.progname

    bin_dir = Path(sys.executable).parent
    remove_old_symlink(bin_dir)
    create_symlink(args.progname, bin_dir)

    config = {
        "progname": args.progname,
        "launch_template": args.launch_template,
        "display_name": args.display_name,
        "executable_path": args.executable_path,
        "pre_command": args.pre_command,
        "mpirun_path": args.mpirun_path,
    }

    CONFIG_PATH.write_text(json.dumps(config, indent=2) + "\n")
