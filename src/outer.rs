use std::ffi::{OsStr, OsString};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, fs};

use clap::Parser;

use crate::config;
use crate::queue_system::QueueSystem;
use crate::runner;
use crate::util::exit;

#[derive(Parser)]
#[command(name = env!("CARGO_PKG_NAME"))]
pub struct Args {
    /// Cirrus .in input file
    #[arg(required_unless_present = "print_versions")]
    pub input: Option<PathBuf>,

    /// Job queue, or 'local' to run on this machine
    #[arg(short, long, default_value = "local")]
    pub queue: String,

    /// Number of tasks/processes per machine
    #[arg(short = 'n', long)]
    pub num_tasks_per_machine: Option<usize>,

    /// Number of machines (nodes)
    #[arg(short = 'm', long, default_value_t = 1)]
    pub num_machines: usize,

    /// Use 'local' job queue
    #[arg(short, long)]
    pub interactive: bool,

    /// Version of Cirrus to use
    #[arg(short = 'v', long)]
    pub version: Option<String>,

    /// Directory to store the output to
    #[arg(short = 'o', long)]
    pub output_directory: Option<PathBuf>,

    /// Exclusive node usage [default: shared]
    #[arg(short = 'e', long)]
    pub exclusive: bool,

    /// Output Cirrus versions and exit
    #[arg(long)]
    pub print_versions: bool,

    /// Print the command that would be executed
    #[arg(long)]
    pub dry_run: bool,
}

fn converted_deprecated_args() -> Vec<OsString> {
    env::args_os()
        .map(|arg| match arg.to_str() {
            Some("-nm") => OsString::from("-n"),
            Some("-nn") => OsString::from("-m"),
            _ => arg,
        })
        .collect()
}

fn split_input_into_dir_and_case(input_file: &Path) -> (PathBuf, String) {
    let name = |s: Option<&OsStr>| {
        s.and_then(OsStr::to_str)
            .expect("Couldn't get file name from path")
            .to_string()
    };

    if input_file.is_dir() {
        (input_file.to_path_buf(), name(input_file.file_name()))
    } else {
        (
            input_file.parent().unwrap().to_owned(),
            name(input_file.file_stem()),
        )
    }
}

pub fn get_install_root(version: String) -> PathBuf {
    let versions_dir = config::karsksal_root().join("versions");
    let path = versions_dir.join(&version);
    fs::canonicalize(path).unwrap_or_else(|err| {
        eprintln!("Error finding version '{}'", version);
        eprintln!("Looked in '{}'", versions_dir.display());
        eprintln!(
            "Use `{} --print-versions` to see a list of valid versions",
            env!("CARGO_BIN_NAME")
        );
        exit!("Error: {}", err);
    })
}

pub fn main() {
    let args = Args::parse_from(converted_deprecated_args());

    if args.print_versions {
        let err = Command::new(config::wrapper_path())
            .arg("--print-versions")
            .exec();
        exit!(
            code = 127,
            "Failed to run {}: {}",
            config::wrapper_path().display(),
            err
        );
    }

    let input_file = args
        .input
        .map(|input| {
            fs::canonicalize(&input)
                .unwrap_or_else(|e| exit!("Couldn't use input '{}': {}", input.display(), e))
        })
        .unwrap();

    let (outdir_default, case) = split_input_into_dir_and_case(&input_file);

    let cwd = args.output_directory.unwrap_or(outdir_default);
    let install_root = get_install_root(args.version.unwrap_or(String::from("stable")));
    let program_args: Vec<OsString> = Vec::from([
        install_root.join("bin/cirrus").into_os_string(),
        OsString::from("-output_prefix"),
        cwd.clone().into_os_string(),
        OsString::from("-cirrusin"),
        input_file.into_os_string(),
    ]);

    match QueueSystem::from_args(args.queue, args.num_tasks_per_machine, args.num_machines) {
        QueueSystem::Local {
            num_tasks_per_machine,
        } => {
            runner::main_with_args(runner::Args {
                run: true,

                prefix: PathBuf::default(),
                cwd,
                case,
                num_tasks: num_tasks_per_machine,
                mpirun_path: install_root.join("bin/mpirun"),
                program_args,
                dry_run: args.dry_run,
            });
        }
        _ => exit!("Queue system not implemented"),
    }
}
