use crate::config;
use crate::util::have_linux_module;

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;

use clap::Parser;
use std::process::Command as StdCommand;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::Command;

#[derive(Parser, Debug)]
#[command(name = env!("CARGO_BIN_NAME"))]
pub struct Args {
    /// Prefix to the exact installation (not Karsk root)
    #[arg(long)]
    pub prefix: PathBuf,

    /// This argument doesn't do anything, but is needed for Clap to accept the
    /// arguments. In the actual program main, `--run` distinguishes between
    /// having to run this file's main or the "outer" main
    #[arg(long, hide = true)]
    pub run: bool,

    #[arg(long)]
    pub cwd: PathBuf,

    #[arg(long)]
    pub case: String,

    #[arg(long)]
    pub num_tasks: usize,

    #[arg(long)]
    pub mpirun_path: PathBuf,

    #[arg(long)]
    pub dry_run: bool,

    pub program_args: Vec<OsString>,
}

fn build_command(args: &Args) -> Command {
    let mut command = Command::new(args.mpirun_path.clone());
    command
        .current_dir(&args.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if have_linux_module("bnxt_re") {
        command.args(["-mca", "btl", "vader,self,tcp", "-mca", "pml", "^ucx"]);
    }

    if let Some(machinefile) = config::MACHINEFILES.iter().find_map(std::env::var_os) {
        command.arg("-machinefile").arg(machinefile);
    } else {
        command.arg("-np").arg(args.num_tasks.to_string());
    }

    command.args(&args.program_args);
    command
}

/// Pretty-print the command that we're about to execute. We pretend like we're
/// using /usr/bin/env for things like setting environment variables.
fn print_command(cmd: StdCommand) {
    print!("/usr/bin/env");

    if let Some(cwd) = cmd.get_current_dir() {
        print!(" -C{} ", cwd.display());
    }

    for (key, val) in cmd.get_envs() {
        match val {
            Some(x) => print!(" {}={}", key.display(), x.display()),
            None => print!(" -u{}", key.display()),
        }
    }

    print!("-- {}", cmd.get_program().display());
    for arg in cmd.get_args() {
        print!(" {}", arg.display());
    }
    println!();
}

async fn tee<R, W1, W2>(mut r: R, mut w1: W1, mut w2: W2)
where
    R: AsyncRead + Unpin,
    W1: AsyncWrite + Unpin,
    W2: AsyncWrite + Unpin,
{
    let mut buf = [0u8; 8192];

    loop {
        match r.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            _ => {}
        };

        // We ignore errors when writing
        let _ = w1.write_all(&buf).await;
        let _ = w2.write_all(&buf).await;
    }

    let _ = w1.flush().await;
    let _ = w2.flush().await;
}

#[tokio::main]
pub async fn main_with_args(args: Args) {
    let mut command = build_command(&args);

    let _ = std::fs::create_dir_all(&args.cwd);

    if args.dry_run {
        print_command(command.into_std());
        return;
    }

    let mut child = command.spawn().expect("Could not start job");

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    let stdout_file = File::create(args.cwd.join(format!("{}.LOG", args.case)))
        .await
        .expect("Couldn't create");
    let stderr_file = File::create(args.cwd.join(format!("{}.ERR", args.case)))
        .await
        .expect("Couldn't create");

    let stdout_task = tokio::task::spawn(tee(stdout, stdout_file, tokio::io::stdout()));
    let stderr_task = tokio::task::spawn(tee(stderr, stderr_file, tokio::io::stderr()));

    let status = child.wait().await.unwrap();
    stdout_task.await.unwrap();
    stderr_task.await.unwrap();

    std::process::exit(status.code().unwrap_or_default());
}

pub fn main() {
    let args = Args::parse();
    main_with_args(args);
}
