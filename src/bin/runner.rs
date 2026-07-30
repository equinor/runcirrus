use runkarsk::config;
use runkarsk::util::have_linux_module;

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;

use clap::Parser;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::Command;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short = 'C')]
    output_directory: PathBuf,

    #[arg(short = 'c')]
    case: String,

    #[arg(short = 'n')]
    num_tasks: usize,

    #[arg(short = 'M')]
    mpirun_path: PathBuf,

    #[arg(trailing_var_arg = true, allow_hyphen_values = true, num_args = 1..)]
    program_args: Vec<OsString>,
}

fn build_command(args: &Args) -> Command {
    let mut command = Command::new(args.mpirun_path.clone());
    command
        .current_dir(&args.output_directory)
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

async fn tee<R, W1, W2>(mut r: R, mut w1: W1, mut w2: W2)
where
    R: AsyncRead + Unpin,
    W1: AsyncWrite + Unpin,
    W2: AsyncWrite + Unpin,
{
    let mut buf = [0u8; 8192];

    loop {
        let nbytes = match r.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };

        // We ignore errors when writing
        let _ = w1.write_all(&buf[..nbytes]).await;
        let _ = w2.write_all(&buf[..nbytes]).await;
    }

    let _ = w1.flush().await;
    let _ = w2.flush().await;
}

#[tokio::main]
pub async fn main() {
    let args = Args::parse();
    let mut command = build_command(&args);

    let _ = std::fs::create_dir_all(&args.output_directory);

    let mut child = command.spawn().expect("Could not start job");

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    let stdout_file = File::create(args.output_directory.join(format!("{}.LOG", args.case)))
        .await
        .expect("Couldn't create");
    let stderr_file = File::create(args.output_directory.join(format!("{}.ERR", args.case)))
        .await
        .expect("Couldn't create");

    let stdout_task = tokio::task::spawn(tee(stdout, stdout_file, tokio::io::stdout()));
    let stderr_task = tokio::task::spawn(tee(stderr, stderr_file, tokio::io::stderr()));

    let status = child.wait().await.unwrap();
    stdout_task.await.unwrap();
    stderr_task.await.unwrap();

    std::process::exit(status.code().unwrap_or_default());
}
