use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

use clap::Parser;

#[derive(Parser)]
#[command(name = env!("CARGO_BIN_NAME"))]
pub struct Args {
    outdir: PathBuf,
    case: String,

    num_tasks: i32,

    mpirun_path: PathBuf,
    mpi_args: Vec<OsString>,
    args: Vec<OsString>,
}

async fn write_from_process<R, W1, W2>(mut reader: R, mut w1: W1, mut w2: W2) -> std::io::Result<()>
where R: AsyncRead + Unpin,
      W1: AsyncWrite + Unpin,
W2: AsyncWrite + Unpin,
{
    let mut buf = [0u8; 8192];

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        w1.write_all(&buf[..n]).await?;
        w2.write_all(&buf[..n]).await?;
    }
    w1.flush().await?;
    w2.flush().await?;

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let mut child = Command::new(args.mpirun_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("It should work");

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    let stdout_file = File::create("stdout.log").await?;
    let stderr_file = File::create("stderr.log").await?;

    let stdout_task = tokio::task::spawn(write_from_process(stdout, stdout_file, tokio::io::stdout()));
    let stderr_task = tokio::task::spawn(write_from_process(stderr, stderr_file, tokio::io::stderr()));

    child.wait().await?;
    stdout_task.await??;
    stderr_task.await??;

    Ok(())
}
