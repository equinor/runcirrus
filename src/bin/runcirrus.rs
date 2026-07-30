use runkarsk::config;

use runkarsk::common_args;
use runkarsk::queue_system::QueueSystem;
use runkarsk::spec::Spec;
use std::env;
use std::process::Command;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = common_args::parse(env!("CARGO_BIN_NAME"));
    let spec = Spec::new(args.input.unwrap(), args.version)?;

    let qs = QueueSystem::from_args(args.queue, args.num_tasks_per_machine, args.num_machines);

    let mpirun_path = spec.get_bin("mpirun").to_owned();
    let output_directory = args
        .output_directory
        .unwrap_or(spec.get_case_dir().to_owned());
    let num_tasks = qs.num_tasks();
    let mut command = Command::new(config::runner_path());

    command
        .arg("-C")
        .arg(output_directory)
        .arg("-c")
        .arg(spec.get_case_name())
        .arg("-n")
        .arg(num_tasks.to_string())
        .arg("-M")
        .arg(mpirun_path);

    command
        .arg(spec.get_bin("cirrus"))
        .arg("-cirrusin")
        .arg(spec.get_input());

    qs.exec(command).await;
    Ok(())
}
