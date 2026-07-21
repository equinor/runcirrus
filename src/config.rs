use std::env;
use std::path::PathBuf;

pub fn karsksal_root() -> PathBuf {
    env::var_os("KARSKSAL_ROOT")
        .unwrap_or(env!("KARSKSAL_ROOT").into())
        .into()
}

pub fn wrapper_path() -> PathBuf {
    karsksal_root().join("bin/runcirrus")
}

/// Environment variable for "machinefile"/"hostfile" - a list of hosts with one
/// line per hostname
pub const MACHINEFILES: [&str; 2] = ["LSB_MCPU_HOSTS", "PBS_NODEFILE"];
