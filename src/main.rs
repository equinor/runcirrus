mod config;
mod outer;
mod queue_system;
mod runner;
mod util;

use std::{env, ffi::OsString};

fn main() {
    if env::args_os().nth(1) == Some(OsString::from("--run")) {
        runner::main();
    } else {
        outer::main();
    }
}
