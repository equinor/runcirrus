use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;

macro_rules! exit {
    (code=$code:expr, $($arg:tt)*) => {{
        std::eprint!("{}: ", env!("CARGO_BIN_NAME"));
        std::eprintln!($($arg)*);
        std::process::exit($code);
    }};

    ($($arg:tt)*) => {
        exit!(code=1, $($arg)*)
    };
}

/// Check if a Linux kernel module is loaded. Return
pub fn have_linux_module(name: &str) -> bool {
    if cfg!(target_os = "linux") {
        let file = File::open("/proc/modules").expect("Couldn't open /proc/modules for reading");
        let reader = BufReader::new(file);

        return reader
            .lines()
            .map_while(Result::ok)
            .any(|line| line.split_whitespace().next() == Some(name));
    }

    false
}

pub(crate) use exit;
