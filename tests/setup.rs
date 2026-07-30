use std::path::{Path, PathBuf};
use std::process::Command;

fn copy_dir<P1, P2>(from: P1, to: P2)
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    let from = from.as_ref();
    let to = to.as_ref();
    std::fs::create_dir_all(to)
        .unwrap_or_else(|err| panic!("Error creating directory '{}': {}", to.display(), err));

    for entry in std::fs::read_dir(from)
        .unwrap_or_else(|err| panic!("Error reading directory '{}': {}", from.display(), err))
    {
        let entry = entry.unwrap_or_else(|err| {
            panic!(
                "Couldn't read file while reading directory '{}': {}",
                from.display(),
                err
            )
        });
        let from_path = entry.path();
        let to_path = to.join(entry.file_name());

        if from_path.is_symlink() {
            let target = std::fs::read_link(&from_path).unwrap_or_else(|err| {
                panic!("Error reading symlink '{}': {}", from_path.display(), err)
            });
            std::os::unix::fs::symlink(&target, &to_path).unwrap_or_else(|err| {
                panic!(
                    "Error creating symlink '{}' -> '{}': {}",
                    to_path.display(),
                    target.display(),
                    err
                )
            });
        } else if from_path.is_dir() {
            copy_dir(from_path, to_path);
        } else {
            std::fs::copy(&from_path, &to_path).unwrap_or_else(|err| {
                panic!(
                    "Error copying file '{}' to '{}': {}",
                    from_path.display(),
                    to_path.display(),
                    err
                )
            });
        }
    }
}

pub fn copy_test_data_to<P: AsRef<Path>>(to: P) {
    let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    copy_dir(data_dir, to);
}

#[allow(dead_code)]
pub fn bin_runner() -> Command {
    Command::new(env!("CARGO_BIN_EXE_runner"))
}

#[allow(dead_code)]
pub fn bin_runcirrus() -> Command {
    Command::new(env!("CARGO_BIN_EXE_runcirrus"))
}
