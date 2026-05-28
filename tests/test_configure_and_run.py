import sys

import pytest
from runscript import configure, runner


@pytest.fixture(autouse=True)
def _isolated_config(tmp_path, monkeypatch):
    monkeypatch.setattr(configure, "CONFIG_PATH", tmp_path / "_config.json")
    monkeypatch.setattr(sys, "executable", str(tmp_path / "bin" / "python"))
    monkeypatch.setattr(runner, "CONFIG", None)
    (tmp_path / "bin").mkdir()


def do_configure(progname, executable_path, monkeypatch):
    monkeypatch.setattr(runner, "CONFIG", None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "runscript-configure",
            "--progname",
            progname,
            "--display-name",
            progname,
            "--executable-path",
            executable_path,
            "--mpirun-path",
            "/usr/bin/mpirun",
        ],
    )
    configure.main()


def get_job_script(progname, input_file, monkeypatch, capsys):
    monkeypatch.setattr(
        sys, "argv", [f"run{progname}", "--print-job-script", str(input_file)]
    )
    runner.main()
    return capsys.readouterr().out


def test_configure_then_run(tmp_path, monkeypatch, capsys):
    do_configure("hello", "/usr/bin/hello", monkeypatch)

    input_file = tmp_path / "case.in"
    input_file.touch()

    output = get_job_script("hello", input_file, monkeypatch, capsys)
    assert "/usr/bin/hello" in output


def test_reconfigure_changes_executable(tmp_path, monkeypatch, capsys):
    input_file = tmp_path / "case.in"
    input_file.touch()

    do_configure("myapp", "/opt/first", monkeypatch)
    output = get_job_script("myapp", input_file, monkeypatch, capsys)
    assert "/opt/first" in output

    do_configure("otherapp", "/opt/second", monkeypatch)
    output = get_job_script("otherapp", input_file, monkeypatch, capsys)
    assert "/opt/second" in output
    assert "/opt/first" not in output


def test_reconfigure_removes_old_symlink(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"

    do_configure("oldapp", "/opt/old", monkeypatch)
    assert (bin_dir / "runoldapp").is_symlink()

    do_configure("newapp", "/opt/new", monkeypatch)
    assert (bin_dir / "runnewapp").is_symlink()
    assert not (bin_dir / "runoldapp").exists()
