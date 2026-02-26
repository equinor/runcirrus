import os
import sys

import pytest
from runcirrus import runcirrus
from pathlib import Path


def test_printversions_empty_folder(capsys, tmp_path, monkeypatch):
    monkeypatch.setenv("CIRRUS_VERSIONS_PATH", str(tmp_path))
    with pytest.raises(SystemExit):
        runcirrus.parse_args(["0", "--print-versions"])

    assert capsys.readouterr().out == f"No installed versions found at {tmp_path}\n"


@pytest.mark.parametrize(
    "dirs,expected_out", [(["1.10"], "1.10\n"), (["1.10", ".store"], "1.10\n")]
)
def test_printversions_ignore_hidden(dirs, expected_out, capsys, tmp_path, monkeypatch):
    for name in dirs:
        (tmp_path / name).mkdir()
    monkeypatch.setenv("CIRRUS_VERSIONS_PATH", str(tmp_path))

    with pytest.raises(SystemExit):
        runcirrus.parse_args(["0", "--print-versions"])

    assert capsys.readouterr().out == expected_out


@pytest.mark.parametrize(
    "script_name,expect",
    [
        ("runcirrus", "stable"),
        ("runpflotran", "1.8"),
        ("runpflotran1.8.12", "1.8.12"),
        ("runpflotran1.8-openpbs-rh8", "1.8"),
    ],
)
def test_default_version(script_name, expect):
    assert runcirrus.default_version(script_name) == expect, f"{script_name=}"


@pytest.mark.parametrize(
    "dir,expected_out",
    [
        (
            "cirrus/versions/.store/d312321-runcirrus-1.0.0/bin/runcirrus",
            "cirrus/versions",
        ),
        ("cirrus/versions/.store/further_up", "cirrus/versions"),
    ],
)
def test_get_versions_path_correctly_identifies_location(
    dir, expected_out, monkeypatch
):
    def mockreturn(_):
        return dir

    monkeypatch.setattr(os.path, "dirname", mockreturn)

    assert str(runcirrus.get_versions_path()).endswith(expected_out)


def test_get_versions_path_correctly_identifies_symlinked_location(
    tmp_path, monkeypatch
):
    script_path = (
        tmp_path / "cirrus/versions/.store/d312321-runcirrus-1.0.0/bin/runcirrus"
    )
    script_path.mkdir(parents=True)

    destination = tmp_path / "bin/runcirrus"
    destination.parent.mkdir()
    os.symlink(script_path, destination)

    def mockreturn(_):
        return destination

    monkeypatch.setattr(os.path, "dirname", mockreturn)
    assert str(runcirrus.get_versions_path()).endswith("")


def test_get_versions_path_correctly_fails(capsys, monkeypatch):
    def mockreturn(_):
        # the important thing is that "versions" does not exist in path
        return "/some/example/path"

    monkeypatch.setattr(os.path, "dirname", mockreturn)

    with pytest.raises(RuntimeError) as err:
        runcirrus.parse_args(["0", "--print-versions"])

    assert err.match("Not able to locate install location from /some/example/path")


def test_get_max_allowed_cpu_with_no_hostfile_defined(monkeypatch):
    monkeypatch.setattr(os, "cpu_count", lambda: 1337)

    assert runcirrus.get_max_allowed_cpu() == 1337


@pytest.mark.parametrize("name,num_cpus", [("LSB_DJOB_RANKFILE", 2), ("LSB_DJOB_RANKFILE", 4), ("PBS_NODEFILE", 8)])
def test_get_max_allowed_cpu(tmp_path, monkeypatch, name, num_cpus):
    hostfile_path = tmp_path / "hostfile"
    hostfile_path.write_text("host0\n" * num_cpus)
    monkeypatch.setenv(name, str(hostfile_path))

    assert runcirrus.get_max_allowed_cpu() == num_cpus


def test_num_cpu_precedence_correct_nei_jeg_vet_ikke(tmp_path, mocker, monkeypatch):
    run_local = mocker.Mock()
    monkeypatch.setattr(runcirrus, "run_local", run_local)
    monkeypatch.setattr(runcirrus, "get_versions_path", lambda: Path("/"))

    hostfile_path = tmp_path / "hostfile"
    hostfile_path.write_text("host0" * 7)
    monkeypatch.setenv("LSB_DJOB_RANKFILE", str(hostfile_path))

    monkeypatch.setattr(sys, "argv", ["arg0", "--version", "dev", "-n", "13", "/dev/null"])

    runcirrus.main()

    assert run_local.call_args[0][1].num_tasks_per_machine == 13
