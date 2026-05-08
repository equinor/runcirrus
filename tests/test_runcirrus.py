import os
import sys

import pytest
from runcirrus import runcirrus
from pathlib import Path


def test_get_install_path_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("CIRRUS_INSTALL_PATH", str(tmp_path))
    assert runcirrus.get_install_path() == tmp_path


def test_get_install_path_from_config(monkeypatch, tmp_path):
    monkeypatch.delenv("CIRRUS_INSTALL_PATH", raising=False)
    from runcirrus import configure

    monkeypatch.setattr(
        configure, "read_config", lambda: {"cirrus-install-path": str(tmp_path)}
    )
    assert runcirrus.get_install_path() == tmp_path


def test_get_install_path_env_overrides_config(monkeypatch, tmp_path):
    from runcirrus import configure

    monkeypatch.setattr(
        configure, "read_config", lambda: {"cirrus-install-path": str(tmp_path)}
    )
    assert runcirrus.get_install_path() == tmp_path

    env_path = tmp_path / "from_env"
    env_path.mkdir()
    monkeypatch.setenv("CIRRUS_INSTALL_PATH", str(env_path))

    assert runcirrus.get_install_path() == env_path


def test_configure_and_get_install_path(monkeypatch, tmp_path):
    monkeypatch.delenv("CIRRUS_INSTALL_PATH", raising=False)
    from runcirrus import configure

    config_file = tmp_path / "config.json"
    monkeypatch.setattr(configure, "CONFIG_PATH", config_file)

    install_dir = tmp_path / "cirrus_bin"
    install_dir.mkdir()
    monkeypatch.setattr(
        "sys.argv", ["runcirrus-configure", "--cirrus-install-path", str(install_dir)]
    )

    configure.main()

    assert config_file.exists()
    assert runcirrus.get_install_path() == install_dir


def test_get_install_path_exits_when_not_configured(monkeypatch):
    monkeypatch.delenv("CIRRUS_INSTALL_PATH", raising=False)
    from runcirrus import configure

    monkeypatch.setattr(configure, "read_config", lambda: {})
    with pytest.raises(SystemExit):
        runcirrus.get_install_path()


def test_get_max_allowed_cpu_with_no_hostfile_defined(monkeypatch):
    monkeypatch.setattr(os, "cpu_count", lambda: 1337)

    assert runcirrus.get_max_allowed_cpu() == 1337


@pytest.mark.parametrize(
    "name,num_cpus",
    [("LSB_DJOB_RANKFILE", 2), ("LSB_DJOB_RANKFILE", 4), ("PBS_NODEFILE", 8)],
)
def test_get_max_allowed_cpu(tmp_path, monkeypatch, name, num_cpus):
    hostfile_path = tmp_path / "hostfile"
    hostfile_path.write_text("host0\n" * num_cpus)
    monkeypatch.setenv(name, str(hostfile_path))

    assert runcirrus.get_max_allowed_cpu() == num_cpus


@pytest.mark.parametrize(
    "machinefile_cpu,user_specified_cpu,machine_cpu,expected_cpu",
    [
        pytest.param(3, 5, 8, 3, id="machinefile limits user"),
        pytest.param(5, 3, 8, 3, id="user limits machinefile"),
        pytest.param(5, None, 8, 5, id="machinefile limits default"),
        pytest.param(None, None, 8, 8, id="default is machine cpu"),
        pytest.param(None, 10, 8, 8, id="user cpu cannot exceed machine cpu"),
        pytest.param(None, 5, 8, 5, id="user cpu is respected when no machinefile"),
    ],
)
def test_num_cpu_precedence_is_correct(
    tmp_path,
    mocker,
    monkeypatch,
    machinefile_cpu,
    user_specified_cpu,
    machine_cpu,
    expected_cpu,
):
    monkeypatch.setattr(os, "cpu_count", lambda: machine_cpu)

    run_local = mocker.Mock()
    monkeypatch.setattr(runcirrus, "run_local", run_local)
    monkeypatch.setattr(runcirrus, "get_install_path", lambda: Path("/"))

    if machinefile_cpu is not None:
        hostfile_path = tmp_path / "hostfile"
        hostfile_path.write_text("host0\n" * machinefile_cpu)
        monkeypatch.setenv("LSB_DJOB_RANKFILE", str(hostfile_path))

    args = [
        "arg0",
        "--version",
        "dev",
        *(["-n", str(user_specified_cpu)] if user_specified_cpu is not None else []),
        "/dev/null",
    ]
    monkeypatch.setattr(sys, "argv", args)

    runcirrus.main()

    # Don't let the user-specified '-n' flag be greater than the limit set by hostfile
    assert run_local.call_args[0][1].num_tasks_per_machine == expected_cpu
