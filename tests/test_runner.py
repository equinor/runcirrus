import os
import sys

import pytest
from runscript import runner


def test_get_max_allowed_cpu_with_no_hostfile_defined(monkeypatch):
    monkeypatch.setattr(os, "cpu_count", lambda: 1337)

    assert runner.get_max_allowed_cpu() == 1337


@pytest.mark.parametrize(
    "name,num_cpus",
    [("LSB_DJOB_RANKFILE", 2), ("LSB_DJOB_RANKFILE", 4), ("PBS_NODEFILE", 8)],
)
def test_get_max_allowed_cpu(tmp_path, monkeypatch, name, num_cpus):
    hostfile_path = tmp_path / "hostfile"
    hostfile_path.write_text("host0\n" * num_cpus)
    monkeypatch.setenv(name, str(hostfile_path))

    assert runner.get_max_allowed_cpu() == num_cpus


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
    monkeypatch.setattr(runner, "run_local", run_local)

    if machinefile_cpu is not None:
        hostfile_path = tmp_path / "hostfile"
        hostfile_path.write_text("host0\n" * machinefile_cpu)
        monkeypatch.setenv("LSB_DJOB_RANKFILE", str(hostfile_path))

    input_file = tmp_path / "test.in"
    input_file.touch()

    args = [
        "arg0",
        "--version",
        "dev",
        *(["-n", str(user_specified_cpu)] if user_specified_cpu is not None else []),
        str(input_file),
    ]
    monkeypatch.setattr(sys, "argv", args)

    runner.main()

    assert run_local.call_args[0][1].num_tasks_per_machine == expected_cpu
