import pytest
from runscript.configure import ProgramConfig
from runscript import runner


@pytest.fixture(autouse=True)
def _provide_config(monkeypatch):
    config = ProgramConfig(
        progname="testprog",
        launch_template="{executable} {program_args}",
        display_name="TestProg",
        executable_path="/usr/bin/testprog",
        pre_command="",
        mpirun_path="/usr/bin/mpirun",
    )
    monkeypatch.setattr(runner, "CONFIG", config)
