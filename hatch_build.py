import os
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version, build_data):
        versions_path = os.environ.get("CIRRUS_VERSIONS_PATH", "")
        config_file = Path(self.root) / "src" / "runcirrus" / "_config.py"
        config_file.write_text(f'CIRRUS_VERSIONS_PATH = "{versions_path}"\n')
