import argparse
import json
from pathlib import Path

CONFIG_PATH = Path(__file__).parent / "_config.json"


def read_config() -> dict[str, str]:
    if CONFIG_PATH.exists():
        result: dict[str, str] = json.loads(CONFIG_PATH.read_text())
        return result
    return {}


def main() -> None:
    ap = argparse.ArgumentParser(
        prog="runcirrus-configure",
        description="Configure runcirrus installation settings",
    )
    ap.add_argument(
        "--cirrus-install-path",
        required=True,
        help="Path to the directory where cirrus and mpirun binary is located",
    )
    args = ap.parse_args()

    install_path = Path(args.cirrus_install_path).expanduser().resolve()

    config = read_config()
    config["cirrus-install-path"] = str(install_path)
    CONFIG_PATH.write_text(json.dumps(config, indent=2) + "\n")
    print(f"Cirrus install path set to: {install_path}")
