"""This file is a stub logger implementation and is meant to be replaced with
something more "serious" when deploying, if one wants telemetry.

Equinor uses Microsoft Azure Application Insights services and visualises it
using Grafana, where we replace this file with our own "logger.py" (located in
a private repository) and pip-install the necessary additional telemetry
libraries.

"""

import logging

logger = logging.getLogger("runcirrus")

__all__ = ["logger"]
