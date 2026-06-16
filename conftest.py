"""Root pytest hooks for service-local ``app`` packages.

The API gateway and worker are separate Python services, but both expose their
runtime package as top-level ``app``. When pytest collects both service suites in
one interpreter, whichever service was imported last can leak into the next
suite. Before pytest imports each service test module, activate that service's
root so ``app.*`` resolves to the intended package.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SERVICE_TEST_ROOTS = (
    (ROOT / "services" / "api-gateway" / "tests", ROOT / "services" / "api-gateway"),
    (ROOT / "services" / "worker" / "tests", ROOT / "services" / "worker"),
)
SERVICE_ROOTS = {str(service_root) for _, service_root in SERVICE_TEST_ROOTS}
_ACTIVE_SERVICE_ROOT: str | None = None


def _activate_service_app(service_root: Path) -> None:
    global _ACTIVE_SERVICE_ROOT
    normalized = str(service_root)
    if normalized == _ACTIVE_SERVICE_ROOT:
        return

    for key in list(sys.modules):
        if key == "app" or key.startswith("app."):
            del sys.modules[key]

    sys.path[:] = [entry for entry in sys.path if entry not in SERVICE_ROOTS]
    sys.path.insert(0, normalized)
    _ACTIVE_SERVICE_ROOT = normalized


def pytest_pycollect_makemodule(module_path: Path, parent: object) -> None:
    del parent
    path = Path(str(module_path)).resolve()
    for tests_root, service_root in SERVICE_TEST_ROOTS:
        try:
            path.relative_to(tests_root)
        except ValueError:
            continue
        _activate_service_app(service_root)
        return None
    return None
