# MoiraWeave Core

[![CI](https://github.com/moiraweave-labs/moiraweave-core/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/moiraweave-labs/moiraweave-core/actions/workflows/ci.yml)
[![Release Please](https://github.com/moiraweave-labs/moiraweave-core/actions/workflows/release.yml/badge.svg?branch=main)](https://github.com/moiraweave-labs/moiraweave-core/actions/workflows/release.yml)
[![Publish to PyPI](https://github.com/moiraweave-labs/moiraweave-core/actions/workflows/publish.yml/badge.svg?branch=main)](https://github.com/moiraweave-labs/moiraweave-core/actions/workflows/publish.yml)
[![Python](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](pyproject.toml)

Runtime and infrastructure repository for the MoiraWeave platform.

## Scope

This repository owns platform runtime capabilities, not customer business logic.

### Included

- `services/`: API gateway, worker, shared runtime package, and step SDK
- `infra/`: Helm, Kubernetes, kind, and Terraform assets
- `monitoring/`: observability assets and dashboards
- `tests/`: integration and platform-level validation

### Excluded

- customer pipelines
- customer custom steps
- customer environment overlays and secrets
- step-specific model services in the base runtime compose profile

## For platform users

You usually do not need to clone this repository directly.

Use the CLI instead:

1. `uv tool install moiraweave-cli`
2. `moira project init`
3. Author pipelines and steps in your workspace

## Local development

```bash
uv sync --frozen --all-packages
make ci
```

## CI/CD summary

- `ci.yml`: lint, typecheck, tests, image build and security scan
- `publish.yml`: publishes shared Python packages on release
- `release.yml`: automated release PR/versioning via Release Please

## Repository model

`docker-compose.yml` is intentionally generic. Step-specific runtime dependencies should be configured in the user workspace, not embedded in core.

## Related repositories

- [moiraweave-cli](https://github.com/moiraweave-labs/moiraweave-cli): user-facing CLI
- [moiraweave-steps](https://github.com/moiraweave-labs/moiraweave-steps): official step catalog
- [moiraweave-docs](https://github.com/moiraweave-labs/moiraweave-docs): public documentation
- [.github](https://github.com/moiraweave-labs/.github): org-wide standards
