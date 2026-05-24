# MoiraWeave

[![CI](https://github.com/moiraweave-labs/moiraweave/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/moiraweave-labs/moiraweave/actions/workflows/ci.yml)
[![Release Please](https://github.com/moiraweave-labs/moiraweave/actions/workflows/release.yml/badge.svg?branch=main)](https://github.com/moiraweave-labs/moiraweave/actions/workflows/release.yml)
[![Publish to PyPI](https://github.com/moiraweave-labs/moiraweave/actions/workflows/publish.yml/badge.svg?branch=main)](https://github.com/moiraweave-labs/moiraweave/actions/workflows/publish.yml)
[![Python](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/)
[![Docs](https://img.shields.io/badge/docs-live-blue)](https://moiraweave-labs.github.io/moiraweave-docs/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](pyproject.toml)

Self-hosted AI workload and agent operations platform. MoiraWeave gives teams a
single control plane to deploy, chat with, observe, cancel, and operate agents,
model services, and pipelines.

## Scope

This repository owns platform runtime capabilities, not customer business logic.

### Included

- `services/`: API gateway, worker, shared runtime package, adapters, and step SDK
- `infra/`: Helm, Kubernetes, kind, and Terraform assets
- `monitoring/`: observability assets and dashboards
- `tests/`: integration and platform-level validation

### Excluded

- customer workload manifests
- customer agent/model internals
- customer environment overlays and secrets
- step-specific model services in the base runtime compose profile

MoiraWeave manages the control plane around agents: deployment, sessions,
messages, runs, events, cancellation, health, and artifacts. Agent runtimes such
as Hermes, OpenClaw, or LangGraph keep their own reasoning loop, memory, tools,
and configuration semantics.

## For platform users

You usually do not need to clone this repository directly.

Use the CLI instead. The fastest local path is:

1. `uv tool install moiraweave-cli`
2. `moira up`
3. Open `http://localhost:3000`

`moira up` initializes a workspace if needed, creates a no-secret demo agent
when there are no workloads, starts API, worker, Postgres, Redis, Qdrant, UI,
and workload services, then registers deployment records.

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

`docker-compose.yml` is intentionally generic. Workload-specific runtimes should
be configured in the user workspace, not embedded in the platform runtime.

## Related repositories

- [moiraweave-cli](https://github.com/moiraweave-labs/moiraweave-cli): user-facing CLI
- [moiraweave-ui](https://github.com/moiraweave-labs/moiraweave-ui): Ops dashboard
- [moiraweave-docs](https://github.com/moiraweave-labs/moiraweave-docs): public documentation
- [.github](https://github.com/moiraweave-labs/.github): org-wide standards
