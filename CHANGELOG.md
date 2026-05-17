# Changelog

All notable changes to this project will be documented in this file.

The format is inspired by Keep a Changelog, and versions follow SemVer.

## 0.1.0 (2026-05-17)


### ⚠ BREAKING CHANGES

* rebrand repository from audiomind to inferflow

### Features

* add ArgoCD GitOps, ApplicationSet, and GitHub Actions CI/CD pipeline ([c74831e](https://github.com/moiraweave-labs/moiraweave-core/commit/c74831ece8f4a282efd60347d040accb7b0bd59d))
* add docker-compose with profiles for local dev stack ([30cc192](https://github.com/moiraweave-labs/moiraweave-core/commit/30cc1924f58ea8c650dd8756209f368bd48667de))
* add Helm chart for api-gateway, worker, Redis and Qdrant ([69967ad](https://github.com/moiraweave-labs/moiraweave-core/commit/69967ad797d0120236bbf06f157504dc979f8452))
* add kind cluster bootstrap targets and Kubernetes setup docs ([6451fc9](https://github.com/moiraweave-labs/moiraweave-core/commit/6451fc9dd0fa25199cbe29ca58d3b422924e7b69))
* add MLflow tracking, Argo Rollouts canary, and Evidently drift detection ([e2e03bb](https://github.com/moiraweave-labs/moiraweave-core/commit/e2e03bbb543b05b432b5fad40ab684c1cb68b185))
* **api-gateway:** add FastAPI service with JWT auth and rate limiting ([67ede71](https://github.com/moiraweave-labs/moiraweave-core/commit/67ede713260042499e60e7b3657d3b19861784ed))
* **api-gateway:** add OpenTelemetry tracing with OTLP/HTTP exporter ([267f3ef](https://github.com/moiraweave-labs/moiraweave-core/commit/267f3ef534fcf28e1d11184bdfa17fd1e2e37484))
* **cli:** add inferflow CLI and docs flow (F7-7, F7-README) ([03f7cf8](https://github.com/moiraweave-labs/moiraweave-core/commit/03f7cf8e87e471ee1bd90158fe18882b60d0ce3f))
* **f2:** complete Phase 2 Kubernetes infra ([ef30dde](https://github.com/moiraweave-labs/moiraweave-core/commit/ef30ddebadb26bff532905ff123a9a9b3f9d0e29))
* **f8:** close backlog with community, release, and docs infrastructure ([e535a8c](https://github.com/moiraweave-labs/moiraweave-core/commit/e535a8c304beb76672ca212ecb83ea343e5d7692))
* **f9:** phase 9 final quality audit complete ([c653524](https://github.com/moiraweave-labs/moiraweave-core/commit/c6535242454952a36bae95f4af63c7e0f3d1b928))
* **helm:** generic pipeline step chart (F7-4) ([bc96b9d](https://github.com/moiraweave-labs/moiraweave-core/commit/bc96b9d96c70b171004d88f8440a6bc1cb4f1df3))
* **infra:** add Terraform IaC for local/AWS/GCP Kubernetes envs ([8f591bf](https://github.com/moiraweave-labs/moiraweave-core/commit/8f591bf955082c188e18592f4e8c89c3e0cb82cf))
* **observability:** add Prometheus metrics, ServiceMonitors, PrometheusRules, and Grafana dashboards ([14dba2a](https://github.com/moiraweave-labs/moiraweave-core/commit/14dba2a01fe40da8d892055a40e47731fd1da731))
* **pipeline:** add async transcription pipeline via Redis Streams ([cc7b165](https://github.com/moiraweave-labs/moiraweave-core/commit/cc7b165a2121775b1a60d4c2a3307a5bae910ee9))
* **pipelines:** add pipeline-as-code runtime (F7-1 + F7-3) ([a0a86fb](https://github.com/moiraweave-labs/moiraweave-core/commit/a0a86fb3c4c3fd612147ca139cd43e11859952ed))
* **rag:** add semantic search with Qdrant + FastEmbed (F1-6) ([708c2fe](https://github.com/moiraweave-labs/moiraweave-core/commit/708c2feee25232bd06c86800f0c487d2b5cc309d))
* **shared:** extract Redis stream constants and schemas to audiomind-shared ([b898946](https://github.com/moiraweave-labs/moiraweave-core/commit/b89894652fccd2faaea501f697e7be2d5e60d07f))
* **steps:** add inferflow-step-sdk and audio-transcribe-whisper step ([e7f4e57](https://github.com/moiraweave-labs/moiraweave-core/commit/e7f4e57116b84a7f33ad6472b69b59d5460270a0))
* **steps:** add step registry — text-embed-fastembed, vector-index-qdrant, vector-search-qdrant (F7-2) ([43bfb6f](https://github.com/moiraweave-labs/moiraweave-core/commit/43bfb6f5c59c9521d658d8aa76b39ab6773661b0))
* **steps:** add vision-clip and image-search demo pipeline (F7-5) ([e12fe24](https://github.com/moiraweave-labs/moiraweave-core/commit/e12fe24fc3ac3c096e7b94f3714d1935eaf3ad7f))


### Bug Fixes

* add Trivy — print findings to log step in actions ([28b1ea8](https://github.com/moiraweave-labs/moiraweave-core/commit/28b1ea852e2d418087dfa7f3f1795a4135b98746))
* align monitoring manifests with moiraweave naming ([a6ede5c](https://github.com/moiraweave-labs/moiraweave-core/commit/a6ede5cc6cc5aa8230f82bd87af3d28b1f8af201))
* change Trivy steps ([b5772f3](https://github.com/moiraweave-labs/moiraweave-core/commit/b5772f363bc37258c3179dc67ec3dfaee5145d93))
* **ci:** bump-tag targets develop to avoid protected-branch rejection ([094b2cd](https://github.com/moiraweave-labs/moiraweave-core/commit/094b2cd46ecb19b68d68447b2375446eaf3f5fc9))
* **ci:** bump-tag uses HELM_BUMP_PAT to push directly to protected main ([dc7c79d](https://github.com/moiraweave-labs/moiraweave-core/commit/dc7c79d7b0b9a1ac3014f1997ab985602bbfccfb))
* **ci:** disable GHA layer cache on Docker build to guarantee fresh OS patches ([6959eb6](https://github.com/moiraweave-labs/moiraweave-core/commit/6959eb6dafdab9cca127b1bd9b35024032f2c007))
* **ci:** remove stale type: ignore comments and fix ruff import order ([8db54fb](https://github.com/moiraweave-labs/moiraweave-core/commit/8db54fb0d8350915d6f2ff27cd88a7d1069371b6))
* **ci:** use yq action instead of wget install ([2837f45](https://github.com/moiraweave-labs/moiraweave-core/commit/2837f4558a6cd679a6c797a2dad14e81ad6a5041))
* clean uv.lock ([8207750](https://github.com/moiraweave-labs/moiraweave-core/commit/82077506f48d79579d2722a46f0861727bcc4092))
* docker image and trivy failing ([41da6dd](https://github.com/moiraweave-labs/moiraweave-core/commit/41da6ddca5444ec49cbc0c8e090888c5de43c293))
* error in Dockerfile ([fff34c6](https://github.com/moiraweave-labs/moiraweave-core/commit/fff34c6faa22fb042719f3f9ca54e740fb64b8eb))
* github actions ([c2a5f28](https://github.com/moiraweave-labs/moiraweave-core/commit/c2a5f28c71fb5c8ac0227a3a35140999ab7f0a7c))
* **phase5:** correct Qdrant cursor pagination, migrate MLflow stages to aliases, remove unused deps ([5070d77](https://github.com/moiraweave-labs/moiraweave-core/commit/5070d77cae4103d3616963ca252ef3081ccd0f08))
* Ruff lint faling ([735e8a5](https://github.com/moiraweave-labs/moiraweave-core/commit/735e8a55bc97fd9463714e25d8ead00032bda2e8))
* upgrade deps, fix mypy 2.x, repair pre-commit hooks ([8917376](https://github.com/moiraweave-labs/moiraweave-core/commit/8917376e3f524f6a99be968ca0afa17967be89cf))
* wire dead config fields, harden consumer error handling ([6ef469d](https://github.com/moiraweave-labs/moiraweave-core/commit/6ef469de913f51e5bcd44a2ffe1638709396bd55))


### Documentation

* add F10 migration runbook and fix rebrand wording ([fafd9f1](https://github.com/moiraweave-labs/moiraweave-core/commit/fafd9f1751dd50a33f04bbffad1631906925a094))
* **backlog:** mark Phase 6 Terraform IaC as complete (8f591bf) ([f9d173b](https://github.com/moiraweave-labs/moiraweave-core/commit/f9d173b2fa35349978b400654c76d47f58740b40))
* cleanup phase-specific docs and streamline backlog with F10 planning ([1cb7ba7](https://github.com/moiraweave-labs/moiraweave-core/commit/1cb7ba7c0ed035fac9a89ceff14f22377b13d2cc))
* make phase 0 rebrand blocking and enforce moira naming ([69e539f](https://github.com/moiraweave-labs/moiraweave-core/commit/69e539f89a54106040422cce3df63277398472ff))
* remove F9 audit artifacts (engineering-audit, final-quality-gate) ([3f38368](https://github.com/moiraweave-labs/moiraweave-core/commit/3f38368a4daa25be06a0ee6fbbb13ed6b930a17a))


### Code Refactoring

* rebrand repository from audiomind to inferflow ([460848f](https://github.com/moiraweave-labs/moiraweave-core/commit/460848f90fe8e55432df6a0745276db60ae3c24e))

## [Unreleased]

### Added
- F7-4: Generic Helm step templates per pipeline.
- F7-5: `vision-clip` step and `image-search` demo pipeline.
- F7-6: Step CI workflow with dynamic matrix and per-step `VERSION` files.
- F7-7: Initial `moira` CLI package (`init`, list commands, and pipeline validation).

### Changed
- Image naming aligned toward `moiraweave-*` conventions in CI/release workflows.
- README rewritten to a moira-first onboarding flow.

## [0.1.0] - 2026-05-15

### Added
- Initial public baseline of runtime services, step SDK, and pipeline-as-code foundation.
