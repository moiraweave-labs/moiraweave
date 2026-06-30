# Changelog

All notable changes to this project will be documented in this file.

The format is inspired by Keep a Changelog, and versions follow SemVer.

> **Migration notice (2026-05-20)**: This project was previously named `inferflow` and before that `audiomind`.
> All CLI commands are now under the `moira` entrypoint. No legacy aliases are maintained.

## [0.1.1](https://github.com/moiraweave-labs/moiraweave/compare/v0.1.0...v0.1.1) (2026-06-30)


### Features

* add actionable preflight diagnostics ([470451a](https://github.com/moiraweave-labs/moiraweave/commit/470451a9681e5b3266725b9ca4e5d9039d73cab0))
* add AI workload control plane ([425efd9](https://github.com/moiraweave-labs/moiraweave/commit/425efd9499b1b547e5ec9457316187c0285c658b))
* add alembic control-plane baseline ([4e83929](https://github.com/moiraweave-labs/moiraweave/commit/4e8392934a4f5f4f81e448b78c8e8fa35a2aecf0))
* add control-plane audit events ([b8ba7d5](https://github.com/moiraweave-labs/moiraweave/commit/b8ba7d5e2bb0e755a47ed62fcf19bd40097b0ac0))
* add deployment controller operations ([fcdf830](https://github.com/moiraweave-labs/moiraweave/commit/fcdf830add48b95cff59097d3da4917af2357a8a))
* add managed agent runtime probes ([f537930](https://github.com/moiraweave-labs/moiraweave/commit/f5379306df6f43c367c9f0b437dec80dd8d7dd7e))
* add persistent api keys ([2e000c1](https://github.com/moiraweave-labs/moiraweave/commit/2e000c11f66f95417922d88fed822b8e65390008))
* add preflight action guide ([8533ff9](https://github.com/moiraweave-labs/moiraweave/commit/8533ff98ccf30fc41f731bbbdd39ce5143e2294c))
* add production agent runtime adapters ([5074c1d](https://github.com/moiraweave-labs/moiraweave/commit/5074c1d0373d075f58d341bf0eeaee952160bd9b))
* add rbac api keys ([9853bd2](https://github.com/moiraweave-labs/moiraweave/commit/9853bd207639a68695128a915af1d5a9714e2acb))
* add real agent certification target ([94d5ece](https://github.com/moiraweave-labs/moiraweave/commit/94d5ececb0ad40c193a656ea9306721b1696569a))
* add real agent workload examples ([c58ecd0](https://github.com/moiraweave-labs/moiraweave/commit/c58ecd0a99ebae590e0aa92f1aec1924ddbaf7c6))
* add runtime-owned channel templates ([c7844d9](https://github.com/moiraweave-labs/moiraweave/commit/c7844d9587898062d790f2c1d71556d49e123a59))
* add users teams and scoped api keys ([3ef6f3b](https://github.com/moiraweave-labs/moiraweave/commit/3ef6f3bc5a1ded8d99af0c1bf391c589ab48dbea))
* add workload deployment placement ([46f3024](https://github.com/moiraweave-labs/moiraweave/commit/46f302432db3e27f7970bbd11ed3eb96527214fc))
* add workload secret inventory ([edb2c6e](https://github.com/moiraweave-labs/moiraweave/commit/edb2c6ee994c0f3dc42553e362ff57002bb69e8b))
* **agent:** paginate sessions and message history ([865bde2](https://github.com/moiraweave-labs/moiraweave/commit/865bde26740ffcc30a3e7b8be7752191c9042f81))
* align deployment status language ([0b5bdc8](https://github.com/moiraweave-labs/moiraweave/commit/0b5bdc8173e9d968592d60b00258ddf812a2a690))
* **api:** add GET /v1/pipelines/jobs endpoint to list user jobs ([d80e310](https://github.com/moiraweave-labs/moiraweave/commit/d80e310613f34b1a247e872bfaba905cc72ddf49))
* **api:** add guided workload ops ([b7b967b](https://github.com/moiraweave-labs/moiraweave/commit/b7b967b1cc8906631c2aac2fff110fc855f65b74))
* **api:** add operations alerts ([f310e9e](https://github.com/moiraweave-labs/moiraweave/commit/f310e9ebdb420261e48ff5b6f2a755cde8535c3c))
* **api:** alert on duplicate run dispatches ([69a9bdf](https://github.com/moiraweave-labs/moiraweave/commit/69a9bdfe6cc6dbd853b7c0f8f6f0840b0fbf1931))
* **api:** alert on pending run dispatch ([d8ca7f2](https://github.com/moiraweave-labs/moiraweave/commit/d8ca7f2a129d04fc1dc97a01b885b02c3435b496))
* **api:** audit login attempts ([d554a86](https://github.com/moiraweave-labs/moiraweave/commit/d554a861bc9417759b8ac27003d1e95dbe65d439))
* **api:** audit secret inventory reads ([3d89fa6](https://github.com/moiraweave-labs/moiraweave/commit/3d89fa60cae3afcdcd6510f0fa27ce0e93c29168))
* **api:** enforce team-scoped visibility ([1245166](https://github.com/moiraweave-labs/moiraweave/commit/1245166b8e2c322b438f20b14d577db8dce1fa4b))
* **api:** filter audit events by environment ([ae4f677](https://github.com/moiraweave-labs/moiraweave/commit/ae4f6773c6969b49ecd900aff457021d7cb0733a))
* **api:** filter runs by environment ([84b297a](https://github.com/moiraweave-labs/moiraweave/commit/84b297a2ce977a3ce87b47496c76bc3e72b62980))
* **api:** harden auth and dead-letter replay ([c4c9ddf](https://github.com/moiraweave-labs/moiraweave/commit/c4c9ddf6f2bca00b310edafae4b0d6e830999985))
* **api:** rate limit sensitive operations ([7fdcfa8](https://github.com/moiraweave-labs/moiraweave/commit/7fdcfa886760e12a1f3276ed5e051a9a775113c7))
* **api:** require signed agent webhooks ([69b14b0](https://github.com/moiraweave-labs/moiraweave/commit/69b14b00e782d4dd35cfa8df534679091fd4c4ab))
* **api:** scope signed webhooks by team ([d370d49](https://github.com/moiraweave-labs/moiraweave/commit/d370d49b007b64b57dfc9b2308dbf5dc22f29375))
* **api:** scope workloads to teams ([90a726f](https://github.com/moiraweave-labs/moiraweave/commit/90a726f8fef5c03487f2589c005231223d390ad1))
* **api:** track deployment controller leases ([36c7633](https://github.com/moiraweave-labs/moiraweave/commit/36c763357d8abdd1dda8f59f6f69e624b3006f3d))
* **auth:** harden team administration ([116616a](https://github.com/moiraweave-labs/moiraweave/commit/116616a72c54491a7cf948cc3e19b74d52bba9f7))
* declare runtime-owned agent boundaries ([cc3b4b2](https://github.com/moiraweave-labs/moiraweave/commit/cc3b4b2e7d776d225b3fff8ac9e0275f84f5b210))
* **e2e:** add docker-compose E2E test suite with mock echo step ([3dae142](https://github.com/moiraweave-labs/moiraweave/commit/3dae1423cdf4c90671f7c5e3fcc029c6952486fd))
* enforce declared agent channels ([a6741ee](https://github.com/moiraweave-labs/moiraweave/commit/a6741ee41dc0fd155bcdc0dddad07d0694c8eb19))
* enrich artifact context ([5d349e6](https://github.com/moiraweave-labs/moiraweave/commit/5d349e695651a8ed7084231ca8746eb747c626a5))
* enrich artifact metadata with run context ([babc1e2](https://github.com/moiraweave-labs/moiraweave/commit/babc1e2258e4c0e22b74536d0eca804c3bf9b896))
* **events:** resume long-running run streams ([a97ff87](https://github.com/moiraweave-labs/moiraweave/commit/a97ff8735e3af3433a27db60d39c88ca392a4c01))
* expose auth profile ([b5f1b4c](https://github.com/moiraweave-labs/moiraweave/commit/b5f1b4c62229abb2e75a011a139458f7b2d21bbc))
* expose deployment plans ([9679f92](https://github.com/moiraweave-labs/moiraweave/commit/9679f92ba22b7dc71179cb90c3e62a796a2b6ce7))
* expose environments and webhook channel ([be6e7fe](https://github.com/moiraweave-labs/moiraweave/commit/be6e7feb8fe9b1c99d4ee46701459f9b9c779059))
* **helm:** add imagePullSecrets for user step private registries ([e3d74e5](https://github.com/moiraweave-labs/moiraweave/commit/e3d74e5ae34a1ab4badb9e14e1d76df828320b06))
* **helm:** harden chart with security and production readiness ([b440f9f](https://github.com/moiraweave-labs/moiraweave/commit/b440f9fed1be58b19c15761c59b4600a205c3dc0))
* list deployment operations ([0d587bd](https://github.com/moiraweave-labs/moiraweave/commit/0d587bd6c35eddd810c13412686b15f26df4e761))
* package deployment controller for k8s ([29aa8bd](https://github.com/moiraweave-labs/moiraweave/commit/29aa8bd69f4260b2a93dca2e7325340221fcbd18))
* preflight worker dispatch health ([7ecb714](https://github.com/moiraweave-labs/moiraweave/commit/7ecb7145d615a04df6f18d87df86b79abffd63ad))
* probe workload deployment health ([d236ef4](https://github.com/moiraweave-labs/moiraweave/commit/d236ef48c345de8f03af68df91b3040d19e29c26))
* reclaim abandoned run messages ([63e6c7f](https://github.com/moiraweave-labs/moiraweave/commit/63e6c7ffcd93b304975700ead25118ab79f79d37))
* **release:** add release-please config files and semver tag trigger in CI ([08df326](https://github.com/moiraweave-labs/moiraweave/commit/08df326bc068954a5cd64efa683dddd95cbaabdc))
* report run queue readiness ([1aa1263](https://github.com/moiraweave-labs/moiraweave/commit/1aa1263d9ad3a24d8f5f3fff03433c60c28052ce))
* retry transient worker failures ([7850ec0](https://github.com/moiraweave-labs/moiraweave/commit/7850ec0015a1e1f88ecb80445f8bc1de1363e986))
* return deployment action guidance ([4ae8561](https://github.com/moiraweave-labs/moiraweave/commit/4ae85612bfdb0d600c3d35e3fe7f69a054939283))
* return deployment log guidance ([c59f4ee](https://github.com/moiraweave-labs/moiraweave/commit/c59f4eeae6021aebb6380b0c520f5389245c74e0))
* rotate persistent api keys ([9e5871b](https://github.com/moiraweave-labs/moiraweave/commit/9e5871b7f8af5c9a41d1cff4b9122c7887a703bf))
* scope deployments by environment ([8478329](https://github.com/moiraweave-labs/moiraweave/commit/8478329832b2d22925bc67d3da85e0cf9e4b8648))
* serve local artifact content ([26fbef2](https://github.com/moiraweave-labs/moiraweave/commit/26fbef2962e4a132bb3a2eb07d2003120f3b7e89))
* **shared:** validate operation state transitions ([bbabcab](https://github.com/moiraweave-labs/moiraweave/commit/bbabcab9cffbd91e75e01ac0a80bac387dc47e42))
* **worker:** add recovery metrics ([ca8d67a](https://github.com/moiraweave-labs/moiraweave/commit/ca8d67a884d732c155f94a961b944827fc69b59e))
* **worker:** classify retryable run failures ([67bb6e0](https://github.com/moiraweave-labs/moiraweave/commit/67bb6e0cbbdebc309f6c9b992de5bd2f49aa91cc))
* **worker:** expose dead-letter recovery ([7a4ed83](https://github.com/moiraweave-labs/moiraweave/commit/7a4ed830a64f3418e62cb4e22627c0b6a05e44d9))


### Bug Fixes

* **api:** normalize redis stream fields ([e145182](https://github.com/moiraweave-labs/moiraweave/commit/e1451827e1af09aee174e29fad675b2aa0d5fece))
* **api:** type dead-letter replay payload ([f392ace](https://github.com/moiraweave-labs/moiraweave/commit/f392acee3509e666ef67a82072deac0f18f271d7))
* **api:** validate channel team scope ([080151e](https://github.com/moiraweave-labs/moiraweave/commit/080151ecb89eb884f5bc6665815c2ba2ef0b41d7))
* **ci:** pass JWT_SECRET_KEY env to all docker compose steps in e2e job ([1e29b6d](https://github.com/moiraweave-labs/moiraweave/commit/1e29b6d23bbe49bc97565339e2f41a078a6d88a6))
* coerce postgres timestamps ([f3cf277](https://github.com/moiraweave-labs/moiraweave/commit/f3cf277ba5d585d2073bce5d1637dabe3580eea3))
* **core:** align stream constants, fix KEDA config, reuse httpx client ([9252753](https://github.com/moiraweave-labs/moiraweave/commit/92527538f836743f53dd6c2b829cfa3c7fcc4124))
* **core:** remove domain-specific QDRANT_COLLECTION from helm values, fix drift-detector default ([3263e1f](https://github.com/moiraweave-labs/moiraweave/commit/3263e1fb03f1aedfd85f6b065f925f087e3d87db))
* **core:** remove duplicate field, fix worker job_ttl, clean auth/search routes ([b147ff8](https://github.com/moiraweave-labs/moiraweave/commit/b147ff8ab1a9e71e9c509c44cea1c98d9abac070))
* **e2e:** apply formatter fixes post-commit ([7902230](https://github.com/moiraweave-labs/moiraweave/commit/7902230197c25203d7468713ada5093fa727ff13))
* **e2e:** rebuild current compose images ([0cbf40e](https://github.com/moiraweave-labs/moiraweave/commit/0cbf40ee7831d068b2b53838e2efc2299a0dde3e))
* **e2e:** reliable qdrant healthcheck + log before teardown ([64737dd](https://github.com/moiraweave-labs/moiraweave/commit/64737dd17ce92506365a6ed8c852cfb1cb1db84d))
* enforce run state transitions ([b61df04](https://github.com/moiraweave-labs/moiraweave/commit/b61df04013a2711983390ad731705f5c89e7569c))
* guide kubernetes secret preflight ([20814b8](https://github.com/moiraweave-labs/moiraweave/commit/20814b8da346cb78aad17956548d1e8b0d7758bf))
* harden api metrics dependencies ([5407396](https://github.com/moiraweave-labs/moiraweave/commit/54073960d84ce2d9978ddfc9a35714efef5e754c))
* **helm:** require controller token secret ([0aad13f](https://github.com/moiraweave-labs/moiraweave/commit/0aad13f65c5178a0f4bd854271b1e5a79b64a68e))
* **lint:** apply ruff format to pipeline_runner.py ([2a957bb](https://github.com/moiraweave-labs/moiraweave/commit/2a957bb6a78d8c829229081cb96fa858a4cf8530))
* **lint:** sort imports in test_pipeline_runner (ruff I001) ([f29934f](https://github.com/moiraweave-labs/moiraweave/commit/f29934f76ca43d8873ca4e07b7cadced50501690))
* package helm chart dependencies ([a7d1a80](https://github.com/moiraweave-labs/moiraweave/commit/a7d1a802346318d490af972a7ec7b5c2a82a047f))
* preserve agent message run links ([57b9f77](https://github.com/moiraweave-labs/moiraweave/commit/57b9f77544ee43b5153997ec6c3b618976397717))
* simplify agent secret injection ([303bb46](https://github.com/moiraweave-labs/moiraweave/commit/303bb468cb018e11c6f3488ed58486adc4ffb298))
* stabilize migrations and image scans ([49e2fb9](https://github.com/moiraweave-labs/moiraweave/commit/49e2fb973402eb7cf27f1cf773a95a4238884d7d))
* **worker:** implement input_from routing, add InferResponse validation ([beb466d](https://github.com/moiraweave-labs/moiraweave/commit/beb466d1c383927aaf7dbf9a07458e3771da713b))
* **worker:** move step timeout to Settings, guard json.loads, add early expire ([08df326](https://github.com/moiraweave-labs/moiraweave/commit/08df326bc068954a5cd64efa683dddd95cbaabdc))


### Documentation

* add docs badge linking to moiraweave-labs.github.io ([e66192c](https://github.com/moiraweave-labs/moiraweave/commit/e66192cbd818d5cce578f6ff609a8688a8c3029e))
* **changelog:** add rebrand migration notice ([a6f0920](https://github.com/moiraweave-labs/moiraweave/commit/a6f0920b9d2852c9fbb256ba89c7deaa3b1bc0f5))
* **core:** note team scoped webhooks ([2e1f747](https://github.com/moiraweave-labs/moiraweave/commit/2e1f747e61732a304dc0d5cf70383a48e703d78b))
* describe integrated UI ([71cdbfd](https://github.com/moiraweave-labs/moiraweave/commit/71cdbfd174f82a24f20dbe0fd5d0d1d2a3f8eb5c))
* **readme:** improve structure, scope, and CI badges ([c293d65](https://github.com/moiraweave-labs/moiraweave/commit/c293d6532f8920c72afc4c845a3bcd47e8eea373))
* update contributing workload guidance ([07ef770](https://github.com/moiraweave-labs/moiraweave/commit/07ef770471f3ad11ab6c5f3ddcf5f05bd574e336))
* update platform identity overview ([f081d91](https://github.com/moiraweave-labs/moiraweave/commit/f081d911664b673b38400fe3de47b70b0a934924))
* update renamed repository links ([54a5c19](https://github.com/moiraweave-labs/moiraweave/commit/54a5c19adca3534658bfb05fef9b1ca9875853fd))

## [0.1.1](https://github.com/moiraweave-labs/moiraweave/compare/v0.1.0...v0.1.1) (2026-05-17)


### Documentation

* **readme:** improve structure, scope, and CI badges ([c293d65](https://github.com/moiraweave-labs/moiraweave/commit/c293d6532f8920c72afc4c845a3bcd47e8eea373))

## 0.1.0 (2026-05-17)


### ⚠ BREAKING CHANGES

* rebrand repository from audiomind to inferflow

### Features

* add ArgoCD GitOps, ApplicationSet, and GitHub Actions CI/CD pipeline ([c74831e](https://github.com/moiraweave-labs/moiraweave/commit/c74831ece8f4a282efd60347d040accb7b0bd59d))
* add docker-compose with profiles for local dev stack ([30cc192](https://github.com/moiraweave-labs/moiraweave/commit/30cc1924f58ea8c650dd8756209f368bd48667de))
* add Helm chart for api-gateway, worker, Redis and Qdrant ([69967ad](https://github.com/moiraweave-labs/moiraweave/commit/69967ad797d0120236bbf06f157504dc979f8452))
* add kind cluster bootstrap targets and Kubernetes setup docs ([6451fc9](https://github.com/moiraweave-labs/moiraweave/commit/6451fc9dd0fa25199cbe29ca58d3b422924e7b69))
* add MLflow tracking, Argo Rollouts canary, and Evidently drift detection ([e2e03bb](https://github.com/moiraweave-labs/moiraweave/commit/e2e03bbb543b05b432b5fad40ab684c1cb68b185))
* **api-gateway:** add FastAPI service with JWT auth and rate limiting ([67ede71](https://github.com/moiraweave-labs/moiraweave/commit/67ede713260042499e60e7b3657d3b19861784ed))
* **api-gateway:** add OpenTelemetry tracing with OTLP/HTTP exporter ([267f3ef](https://github.com/moiraweave-labs/moiraweave/commit/267f3ef534fcf28e1d11184bdfa17fd1e2e37484))
* **cli:** add inferflow CLI and docs flow (F7-7, F7-README) ([03f7cf8](https://github.com/moiraweave-labs/moiraweave/commit/03f7cf8e87e471ee1bd90158fe18882b60d0ce3f))
* **f2:** complete Phase 2 Kubernetes infra ([ef30dde](https://github.com/moiraweave-labs/moiraweave/commit/ef30ddebadb26bff532905ff123a9a9b3f9d0e29))
* **f8:** close backlog with community, release, and docs infrastructure ([e535a8c](https://github.com/moiraweave-labs/moiraweave/commit/e535a8c304beb76672ca212ecb83ea343e5d7692))
* **f9:** phase 9 final quality audit complete ([c653524](https://github.com/moiraweave-labs/moiraweave/commit/c6535242454952a36bae95f4af63c7e0f3d1b928))
* **helm:** generic pipeline step chart (F7-4) ([bc96b9d](https://github.com/moiraweave-labs/moiraweave/commit/bc96b9d96c70b171004d88f8440a6bc1cb4f1df3))
* **infra:** add Terraform IaC for local/AWS/GCP Kubernetes envs ([8f591bf](https://github.com/moiraweave-labs/moiraweave/commit/8f591bf955082c188e18592f4e8c89c3e0cb82cf))
* **observability:** add Prometheus metrics, ServiceMonitors, PrometheusRules, and Grafana dashboards ([14dba2a](https://github.com/moiraweave-labs/moiraweave/commit/14dba2a01fe40da8d892055a40e47731fd1da731))
* **pipeline:** add async transcription pipeline via Redis Streams ([cc7b165](https://github.com/moiraweave-labs/moiraweave/commit/cc7b165a2121775b1a60d4c2a3307a5bae910ee9))
* **pipelines:** add pipeline-as-code runtime (F7-1 + F7-3) ([a0a86fb](https://github.com/moiraweave-labs/moiraweave/commit/a0a86fb3c4c3fd612147ca139cd43e11859952ed))
* **rag:** add semantic search with Qdrant + FastEmbed (F1-6) ([708c2fe](https://github.com/moiraweave-labs/moiraweave/commit/708c2feee25232bd06c86800f0c487d2b5cc309d))
* **shared:** extract Redis stream constants and schemas to audiomind-shared ([b898946](https://github.com/moiraweave-labs/moiraweave/commit/b89894652fccd2faaea501f697e7be2d5e60d07f))
* **steps:** add inferflow-step-sdk and audio-transcribe-whisper step ([e7f4e57](https://github.com/moiraweave-labs/moiraweave/commit/e7f4e57116b84a7f33ad6472b69b59d5460270a0))
* **steps:** add step registry — text-embed-fastembed, vector-index-qdrant, vector-search-qdrant (F7-2) ([43bfb6f](https://github.com/moiraweave-labs/moiraweave/commit/43bfb6f5c59c9521d658d8aa76b39ab6773661b0))
* **steps:** add vision-clip and image-search demo pipeline (F7-5) ([e12fe24](https://github.com/moiraweave-labs/moiraweave/commit/e12fe24fc3ac3c096e7b94f3714d1935eaf3ad7f))


### Bug Fixes

* add Trivy — print findings to log step in actions ([28b1ea8](https://github.com/moiraweave-labs/moiraweave/commit/28b1ea852e2d418087dfa7f3f1795a4135b98746))
* align monitoring manifests with moiraweave naming ([a6ede5c](https://github.com/moiraweave-labs/moiraweave/commit/a6ede5cc6cc5aa8230f82bd87af3d28b1f8af201))
* change Trivy steps ([b5772f3](https://github.com/moiraweave-labs/moiraweave/commit/b5772f363bc37258c3179dc67ec3dfaee5145d93))
* **ci:** bump-tag targets develop to avoid protected-branch rejection ([094b2cd](https://github.com/moiraweave-labs/moiraweave/commit/094b2cd46ecb19b68d68447b2375446eaf3f5fc9))
* **ci:** bump-tag uses HELM_BUMP_PAT to push directly to protected main ([dc7c79d](https://github.com/moiraweave-labs/moiraweave/commit/dc7c79d7b0b9a1ac3014f1997ab985602bbfccfb))
* **ci:** disable GHA layer cache on Docker build to guarantee fresh OS patches ([6959eb6](https://github.com/moiraweave-labs/moiraweave/commit/6959eb6dafdab9cca127b1bd9b35024032f2c007))
* **ci:** remove stale type: ignore comments and fix ruff import order ([8db54fb](https://github.com/moiraweave-labs/moiraweave/commit/8db54fb0d8350915d6f2ff27cd88a7d1069371b6))
* **ci:** use yq action instead of wget install ([2837f45](https://github.com/moiraweave-labs/moiraweave/commit/2837f4558a6cd679a6c797a2dad14e81ad6a5041))
* clean uv.lock ([8207750](https://github.com/moiraweave-labs/moiraweave/commit/82077506f48d79579d2722a46f0861727bcc4092))
* docker image and trivy failing ([41da6dd](https://github.com/moiraweave-labs/moiraweave/commit/41da6ddca5444ec49cbc0c8e090888c5de43c293))
* error in Dockerfile ([fff34c6](https://github.com/moiraweave-labs/moiraweave/commit/fff34c6faa22fb042719f3f9ca54e740fb64b8eb))
* github actions ([c2a5f28](https://github.com/moiraweave-labs/moiraweave/commit/c2a5f28c71fb5c8ac0227a3a35140999ab7f0a7c))
* **phase5:** correct Qdrant cursor pagination, migrate MLflow stages to aliases, remove unused deps ([5070d77](https://github.com/moiraweave-labs/moiraweave/commit/5070d77cae4103d3616963ca252ef3081ccd0f08))
* Ruff lint faling ([735e8a5](https://github.com/moiraweave-labs/moiraweave/commit/735e8a55bc97fd9463714e25d8ead00032bda2e8))
* upgrade deps, fix mypy 2.x, repair pre-commit hooks ([8917376](https://github.com/moiraweave-labs/moiraweave/commit/8917376e3f524f6a99be968ca0afa17967be89cf))
* wire dead config fields, harden consumer error handling ([6ef469d](https://github.com/moiraweave-labs/moiraweave/commit/6ef469de913f51e5bcd44a2ffe1638709396bd55))


### Documentation

* add F10 migration runbook and fix rebrand wording ([fafd9f1](https://github.com/moiraweave-labs/moiraweave/commit/fafd9f1751dd50a33f04bbffad1631906925a094))
* **backlog:** mark Phase 6 Terraform IaC as complete (8f591bf) ([f9d173b](https://github.com/moiraweave-labs/moiraweave/commit/f9d173b2fa35349978b400654c76d47f58740b40))
* cleanup phase-specific docs and streamline backlog with F10 planning ([1cb7ba7](https://github.com/moiraweave-labs/moiraweave/commit/1cb7ba7c0ed035fac9a89ceff14f22377b13d2cc))
* make phase 0 rebrand blocking and enforce moira naming ([69e539f](https://github.com/moiraweave-labs/moiraweave/commit/69e539f89a54106040422cce3df63277398472ff))
* remove F9 audit artifacts (engineering-audit, final-quality-gate) ([3f38368](https://github.com/moiraweave-labs/moiraweave/commit/3f38368a4daa25be06a0ee6fbbb13ed6b930a17a))


### Code Refactoring

* rebrand repository from audiomind to inferflow ([460848f](https://github.com/moiraweave-labs/moiraweave/commit/460848f90fe8e55432df6a0745276db60ae3c24e))

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
