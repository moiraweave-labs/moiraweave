# Workload Examples

These examples are versioned workload manifests for real agent runtimes.
They are meant to be copied into `.moiraweave/workloads/<name>/workload.yaml`
or used as a reference for `moira workload` and `moira deploy` flows.

MoiraWeave owns deployment, sessions, runs, events, cancellation, artifact
metadata, health, and audit records. The agent runtime owns its reasoning loop,
tools, memory, provider configuration, and runtime-native channels.

- `hermes`: managed Hermes Agent runtime with HTTP health probes.
- `openclaw`: managed OpenClaw Gateway runtime with TCP probes and RPC health.
- `external-hermes`: externally deployed Hermes runtime supervised by endpoint.
