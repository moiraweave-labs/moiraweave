# k8s-cluster — cloud-agnostic interface module

## Purpose

This module does **not** create any cloud resources.
It defines the canonical variable interface that every environment root module (`envs/local`, `envs/aws`, `envs/gcp`) must satisfy, and performs cross-field validation before any cloud-specific code runs.

## Usage

```hcl
module "cluster_config" {
  source = "../../modules/k8s-cluster"

  cluster_name       = var.cluster_name
  node_count         = var.node_count
  gpu_node_count     = var.gpu_node_count
  gpu_instance_type  = var.gpu_instance_type
  kubernetes_version = var.kubernetes_version
}
```

Then reference validated values with `module.cluster_config.<output>`.

## Variables

| Name | Type | Default | Description |
|---|---|---|---|
| `cluster_name` | string | — | Cluster name (3–40 lowercase chars) |
| `node_count` | number | `2` | CPU worker nodes |
| `gpu_node_count` | number | `0` | GPU worker nodes (0 = disabled) |
| `gpu_instance_type` | string | `""` | Cloud instance type for GPU nodes |
| `kubernetes_version` | string | `"latest"` | Control-plane version |

## Supported environments

| Environment | Cloud | Module |
|---|---|---|
| `envs/local` | Local (Docker) | `tehcyx/kind` provider v0.11.x |
| `envs/aws` | AWS | `terraform-aws-modules/eks/aws` v21.x |
| `envs/gcp` | GCP | `terraform-google-modules/kubernetes-engine/google` v44.x |
