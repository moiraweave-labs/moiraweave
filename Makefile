.DEFAULT_GOAL := help

KIND_CLUSTER_NAME   ?= moiraweave
KIND_CONFIG         ?= infra/kind/cluster.yaml
HELM_RELEASE        ?= moiraweave
HELM_NAMESPACE      ?= moiraweave
HELM_CHART          ?= infra/helm/moiraweave
HELM_VALUES         ?= $(HELM_CHART)/values-dev.yaml

# Infrastructure charts
HELM_MONITORING_CHART   ?= infra/helm/monitoring
HELM_MONITORING_VALUES  ?= $(HELM_MONITORING_CHART)/values-dev.yaml
HELM_MONITORING_NS      ?= monitoring

HELM_GPU_CHART          ?= infra/helm/gpu-operator
HELM_GPU_VALUES         ?= $(HELM_GPU_CHART)/values-dev.yaml
HELM_GPU_NS             ?= gpu-operator

HELM_KEDA_CHART         ?= infra/helm/keda
HELM_KEDA_NS            ?= keda

HELM_INGRESS_CHART      ?= infra/helm/ingress
HELM_INGRESS_VALUES     ?= $(HELM_INGRESS_CHART)/values-dev.yaml
HELM_INGRESS_NS         ?= ingress-nginx

HELM_ESO_CHART          ?= infra/helm/external-secrets
HELM_ESO_NS             ?= external-secrets

HELM_ARGOCD_CHART       ?= infra/helm/argocd
HELM_ARGOCD_VALUES      ?= $(HELM_ARGOCD_CHART)/values-dev.yaml
HELM_ARGOCD_NS          ?= argocd
ARGOCD_K8S_DIR          ?= infra/k8s/argocd

# Terraform environment directories
TF_LOCAL_DIR ?= infra/terraform/envs/local
TF_AWS_DIR   ?= infra/terraform/envs/aws
TF_GCP_DIR   ?= infra/terraform/envs/gcp

.PHONY: help install lock lint lint-fix format typecheck test test-fast \
        test-real-agents test-e2e test-kind-controller test-e2e-up test-e2e-down ci \
        pre-commit-install pre-commit-run \
        up up-mlops up-all down logs ps build \
        kind-up kind-status kind-down \
        helm-deps helm-lint helm-install helm-upgrade \
        helm-monitoring-deps helm-monitoring-install helm-monitoring-upgrade \
        helm-gpu-deps helm-gpu-install \
        helm-keda-deps helm-keda-install \
        helm-ingress-deps helm-ingress-install \
        helm-eso-deps helm-eso-install \
        helm-argocd-deps helm-argocd-install argocd-bootstrap argocd-port-forward \
        infra-up infra-namespaces \
        terraform-init-local terraform-plan-local terraform-apply-local terraform-destroy-local \
        terraform-init-aws terraform-plan-aws terraform-apply-aws \
        terraform-init-gcp terraform-plan-gcp terraform-apply-gcp

# ---------------------------------------------------------------------------
# Dev setup
# ---------------------------------------------------------------------------
install:  ## Install workspace deps + dev tools (run once after clone)
	uv sync --all-packages --dev

lock:  ## Regenerate uv.lock after editing any pyproject.toml
	uv lock

pre-commit-install:  ## Install git pre-commit hooks
	uv run pre-commit install

# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------
lint:  ## Run ruff linter
	uv run ruff check .

lint-fix:  ## Run ruff linter with auto-fix
	uv run ruff check --fix .

format:  ## Run ruff formatter
	uv run ruff format .

SHARED_PATH := $(shell pwd)/services/shared
MODEL_SDK_PATH := $(shell pwd)/services/model-sdk

typecheck:  ## Run mypy type checker per service (avoids dual-app namespace conflict)
	cd services/api-gateway && MYPYPATH=$(SHARED_PATH) uv run mypy app/
	cd services/worker && MYPYPATH=$(SHARED_PATH) uv run mypy app/
	cd services/model-sdk && uv run mypy moiraweave_model_sdk/

pre-commit-run:  ## Run pre-commit on all files
	uv run pre-commit run --all-files

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
test:  ## Run pytest with coverage
	uv run pytest

test-fast:  ## Run pytest without coverage (faster)
	uv run pytest --no-cov

test-real-agents:  ## Run optional live Hermes/OpenClaw adapter certification tests
	@echo "==> Running optional live Hermes/OpenClaw adapter tests..."
	@echo "    Set MOIRAWEAVE_REAL_AGENT_TESTS=1 plus runtime URL env vars to hit live runtimes."
	uv run --frozen pytest services/worker/tests/test_real_agent_runtimes.py -q --no-cov --import-mode=importlib -m real_agent

test-kind-controller:  ## Run optional kind/Kubernetes deployment-controller smoke test
	@command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl is required."; exit 127; }
	@command -v helm >/dev/null 2>&1 || { echo "ERROR: helm is required."; exit 127; }
	@echo "==> Running optional Kubernetes deployment-controller smoke test..."
	@echo "    Requires a running MoiraWeave API, kube context, and deployment permissions."
	MOIRAWEAVE_KIND_CONTROLLER_E2E=1 uv run pytest tests/e2e/test_kind_controller_smoke.py -q --no-cov --import-mode=importlib

E2E_COMPOSE := docker compose -f docker-compose.yml -f tests/e2e/docker-compose.e2e.yml

test-e2e:  ## Build mock-model, start E2E stack, run E2E tests, tear down
	@echo "==> Building mock-model image..."
	$(E2E_COMPOSE) build mock-model
	@echo "==> Starting E2E stack (waiting for healthchecks)..."
	$(E2E_COMPOSE) up -d --wait --build
	@echo "==> Running E2E tests..."
	uv run pytest tests/e2e/ -v --no-cov --import-mode=importlib; \
	  STATUS=$$?; \
	  echo "==> Tearing down E2E stack..."; \
	  $(E2E_COMPOSE) down; \
	  exit $$STATUS

test-e2e-up:  ## Start E2E stack only (for iterating on tests manually)
	$(E2E_COMPOSE) build mock-model
	$(E2E_COMPOSE) up -d --wait --build

test-e2e-down:  ## Stop and remove E2E stack
	$(E2E_COMPOSE) down

# ---------------------------------------------------------------------------
# Docker Compose
# ---------------------------------------------------------------------------
build:  ## Build Docker images
	docker compose build

up:  ## Start core services (api-gateway, redis, qdrant, jaeger)
	docker compose up -d

up-mlops:  ## Start core + MLOps services (+ postgres, mlflow)
	docker compose --profile mlops up -d

up-all:  ## Start all core services (alias of up-mlops)
	docker compose --profile mlops up -d

down:  ## Stop and remove containers (preserves volumes)
	docker compose down

logs:  ## Tail logs for all running services
	docker compose logs -f

ps:  ## Show status of all services
	docker compose ps

# ---------------------------------------------------------------------------
# Kubernetes + Helm
# ---------------------------------------------------------------------------
kind-up:  ## Create the local multi-node kind cluster
	@command -v kind >/dev/null 2>&1 || { echo "ERROR: kind is required. Install it from https://kind.sigs.k8s.io/docs/user/quick-start/"; exit 127; }
	@command -v docker >/dev/null 2>&1 || { echo "ERROR: Docker is required before creating the kind cluster."; exit 127; }
	kind create cluster --name $(KIND_CLUSTER_NAME) --config $(KIND_CONFIG)

kind-status:  ## Show local kind cluster nodes and namespaces
	@command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl is required. Install it from https://kubernetes.io/docs/tasks/tools/"; exit 127; }
	kubectl cluster-info --context kind-$(KIND_CLUSTER_NAME)
	kubectl get nodes -o wide
	kubectl get namespaces

kind-down:  ## Delete the local kind cluster
	@command -v kind >/dev/null 2>&1 || { echo "ERROR: kind is required. Install it from https://kind.sigs.k8s.io/docs/user/quick-start/"; exit 127; }
	kind delete cluster --name $(KIND_CLUSTER_NAME)

helm-deps:  ## Download and extract Helm subchart dependencies (run once after clone)
	@command -v helm >/dev/null 2>&1 || { echo "ERROR: helm is required. Install it from https://helm.sh/docs/intro/install/"; exit 127; }
	helm dependency build $(HELM_CHART)
	@cd $(HELM_CHART)/charts && for f in *.tgz; do tar xzf "$$f"; done

helm-lint:  ## Lint the Helm chart
	@command -v helm >/dev/null 2>&1 || { echo "ERROR: helm is required."; exit 127; }
	helm lint $(HELM_CHART) -f $(HELM_VALUES)

helm-install:  ## Install the MoiraWeave Helm release
	@test -f $(HELM_CHART)/Chart.yaml || { echo "ERROR: Helm chart not found at $(HELM_CHART). Complete F2-2 before running this target."; exit 2; }
	@command -v helm >/dev/null 2>&1 || { echo "ERROR: helm is required. Install it from https://helm.sh/docs/intro/install/"; exit 127; }
	helm dependency build $(HELM_CHART)
	@cd $(HELM_CHART)/charts && for f in *.tgz; do tar xzf "$$f"; done
	helm install $(HELM_RELEASE) $(HELM_CHART) \
		--namespace $(HELM_NAMESPACE) --create-namespace \
		-f $(HELM_VALUES)

helm-upgrade:  ## Upgrade (or install if absent) the MoiraWeave Helm release
	@test -f $(HELM_CHART)/Chart.yaml || { echo "ERROR: Helm chart not found at $(HELM_CHART). Complete F2-2 before running this target."; exit 2; }
	@command -v helm >/dev/null 2>&1 || { echo "ERROR: helm is required. Install it from https://helm.sh/docs/intro/install/"; exit 127; }
	helm dependency build $(HELM_CHART)
	@cd $(HELM_CHART)/charts && for f in *.tgz; do tar xzf "$$f"; done
	helm upgrade --install $(HELM_RELEASE) $(HELM_CHART) \
		--namespace $(HELM_NAMESPACE) --create-namespace \
		-f $(HELM_VALUES)

# ---------------------------------------------------------------------------
# Monitoring stack (F2-7)
# ---------------------------------------------------------------------------
helm-monitoring-deps:  ## Download monitoring chart dependencies (kube-prometheus-stack, Loki, Jaeger)
	@command -v helm >/dev/null 2>&1 || { echo "ERROR: helm is required."; exit 127; }
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
	helm repo add grafana https://grafana.github.io/helm-charts 2>/dev/null || true
	helm repo add jaegertracing https://jaegertracing.github.io/helm-charts 2>/dev/null || true
	helm repo update
	helm dependency build $(HELM_MONITORING_CHART)

helm-monitoring-install:  ## Install the monitoring stack
	$(MAKE) helm-monitoring-deps
	helm upgrade --install moiraweave-monitoring $(HELM_MONITORING_CHART) \
		--namespace $(HELM_MONITORING_NS) --create-namespace \
		-f $(HELM_MONITORING_VALUES)
	kubectl apply -f infra/k8s/monitoring/

helm-monitoring-upgrade:  ## Upgrade the monitoring stack
	$(MAKE) helm-monitoring-install

# ---------------------------------------------------------------------------
# GPU Operator (F2-3)
# ---------------------------------------------------------------------------
helm-gpu-deps:  ## Download GPU Operator chart dependency
	@command -v helm >/dev/null 2>&1 || { echo "ERROR: helm is required."; exit 127; }
	helm repo add nvidia https://helm.ngc.nvidia.com/nvidia 2>/dev/null || true
	helm repo update
	helm dependency build $(HELM_GPU_CHART)

helm-gpu-install:  ## Install NVIDIA GPU Operator (requires GPU node)
	$(MAKE) helm-gpu-deps
	kubectl apply -f infra/k8s/gpu/time-slicing-config.yaml 2>/dev/null || true
	helm upgrade --install gpu-operator $(HELM_GPU_CHART) \
		--namespace $(HELM_GPU_NS) --create-namespace \
		-f $(HELM_GPU_VALUES)

# ---------------------------------------------------------------------------
# KEDA (F2-5)
# ---------------------------------------------------------------------------
helm-keda-deps:  ## Download KEDA chart dependency
	@command -v helm >/dev/null 2>&1 || { echo "ERROR: helm is required."; exit 127; }
	helm repo add kedacore https://kedacore.github.io/charts 2>/dev/null || true
	helm repo update
	helm dependency build $(HELM_KEDA_CHART)

helm-keda-install:  ## Install KEDA and apply ScaledObjects
	$(MAKE) helm-keda-deps
	helm upgrade --install keda $(HELM_KEDA_CHART) \
		--namespace $(HELM_KEDA_NS) --create-namespace
	@echo "Waiting for KEDA CRDs..."
	kubectl wait --for condition=established crd/scaledobjects.keda.sh --timeout=60s
	kubectl apply -f infra/k8s/keda/ -n $(HELM_NAMESPACE)

# ---------------------------------------------------------------------------
# Ingress + cert-manager (F2-8)
# ---------------------------------------------------------------------------
helm-ingress-deps:  ## Download ingress-nginx + cert-manager chart dependencies
	@command -v helm >/dev/null 2>&1 || { echo "ERROR: helm is required."; exit 127; }
	helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx 2>/dev/null || true
	helm repo add jetstack https://charts.jetstack.io 2>/dev/null || true
	helm repo update
	helm dependency build $(HELM_INGRESS_CHART)

helm-ingress-install:  ## Install ingress-nginx and cert-manager
	$(MAKE) helm-ingress-deps
	helm upgrade --install moiraweave-ingress $(HELM_INGRESS_CHART) \
		--namespace $(HELM_INGRESS_NS) --create-namespace \
		-f $(HELM_INGRESS_VALUES)
	@echo "Waiting for cert-manager webhook..."
	kubectl wait --for=condition=Available deployment/moiraweave-ingress-cert-manager-webhook \
		-n $(HELM_INGRESS_NS) --timeout=120s 2>/dev/null || true
	kubectl apply -f infra/k8s/cert-manager/

# ---------------------------------------------------------------------------
# External Secrets Operator (F2-8)
# ---------------------------------------------------------------------------
helm-eso-deps:  ## Download External Secrets Operator chart dependency
	@command -v helm >/dev/null 2>&1 || { echo "ERROR: helm is required."; exit 127; }
	helm repo add external-secrets https://charts.external-secrets.io 2>/dev/null || true
	helm repo update
	helm dependency build $(HELM_ESO_CHART)

helm-eso-install:  ## Install External Secrets Operator
	$(MAKE) helm-eso-deps
	helm upgrade --install external-secrets $(HELM_ESO_CHART) \
		--namespace $(HELM_ESO_NS) --create-namespace

# ---------------------------------------------------------------------------
# Namespace setup (F2-6)
# ---------------------------------------------------------------------------
infra-namespaces:  ## Apply ResourceQuota and LimitRange to the moiraweave namespace
	@command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl is required."; exit 127; }
	kubectl create namespace $(HELM_NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f infra/k8s/namespaces/moiraweave/

# ---------------------------------------------------------------------------
# Full local infra bootstrap (F2-3 through F2-8 in order)
# ---------------------------------------------------------------------------
infra-up:  ## Bootstrap the full local infra stack on a running kind cluster
	$(MAKE) infra-namespaces
	$(MAKE) helm-ingress-install
	$(MAKE) helm-monitoring-install
	$(MAKE) helm-keda-install
	$(MAKE) helm-eso-install
	$(MAKE) helm-install
	@echo ""
	@echo "✓ Full MoiraWeave infra deployed. Run 'make kind-status' to verify."

# ---------------------------------------------------------------------------
# CI gate (runs locally exactly what CI runs)
# ---------------------------------------------------------------------------
ci: lint typecheck test  ## Run full CI checks: lint + typecheck + test

# ---------------------------------------------------------------------------
# ArgoCD — GitOps controller (F3-1 … F3-5)
# ---------------------------------------------------------------------------
helm-argocd-deps:  ## Download ArgoCD chart dependency (argo/argo-cd 9.5.14)
	@command -v helm >/dev/null 2>&1 || { echo "ERROR: helm is required."; exit 127; }
	helm repo add argo https://argoproj.github.io/argo-helm 2>/dev/null || true
	helm repo update
	helm dependency build $(HELM_ARGOCD_CHART)

helm-argocd-install:  ## Install ArgoCD into the 'argocd' namespace
	$(MAKE) helm-argocd-deps
	kubectl create namespace $(HELM_ARGOCD_NS) --dry-run=client -o yaml | kubectl apply -f -
	helm upgrade --install argocd $(HELM_ARGOCD_CHART) \
		--namespace $(HELM_ARGOCD_NS) \
		-f $(HELM_ARGOCD_VALUES)
	@echo "Waiting for ArgoCD server to become available..."
	kubectl wait --for=condition=Available deployment/argocd-server \
		-n $(HELM_ARGOCD_NS) --timeout=120s 2>/dev/null || true

argocd-bootstrap:  ## Apply App-of-apps + AppProject + ApplicationSet (idempotent)
	@command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl is required."; exit 127; }
	kubectl apply -f $(ARGOCD_K8S_DIR)/project.yaml
	kubectl apply -f $(ARGOCD_K8S_DIR)/applicationset.yaml
	kubectl apply -f $(ARGOCD_K8S_DIR)/app-of-apps.yaml
	@echo "✓ ArgoCD bootstrapped. Access the UI via: make argocd-port-forward"

argocd-port-forward:  ## Forward ArgoCD UI to http://localhost:8080 (Ctrl-C to stop)
	kubectl port-forward svc/argocd-server -n $(HELM_ARGOCD_NS) 8080:80

# ---------------------------------------------------------------------------
# Terraform — Infrastructure as Code
# ---------------------------------------------------------------------------

# local (kind) ----------------------------------------------------------------

terraform-init-local:  ## terraform init for envs/local
	@command -v terraform >/dev/null 2>&1 || { echo "ERROR: terraform is required. Install from https://developer.hashicorp.com/terraform/downloads"; exit 127; }
	terraform -chdir=$(TF_LOCAL_DIR) init

terraform-plan-local:  ## terraform plan for envs/local (requires terraform.tfvars)
	terraform -chdir=$(TF_LOCAL_DIR) plan -var-file=terraform.tfvars

terraform-apply-local:  ## terraform apply for envs/local (auto-approve)
	terraform -chdir=$(TF_LOCAL_DIR) apply -var-file=terraform.tfvars -auto-approve

terraform-destroy-local:  ## terraform destroy for envs/local (auto-approve)
	terraform -chdir=$(TF_LOCAL_DIR) destroy -var-file=terraform.tfvars -auto-approve

# AWS (EKS) -------------------------------------------------------------------

terraform-init-aws:  ## terraform init for envs/aws
	@command -v terraform >/dev/null 2>&1 || { echo "ERROR: terraform is required."; exit 127; }
	terraform -chdir=$(TF_AWS_DIR) init

terraform-plan-aws:  ## terraform plan for envs/aws (requires terraform.tfvars)
	terraform -chdir=$(TF_AWS_DIR) plan -var-file=terraform.tfvars

terraform-apply-aws:  ## terraform apply for envs/aws (auto-approve)
	terraform -chdir=$(TF_AWS_DIR) apply -var-file=terraform.tfvars -auto-approve

# GCP (GKE) -------------------------------------------------------------------

terraform-init-gcp:  ## terraform init for envs/gcp
	@command -v terraform >/dev/null 2>&1 || { echo "ERROR: terraform is required."; exit 127; }
	terraform -chdir=$(TF_GCP_DIR) init

terraform-plan-gcp:  ## terraform plan for envs/gcp (requires terraform.tfvars)
	terraform -chdir=$(TF_GCP_DIR) plan -var-file=terraform.tfvars

terraform-apply-gcp:  ## terraform apply for envs/gcp (auto-approve)
	terraform -chdir=$(TF_GCP_DIR) apply -var-file=terraform.tfvars -auto-approve

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
