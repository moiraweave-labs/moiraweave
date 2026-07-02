{{/*
Expand the name of the chart.
*/}}
{{- define "moiraweave.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated to 63 chars because some Kubernetes name fields have those limits.
*/}}
{{- define "moiraweave.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart label value (name-version).
*/}}
{{- define "moiraweave.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "moiraweave.labels" -}}
helm.sh/chart: {{ include "moiraweave.chart" . }}
{{ include "moiraweave.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels (used in matchLabels + podSelector — must remain stable).
*/}}
{{- define "moiraweave.selectorLabels" -}}
app.kubernetes.io/name: {{ include "moiraweave.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "moiraweave.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "moiraweave.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Fully qualified name for the api-gateway component.
*/}}
{{- define "moiraweave.apiGateway.fullname" -}}
{{- printf "%s-api-gateway" (include "moiraweave.fullname" .) }}
{{- end }}

{{/*
Fully qualified name for the worker component.
*/}}
{{- define "moiraweave.worker.fullname" -}}
{{- printf "%s-worker" (include "moiraweave.fullname" .) }}
{{- end }}

{{/*
Fully qualified name for the UI component.
*/}}
{{- define "moiraweave.ui.fullname" -}}
{{- printf "%s-ui" (include "moiraweave.fullname" .) }}
{{- end }}

{{/*
Fully qualified name for the deployment controller component.
*/}}
{{- define "moiraweave.deploymentController.fullname" -}}
{{- printf "%s-deployment-controller" (include "moiraweave.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Service account name for the deployment controller.
*/}}
{{- define "moiraweave.deploymentController.serviceAccountName" -}}
{{- if .Values.deploymentController.serviceAccount.create }}
{{- default (include "moiraweave.deploymentController.fullname" .) .Values.deploymentController.serviceAccount.name }}
{{- else }}
{{- default (include "moiraweave.serviceAccountName" .) .Values.deploymentController.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Fully qualified name for one workload.
*/}}
{{- define "moiraweave.workload.fullname" -}}
{{- printf "%s-%s" (include "moiraweave.fullname" .root) .name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Stable in-cluster DNS name for one workload runtime.
Defaults to workload metadata.name so manifests resolve the same way in
Docker Compose and Kubernetes.
*/}}
{{- define "moiraweave.workload.serviceName" -}}
{{- $deployment := default dict .workload.deployment -}}
{{- default .name $deployment.serviceName | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Selector labels for one workload.
*/}}
{{- define "moiraweave.workload.selectorLabels" -}}
{{ include "moiraweave.selectorLabels" .root }}
app.kubernetes.io/component: workload
moiraweave.io/workload: {{ .name }}
{{- end }}

{{/*
Redis URL — points to the Bitnami Redis master service.
*/}}
{{- define "moiraweave.redisUrl" -}}
{{- printf "redis://%s-redis-master:6379/0" .Release.Name }}
{{- end }}

{{/*
Qdrant URL — points to the Qdrant service.
*/}}
{{- define "moiraweave.qdrantUrl" -}}
{{- printf "http://%s-qdrant:6333" .Release.Name }}
{{- end }}
