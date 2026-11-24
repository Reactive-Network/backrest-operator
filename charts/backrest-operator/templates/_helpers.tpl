{{/*
Expand the name of the chart.
*/}}
{{- define "backrest-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "backrest-operator.fullname" -}}
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
Chart name and version label.
*/}}
{{- define "backrest-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "backrest-operator.labels" -}}
helm.sh/chart: {{ include "backrest-operator.chart" . }}
{{ include "backrest-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: backrest-operator
{{- end }}

{{/*
Selector labels (operator)
*/}}
{{- define "backrest-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "backrest-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: operator
{{- end }}

{{/*
MCP selector labels
*/}}
{{- define "backrest-operator.mcpSelectorLabels" -}}
app.kubernetes.io/name: {{ include "backrest-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: mcp
{{- end }}

{{/*
MCP labels
*/}}
{{- define "backrest-operator.mcpLabels" -}}
helm.sh/chart: {{ include "backrest-operator.chart" . }}
{{ include "backrest-operator.mcpSelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: backrest-operator
{{- end }}

{{/*
Target namespace
*/}}
{{- define "backrest-operator.namespace" -}}
{{- default .Release.Namespace .Values.namespace.name }}
{{- end }}

{{/*
Operator service account name
*/}}
{{- define "backrest-operator.operatorServiceAccountName" -}}
{{- if .Values.operator.serviceAccount.create }}
{{- default (printf "%s-operator" (include "backrest-operator.fullname" .)) .Values.operator.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.operator.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
MCP service account name
*/}}
{{- define "backrest-operator.mcpServiceAccountName" -}}
{{- if .Values.mcp.serviceAccount.create }}
{{- default (printf "%s-mcp" (include "backrest-operator.fullname" .)) .Values.mcp.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.mcp.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Operator image
*/}}
{{- define "backrest-operator.operatorImage" -}}
{{- printf "%s:%s" .Values.operator.image.repository (.Values.operator.image.tag | default .Chart.AppVersion) }}
{{- end }}

{{/*
MCP image
*/}}
{{- define "backrest-operator.mcpImage" -}}
{{- printf "%s:%s" .Values.mcp.image.repository (.Values.mcp.image.tag | default .Chart.AppVersion) }}
{{- end }}

{{/*
Default Backrest upstream image tag (v-prefixed appVersion)
*/}}
{{- define "backrest-operator.backrestImageTag" -}}
{{- if .Values.backrest.image.tag }}
{{- .Values.backrest.image.tag }}
{{- else }}
{{- printf "v%s" .Chart.AppVersion }}
{{- end }}
{{- end }}

{{/*
Default Backrest upstream image
*/}}
{{- define "backrest-operator.backrestImage" -}}
{{- printf "%s:%s" .Values.backrest.image.repository (include "backrest-operator.backrestImageTag" .) }}
{{- end }}

{{/*
Webhook service name
*/}}
{{- define "backrest-operator.webhookServiceName" -}}
{{- printf "%s-webhook" (include "backrest-operator.fullname" .) }}
{{- end }}

{{/*
Webhook certificate secret name
*/}}
{{- define "backrest-operator.webhookCertSecretName" -}}
{{- printf "%s-webhook-cert" (include "backrest-operator.fullname" .) }}
{{- end }}

{{/*
User-facing ClusterRole names
*/}}
{{- define "backrest-operator.viewerClusterRoleName" -}}
backrest-viewer
{{- end }}

{{- define "backrest-operator.userOperatorClusterRoleName" -}}
backrest-operator
{{- end }}

{{- define "backrest-operator.adminClusterRoleName" -}}
backrest-admin
{{- end }}

{{/*
Operator ClusterRole name (runtime SA)
*/}}
{{- define "backrest-operator.operatorClusterRoleName" -}}
{{- printf "%s-operator" (include "backrest-operator.fullname" .) }}
{{- end }}

{{/*
MCP ClusterRole name
*/}}
{{- define "backrest-operator.mcpClusterRoleName" -}}
{{- printf "%s-mcp" (include "backrest-operator.fullname" .) }}
{{- end }}

{{/*
Watch namespaces env value
*/}}
{{- define "backrest-operator.watchNamespaces" -}}
{{- if .Values.operator.watch.namespaces }}
{{- join "," .Values.operator.watch.namespaces }}
{{- else }}
{{- end }}
{{- end }}

{{/*
Backrest API credentials (HTTP Basic) for the operator and MCP clients, from the Secret that
BackrestCluster.spec.auth.existingSecret names. `default dict` keeps `helm upgrade --reuse-values`
working for releases installed before backrestAuth existed.
*/}}
{{- define "backrest-operator.backrestAuthEnv" -}}
{{- $auth := .Values.backrestAuth | default dict -}}
{{- with $auth.existingSecret }}
- name: BACKREST_AUTH_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ . | quote }}
      key: username
      optional: true
- name: BACKREST_AUTH_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ . | quote }}
      key: password
      optional: true
{{- end }}
{{- end }}
