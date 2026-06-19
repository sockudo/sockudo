{{/*
Expand the name of the chart.
*/}}
{{- define "sockudo.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "sockudo.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "sockudo.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "sockudo.labels" -}}
helm.sh/chart: {{ include "sockudo.chart" . }}
{{ include "sockudo.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "sockudo.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sockudo.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "sockudo.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "sockudo.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Config map name
*/}}
{{- define "sockudo.configMapName" -}}
{{- printf "%s-config" (include "sockudo.fullname" .) }}
{{- end }}

{{/*
Secret name
*/}}
{{- define "sockudo.secretName" -}}
{{- printf "%s-secret" (include "sockudo.fullname" .) }}
{{- end }}

{{/*
Dashboard API deployment/service name
*/}}
{{- define "sockudo.dashboardApiName" -}}
{{- printf "%s-dashboard-api" (include "sockudo.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Dashboard web deployment/service name
*/}}
{{- define "sockudo.dashboardWebName" -}}
{{- printf "%s-dashboard-web" (include "sockudo.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Dashboard secret name
*/}}
{{- define "sockudo.dashboardSecretName" -}}
{{- printf "%s-dashboard-secret" (include "sockudo.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Dashboard component selector labels
*/}}
{{- define "sockudo.dashboardSelectorLabels" -}}
app.kubernetes.io/name: {{ include "sockudo.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Dashboard component labels
*/}}
{{- define "sockudo.dashboardLabels" -}}
helm.sh/chart: {{ include "sockudo.chart" .root }}
{{ include "sockudo.dashboardSelectorLabels" . }}
{{- if .root.Chart.AppVersion }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
{{- end }}
