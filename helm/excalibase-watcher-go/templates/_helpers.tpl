{{/*
Expand the name of the chart.
*/}}
{{- define "excalibase-watcher-go.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "excalibase-watcher-go.fullname" -}}
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
Create chart label.
*/}}
{{- define "excalibase-watcher-go.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "excalibase-watcher-go.labels" -}}
helm.sh/chart: {{ include "excalibase-watcher-go.chart" . }}
{{ include "excalibase-watcher-go.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "excalibase-watcher-go.selectorLabels" -}}
app.kubernetes.io/name: {{ include "excalibase-watcher-go.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name
*/}}
{{- define "excalibase-watcher-go.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "excalibase-watcher-go.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Postgres secret name
*/}}
{{- define "excalibase-watcher-go.postgresSecretName" -}}
{{- if .Values.postgres.existingSecret }}
{{- .Values.postgres.existingSecret }}
{{- else }}
{{- printf "%s-postgres" (include "excalibase-watcher-go.fullname" .) }}
{{- end }}
{{- end }}

{{/*
MySQL secret name
*/}}
{{- define "excalibase-watcher-go.mysqlSecretName" -}}
{{- if .Values.mysql.existingSecret }}
{{- .Values.mysql.existingSecret }}
{{- else }}
{{- printf "%s-mysql" (include "excalibase-watcher-go.fullname" .) }}
{{- end }}
{{- end }}
