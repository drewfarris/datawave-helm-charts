{{- define "queries.name" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 52 | trimSuffix "-" -}}
{{- end -}}
{{- define "queries.jobName" -}}
{{- printf "%s-%s" (include "queries.name" .) .Values.runId -}}
{{- end -}}
{{- define "queries.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
