{{/*
Common labels and helpers for the bigfleet chart.
*/}}

{{- define "bigfleet.labels" -}}
app.kubernetes.io/name: bigfleet
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "bigfleet.selectorLabels" -}}
app.kubernetes.io/name: bigfleet
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "bigfleet.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{- define "bigfleet.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{ default "bigfleet" .Values.serviceAccount.name }}
{{- else -}}
{{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}
