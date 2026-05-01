{{- define "bigfleet-operator.labels" -}}
app.kubernetes.io/name: bigfleet-operator
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "bigfleet-operator.selectorLabels" -}}
app.kubernetes.io/name: bigfleet-operator
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "bigfleet-operator.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{- define "bigfleet-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{ default "bigfleet-operator" .Values.serviceAccount.name }}
{{- else -}}
{{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}
