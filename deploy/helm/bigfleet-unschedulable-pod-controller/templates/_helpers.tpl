{{- define "bigfleet-cr-controller.labels" -}}
app.kubernetes.io/name: bigfleet-unschedulable-pod-controller
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "bigfleet-cr-controller.selectorLabels" -}}
app.kubernetes.io/name: bigfleet-unschedulable-pod-controller
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "bigfleet-cr-controller.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{- define "bigfleet-cr-controller.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{ default "bigfleet-unschedulable-pod-controller" .Values.serviceAccount.name }}
{{- else -}}
{{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}
