{{/*
Common labels for everything the chart deploys. The runId label is
used by the runner to filter metrics for a single run.
*/}}
{{- define "scaletest.labels" -}}
app.kubernetes.io/managed-by: Helm
app.kubernetes.io/part-of: bigfleet-scaletest
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "scaletest.coordinatorAddr" -}}
bigfleet-coordinator.{{ .Values.namespace }}.svc:7790
{{- end -}}

{{- define "scaletest.shardAddr" -}}
bigfleet-shard.{{ .Values.namespace }}.svc:7780
{{- end -}}

{{- define "scaletest.providerAddr" -}}
bigfleet-provider.{{ .Values.namespace }}.svc:7700
{{- end -}}
