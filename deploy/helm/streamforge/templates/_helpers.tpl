{{/*
Common naming and labelling helpers.
*/}}

{{- define "streamforge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamforge.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Labels applied to every object in the release. */}}
{{- define "streamforge.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "streamforge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: streamforge
{{- end -}}

{{/*
Selector labels for one component. Deployment selectors are immutable once
created, so these stay deliberately minimal — nothing version-dependent.
*/}}
{{- define "streamforge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "streamforge.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* Hostname:port of the Kafka broker the services should use. */}}
{{- define "streamforge.kafkaBrokers" -}}
{{- if .Values.infra.enabled -}}
{{- printf "%s-redpanda:%d" (include "streamforge.fullname" .) (int .Values.infra.redpanda.port) -}}
{{- else -}}
{{- required "infra.enabled=false requires external.kafkaBrokers" .Values.external.kafkaBrokers -}}
{{- end -}}
{{- end -}}

{{/* Postgres DSN built from the secret values. */}}
{{- define "streamforge.postgresDSN" -}}
{{- if .Values.infra.enabled -}}
{{- printf "postgres://%s:%s@%s-timescaledb:%d/%s?sslmode=disable"
      .Values.secrets.postgresUser
      .Values.secrets.postgresPassword
      (include "streamforge.fullname" .)
      (int .Values.infra.timescaledb.port)
      .Values.secrets.postgresDatabase -}}
{{- else -}}
{{- required "infra.enabled=false requires external.postgresDSN" .Values.external.postgresDSN -}}
{{- end -}}
{{- end -}}

{{- define "streamforge.otlpEndpoint" -}}
{{- if .Values.infra.enabled -}}
{{- printf "%s-jaeger:%d" (include "streamforge.fullname" .) (int .Values.infra.jaeger.otlpPort) -}}
{{- else -}}
{{- default "" .Values.external.otlpEndpoint -}}
{{- end -}}
{{- end -}}

{{- define "streamforge.prometheusURL" -}}
{{- if .Values.infra.enabled -}}
{{- printf "http://%s-prometheus:%d" (include "streamforge.fullname" .) (int .Values.infra.prometheus.port) -}}
{{- else -}}
{{- default "" .Values.external.prometheusURL -}}
{{- end -}}
{{- end -}}

{{/*
Prometheus scrape config. Lives here as a named template so the ConfigMap body
and the Deployment's roll-on-change checksum can both read it — checksumming
the manifest file instead would make it include itself and recurse.
*/}}
{{- define "streamforge.prometheusConfig" -}}
global:
  scrape_interval: 5s
  scrape_timeout: 3s

scrape_configs:
  - job_name: streamforge-pods
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names: [{{ .Release.Namespace }}]
    relabel_configs:
      # Opt in by annotation, so infra pods that expose no /metrics are never
      # scraped into a permanently "down" target.
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: "true"
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
        action: replace
        target_label: __metrics_path__
        regex: (.+)
      - source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_port]
        action: replace
        regex: ([^:]+)(?::\d+)?;(\d+)
        replacement: $1:$2
        target_label: __address__
      # The dashboards key off `service`, and the compose scrape configs set that
      # same label statically. Mapping the component label onto it here is what
      # lets one dashboard JSON serve both environments unchanged.
      - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component]
        action: replace
        target_label: service
      - source_labels: [__meta_kubernetes_pod_name]
        action: replace
        target_label: pod
{{- end -}}

{{/*
Pod security context shared by the four services. The images are built on
distroless :nonroot, which already runs as UID 65532, so this asserts the
posture rather than imposing a UID the image cannot satisfy.
*/}}
{{- define "streamforge.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
fsGroup: 65532
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "streamforge.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop:
    - ALL
{{- end -}}
