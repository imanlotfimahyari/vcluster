{{/* The runtime owns this immutable ConfigMap; Helm must never rewrite it. */}}
{{- define "vcluster.storageLayout.validate" -}}
{{- $markerName := printf "vc-storage-layout-%s" .Release.Name -}}
{{- $marker := lookup "v1" "ConfigMap" .Release.Namespace $markerName -}}
{{- if $marker -}}
{{- if ne (default "" $marker.data.version) "1" -}}
{{- fail "unsupported persisted vCluster storage layout version; explicit recovery is required" -}}
{{- end -}}
{{- $root := trimSuffix "/" (default "" .Values.controlPlane.backingStore.etcd.external.storePrefix) -}}
{{- if $root -}}{{- $root = printf "%s/" $root -}}{{- end -}}
{{- if ne (default "" $marker.data.rootPrefix) $root -}}
{{- fail "external etcd storePrefix cannot change on an existing vCluster; explicit offline storage migration is required" -}}
{{- end -}}
{{- if $root -}}
{{- $endpoint := trimSuffix "/" (trimPrefix "https://" .Values.controlPlane.backingStore.etcd.external.endpoint) -}}
{{- if ne (default "" $marker.data.etcdEndpoint) $endpoint -}}
{{- fail "tenant etcd endpoint cannot change without explicit storage migration or recovery" -}}
{{- end -}}
{{- end -}}
{{- $configSecret := lookup "v1" "Secret" .Release.Namespace (printf "vc-config-%s" .Release.Name) -}}
{{- if or (not $configSecret) (ne (default "" $marker.data.deploymentUID) (default "" $configSecret.metadata.uid)) -}}
{{- fail "storage layout belongs to another deployment identity; explicit recovery or decommission is required" -}}
{{- end -}}
{{- end -}}
{{- end -}}
