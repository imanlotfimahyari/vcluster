# Supply the matching vCluster Kubernetes distro image explicitly. The locally
# rebuilt API server must have the same Kubernetes version as the base image.
ARG KUBERNETES_BASE_IMAGE
FROM ${KUBERNETES_BASE_IMAGE}
COPY --chmod=0755 kube-apiserver /kubernetes/kube-apiserver
