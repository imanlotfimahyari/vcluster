# Shared etcd worker acceptance

The disposable offline QEMU guest runs a real K3s worker, containerd and Flannel
host-gw. Two vCluster process fixtures use separate authenticated roots on stock
etcd 3.6.16. No existing cluster, host socket, shared host filesystem or external
network is exposed to the guest.

## Recovered fixture prerequisites

Earlier worker attempts did not pass. The initial fixture omitted tenant DNS;
Pod translation therefore waited for a DNS Service indefinitely. Subsequent
attempts exposed missing `/etc/hosts`, netfilter modules, the Helm-provided
workload ServiceAccount, and an offline image with empty OS/architecture fields.
Containerd imported that image's metadata but could not create its container
filesystem. The image generator now declares Linux amd64. These are fixture
repairs; they do not establish a production deployment qualification.

The fixture creates the workload ServiceAccount normally rendered by Helm,
without mounting its API token or granting it host permissions. Host admission
remains enabled. An ordinary networked Pod must become Ready and resolve the
host Kubernetes Service to its exact ClusterIP before either tenant starts.
The guest includes kube-proxy/network-policy modules and a `/sbin/modprobe`
helper. Raw guest logs are private (0600); they can contain disposable K3s join
credentials and must not be included in source checkpoints.

Each tenant resolver is real CoreDNS, using a short-lived tenant-local token
with read access to DNS metadata only. The native DNS identity regression checks
its allowed Service/EndpointSlice reads, denied Secret reads and writes, and
foreign-token rejection through the other proxy and backing API. TLS stays on.

Physical object discovery checks translated ownership/name/namespace metadata.
Pods and PVCs must additionally match the current virtual UID. Service creation
can precede the virtual UID and persisted mapping, so Services use their public
translation metadata. A separate native regression requires actual translated
Service/PVC/Pod objects and an existing workload ServiceAccount.

## Worker assertions

1. Identically named application Pods run in both tenants. Real node/Pod-IP
   status, direct Pod-IP HTTP and translated Service HTTP must work.
2. Standard ClusterFirst DNS resolves a tenant Service and reaches that tenant's
   application through its own resolver.
3. Identically named PVCs bind to different physical claims and PVs. Applications
   write distinct durable bytes to their actual mounts.
4. Tenant B serves its own bytes through DNS while tenant A's control plane is
   stopped.
5. After A restarts, both its client and server Pods are replaced. Virtual and
   physical UIDs must change; a new Service DNS record must resolve through the
   still-running resolver; the original durable bytes must remain.
6. Virtual PVC, physical PVC and PV identities/bindings must remain unchanged.

The server initializes its index only if absent and never recreates the durable
file. A lost or incorrectly rebound volume cannot pass by reseeding test data.

## Reproduce and limits

See [the integration README](../test/integration/sharedetcd/README.md) for pinned
assets and the static runner command. The guest uses two emulated CPUs, 5 GiB RAM
and a disposable 4 GiB ext4 disk. Qualified Ubuntu assets are kernel
6.8.0-138-generic, QEMU 8.2.2 (8.2.2+ds-0ubuntu1.18) and BusyBox 1.36.1-6ubuntu3.1.
Packages are SHA-256 verified against signed official snapshot metadata and
extracted without installing them on the host.

CoreDNS runs as a host-network Pod to reach the loopback tenant process API.
This validates actual service discovery and scoped authentication; it does not
validate the chart-managed tenant CoreDNS Deployment or virtual controllers.
Static node-local hostPath PVs validate actual binding/mount/data persistence;
they do not qualify CSI, dynamic provisioning, cross-node failover or backups.

The vCluster process fixtures use host administrator access. Controller-managed
migration/recovery, normal production RBAC, Helm-managed runtime rollout,
patched Kubernetes/vCluster images, HA and load/fault testing remain separate
gates. The [status document](shared-etcd-status.md) records actual completed runs.
