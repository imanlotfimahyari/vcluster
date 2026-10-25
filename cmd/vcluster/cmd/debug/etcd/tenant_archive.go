package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/constants"
	setupconfig "github.com/loft-sh/vcluster/pkg/setup/config"
	"github.com/loft-sh/vcluster/pkg/tenantbackup"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type tenantArchiveOptions struct {
	restartLeaseTTL                       bool
	config, name, namespace               string
	hostKubeconfig, hostContext           string
	adminCA, adminCert, adminKey          string
	recoveryCA, recoveryCert, recoveryKey string
	resume                                bool
	timeout                               time.Duration
}

func NewTenantArchiveCommands() []*cobra.Command {
	var commands []*cobra.Command
	for _, action := range []string{"backup", "inspect", "restore"} {
		action := action
		o := &tenantArchiveOptions{}
		cmd := &cobra.Command{Use: "tenant-" + action + " ARCHIVE", Short: "Tenant-scoped etcd " + action, Args: cobra.ExactArgs(1)}
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
			defer cancel()
			if action == "inspect" {
				summary, err := tenantbackup.Inspect(args[0])
				if err != nil {
					return err
				}
				return json.NewEncoder(cmd.OutOrStdout()).Encode(summary)
			}
			if o.name == "" {
				return errors.New("--name is required")
			}
			cfg, err := config.ParseConfig(o.config, o.name, nil)
			if err != nil {
				return err
			}
			layout, err := cfg.ResolveEtcdStorageLayout()
			if err != nil {
				return err
			}
			if layout.RootPrefix == "" {
				return errors.New("tenant archive commands require external etcd storePrefix")
			}
			tlsConfig := cfg.ControlPlane.BackingStore.Etcd.External.TLS
			endpoint := "https://" + strings.TrimPrefix(cfg.ControlPlane.BackingStore.Etcd.External.Endpoint, "https://")
			if action == "backup" {
				raw, err := archiveClient(endpoint, tlsConfig.CaFile, tlsConfig.CertFile, tlsConfig.KeyFile)
				if err != nil {
					return err
				}
				defer raw.Close()
				summary, err := tenantbackup.Export(ctx, raw, layout.RootPrefix, args[0])
				if err != nil {
					return err
				}
				return json.NewEncoder(cmd.OutOrStdout()).Encode(summary)
			}
			// Complete validation precedes even contacting the host or etcd.
			summary, err := tenantbackup.Inspect(args[0])
			if err != nil {
				return err
			}
			if summary.Root != layout.RootPrefix {
				return errors.New("archive root differs from configured tenant root")
			}
			if o.namespace == "" || o.hostKubeconfig == "" || o.hostContext == "" {
				return errors.New("restore requires explicit --namespace, --host-kubeconfig and --host-context")
			}
			retired, err := certificateUser(tlsConfig.CertFile, false)
			if err != nil {
				return err
			}
			recovery, err := certificateUser(o.recoveryCert, true)
			if err != nil {
				return err
			}
			if retired == recovery {
				return errors.New("recovery certificate must use a fresh identity distinct from the deployed writer")
			}
			raw, err := archiveClient(endpoint, o.recoveryCA, o.recoveryCert, o.recoveryKey)
			if err != nil {
				return err
			}
			defer raw.Close()
			admin, err := archiveClient(endpoint, o.adminCA, o.adminCert, o.adminKey)
			if err != nil {
				return err
			}
			defer admin.Close()
			loader := &clientcmd.ClientConfigLoadingRules{ExplicitPath: o.hostKubeconfig}
			hostConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, &clientcmd.ConfigOverrides{CurrentContext: o.hostContext}).ClientConfig()
			if err != nil {
				return err
			}
			hostConfig.Timeout = 15 * time.Second
			host, err := kubernetes.NewForConfig(hostConfig)
			if err != nil {
				return err
			}
			check := func(ctx context.Context) error {
				return stoppedTenant(ctx, host, o.namespace, o.name, summary.Ownership)
			}
			summary, err = tenantbackup.Restore(ctx, raw, args[0], tenantbackup.RestoreOptions{Admin: admin, RecoveryUser: recovery, RetiredUsers: []string{retired}, Resume: o.resume, CheckStopped: check, Leases: raw.Lease, RestartLeaseTTL: o.restartLeaseTTL})
			if err != nil {
				return err
			}
			return json.NewEncoder(cmd.OutOrStdout()).Encode(summary)
		}
		cmd.Flags().DurationVar(&o.timeout, "timeout", 10*time.Minute, "Maximum duration of this command")
		if action != "inspect" {
			cmd.Flags().StringVar(&o.config, "config", constants.DefaultVClusterConfigLocation, "Path to the deployed vCluster configuration")
			cmd.Flags().StringVar(&o.name, "name", "", "Deployment name")
		}
		if action == "restore" {
			cmd.Flags().StringVar(&o.namespace, "namespace", "", "Explicit host namespace")
			cmd.Flags().StringVar(&o.hostKubeconfig, "host-kubeconfig", "", "Explicit host kubeconfig file")
			cmd.Flags().StringVar(&o.hostContext, "host-context", "", "Explicit host kubeconfig context")
			cmd.Flags().StringVar(&o.adminCA, "admin-ca", "", "etcd administrator CA certificate")
			cmd.Flags().StringVar(&o.adminCert, "admin-cert", "", "etcd administrator client certificate (kept outside Pods)")
			cmd.Flags().StringVar(&o.adminKey, "admin-key", "", "etcd administrator client key")
			cmd.Flags().StringVar(&o.recoveryCA, "recovery-ca", "", "Recovery etcd CA certificate")
			cmd.Flags().StringVar(&o.recoveryCert, "recovery-cert", "", "Fresh root-scoped recovery client certificate")
			cmd.Flags().StringVar(&o.recoveryKey, "recovery-key", "", "Recovery client key")
			cmd.Flags().BoolVar(&o.resume, "resume", false, "Resume an interrupted restore using the identical archive and identity")
			cmd.Flags().BoolVar(&o.restartLeaseTTL, "restart-lease-ttl", false, "Restore leased data with fresh shared leases and restart captured remaining TTL (server minimum applies)")
		}
		commands = append(commands, cmd)
	}
	return commands
}

func archiveClient(endpoint, caFile, certFile, keyFile string) (*clientv3.Client, error) {
	if caFile == "" || certFile == "" || keyFile == "" {
		return nil, errors.New("explicit CA, client certificate and key files are required")
	}
	leaf, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, errors.New("cannot load etcd client certificate and key")
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return nil, errors.New("cannot read etcd CA certificate")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid etcd CA certificate")
	}
	return clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, MaxCallRecvMsgSize: 64 << 20, Logger: zap.NewNop(), TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{leaf}}})
}

func certificateUser(filename string, requireValid bool) (string, error) {
	b, err := os.ReadFile(filename)
	if err != nil {
		return "", errors.New("cannot read client certificate")
	}
	block, _ := pem.Decode(b)
	if block == nil || block.Type != "CERTIFICATE" {
		return "", errors.New("invalid client certificate PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", errors.New("invalid client certificate")
	}
	if cert.Subject.CommonName == "" || cert.Subject.CommonName == "root" {
		return "", errors.New("tenant client certificate must name a nonadministrator identity")
	}
	if requireValid {
		if time.Now().Before(cert.NotBefore) || time.Now().After(cert.NotAfter) {
			return "", errors.New("recovery certificate is outside its validity period")
		}
		allowed := false
		for _, usage := range cert.ExtKeyUsage {
			if usage == x509.ExtKeyUsageClientAuth {
				allowed = true
			}
		}
		if !allowed {
			return "", errors.New("recovery certificate must allow client authentication")
		}
	}
	return cert.Subject.CommonName, nil
}

// stoppedTenant deliberately does not scale workloads or read Secret values.
// It requires the retained identity/marker, an observed zero-replica controller
// and no live or terminating control-plane Pods. Both chart workload kinds are
// supported; ambiguity is rejected.
func stoppedTenant(ctx context.Context, host kubernetes.Interface, namespace, name string, owner tenantbackup.Ownership) error {
	secret, err := host.CoreV1().Secrets(namespace).Get(ctx, "vc-config-"+name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read deployment identity: %w", err)
	}
	if string(secret.UID) != owner.DeploymentUID {
		return errors.New("host deployment UID differs from archive")
	}
	marker, err := host.CoreV1().ConfigMaps(namespace).Get(ctx, setupconfig.StorageLayoutMarkerName(name), metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read retained layout marker: %w", err)
	}
	for key, want := range map[string]string{"version": owner.Version, "rootPrefix": owner.Root, "deploymentUID": owner.DeploymentUID, "etcdEndpoint": owner.Endpoint} {
		if marker.Data[key] != want {
			return errors.New("host layout marker differs from archive")
		}
	}
	sets, err := host.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	deployments, err := host.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	owners := map[types.UID]bool{}
	count := 0
	for _, set := range sets.Items {
		if set.Name != name {
			continue
		}
		count++
		owners[set.UID] = true
		if set.UID == "" || set.Spec.Replicas == nil || *set.Spec.Replicas != 0 || set.Status.ObservedGeneration < set.Generation || set.Status.Replicas != 0 || set.Status.CurrentReplicas != 0 || set.Status.ReadyReplicas != 0 || set.Status.UpdatedReplicas != 0 || set.Status.AvailableReplicas != 0 {
			return errors.New("StatefulSet must have observed zero replicas")
		}
	}
	for _, deployment := range deployments.Items {
		if deployment.Name != name {
			continue
		}
		count++
		owners[deployment.UID] = true
		if deployment.UID == "" || deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 0 || deployment.Status.ObservedGeneration < deployment.Generation || deployment.Status.Replicas != 0 || deployment.Status.ReadyReplicas != 0 || deployment.Status.AvailableReplicas != 0 || deployment.Status.UpdatedReplicas != 0 || deployment.Status.UnavailableReplicas != 0 || (deployment.Status.TerminatingReplicas != nil && *deployment.Status.TerminatingReplicas != 0) {
			return errors.New("Deployment must have observed zero replicas")
		}
	}
	if count != 1 {
		return errors.New("exactly one named control-plane StatefulSet or Deployment is required")
	}
	replicas, err := host.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, set := range replicas.Items {
		owned := false
		for _, ref := range set.OwnerReferences {
			if owners[ref.UID] {
				owned = true
			}
		}
		if !owned {
			continue
		}
		owners[set.UID] = true
		if set.Spec.Replicas == nil || *set.Spec.Replicas != 0 || set.Status.ObservedGeneration < set.Generation || set.Status.Replicas != 0 || set.Status.ReadyReplicas != 0 || set.Status.AvailableReplicas != 0 || (set.Status.TerminatingReplicas != nil && *set.Status.TerminatingReplicas != 0) {
			return errors.New("owned ReplicaSet has not observed zero replicas")
		}
	}
	pods, err := host.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		owned := false
		for _, ref := range pod.OwnerReferences {
			if owners[ref.UID] {
				owned = true
			}
		}
		if owned || (pod.Labels["app"] == "vcluster" && pod.Labels["release"] == name) {
			return errors.New("control-plane Pods remain, including terminating or orphaned Pods")
		}
	}
	return nil
}
