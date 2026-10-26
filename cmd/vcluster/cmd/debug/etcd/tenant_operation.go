package etcd

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/loft-sh/vcluster/pkg/config"
	setupconfig "github.com/loft-sh/vcluster/pkg/setup/config"
	"github.com/loft-sh/vcluster/pkg/tenantbackup"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
)

type operationOptions struct {
	newOperation                             bool
	name, namespace, kubeconfig, hostContext string
	adminCA, adminCert, adminKey             string
	issuerCert, issuerKey, tlsSecret         string
	caItem, certItem, keyItem, root, archive string
	retired                                  []string
	resume, commit, rollback, restartTTL     bool
	timeout, validity                        time.Duration
}

// operationState is stored in a host Secret, never printed. Original values and
// generated private material stay inside Kubernetes Secret storage; the issuer
// and administrator keys are never uploaded. Mutations use resourceVersion CAS.
type operationState struct {
	TemplateSHA                                string
	Version                                    int `json:"version"`
	ID, Mode, Phase                            string
	Name, Namespace                            string
	Source, Target                             tenantbackup.Ownership
	Access                                     *tenantbackup.AccessPlan
	Config                                     []byte
	TLSName, TLSUID, CAItem, CertItem, KeyItem string
	OriginalTLS                                map[string][]byte
	Kind, WorkloadUID                          string
	Replicas                                   int32
	ArchiveSHA                                 string
	RestartTTL                                 bool
}
type operation struct {
	host   kubernetes.Interface
	admin  *clientv3.Client
	o      *operationOptions
	secret *corev1.Secret
	s      operationState
}

func NewTenantOperationCommands() []*cobra.Command {
	var out []*cobra.Command
	for _, mode := range []string{"migrate", "recover"} {
		o := &operationOptions{}
		cmd := &cobra.Command{Use: "tenant-" + mode + " ARCHIVE", Short: "Journaled offline tenant " + mode + ", writer rotation and cutover", Args: cobra.ExactArgs(1)}
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			if o.name == "" || o.namespace == "" || o.kubeconfig == "" || o.hostContext == "" {
				return errors.New("explicit --name, --namespace, --host-kubeconfig and --host-context are required")
			}
			if o.newOperation && (o.resume || o.rollback) {
				return errors.New("--new-operation cannot be combined with --resume or --rollback")
			}
			if o.rollback && o.commit {
				return errors.New("--rollback and --commit are mutually exclusive")
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
			defer cancel()
			rc, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(&clientcmd.ClientConfigLoadingRules{ExplicitPath: o.kubeconfig}, &clientcmd.ConfigOverrides{CurrentContext: o.hostContext}).ClientConfig()
			if err != nil {
				return err
			}
			rc.Timeout = 15 * time.Second
			host, err := kubernetes.NewForConfig(rc)
			if err != nil {
				return err
			}
			op := &operation{host: host, o: o}
			o.archive = args[0]
			if err := op.run(ctx, mode); err != nil {
				return err
			}
			return json.NewEncoder(cmd.OutOrStdout()).Encode(map[string]string{"operation": op.s.ID, "mode": op.s.Mode, "phase": op.s.Phase})
		}
		f := cmd.Flags()
		f.StringVar(&o.name, "name", "", "Deployment name")
		f.StringVar(&o.namespace, "namespace", "", "Explicit host namespace")
		f.StringVar(&o.kubeconfig, "host-kubeconfig", "", "Explicit host kubeconfig file")
		f.StringVar(&o.hostContext, "host-context", "", "Explicit host context")
		f.StringVar(&o.adminCA, "admin-ca", "", "etcd administrator server CA")
		f.StringVar(&o.adminCert, "admin-cert", "", "etcd administrator certificate, outside Pods")
		f.StringVar(&o.adminKey, "admin-key", "", "etcd administrator key")
		f.StringVar(&o.issuerCert, "issuer-cert", "", "Authorized local client-signing CA certificate")
		f.StringVar(&o.issuerKey, "issuer-key", "", "Authorized local client-signing CA key; never uploaded")
		f.StringVar(&o.tlsSecret, "tls-secret", "", "Existing dedicated Secret mounted for external etcd TLS")
		f.StringVar(&o.caItem, "ca-item", "ca.crt", "CA key in the TLS Secret")
		f.StringVar(&o.certItem, "cert-item", "tls.crt", "Certificate key in the TLS Secret")
		f.StringVar(&o.keyItem, "key-item", "tls.key", "Private-key key in the TLS Secret")
		f.StringVar(&o.root, "store-prefix", "", "New tenant root; required for migration")
		f.StringSliceVar(&o.retired, "retire-user", nil, "Additional explicitly retired etcd writer identities")
		f.BoolVar(&o.newOperation, "new-operation", false, "Archive a completed journal and start a new operation")
		f.BoolVar(&o.resume, "resume", false, "Resume the journaled operation with the identical archive")
		f.BoolVar(&o.commit, "commit", false, "Cut over after verification and restore the original replica count")
		f.BoolVar(&o.rollback, "rollback", false, "Roll back a migration before cutover starts")
		f.BoolVar(&o.restartTTL, "restart-lease-ttl", false, "Restart captured remaining lease TTL using fresh leases")
		f.DurationVar(&o.validity, "certificate-validity", 24*time.Hour, "Replacement client certificate lifetime, bounded by issuer expiry")
		f.DurationVar(&o.timeout, "timeout", 10*time.Minute, "Maximum duration; interruption leaves a resumable journal")
		out = append(out, cmd)
	}
	return out
}
func operationSecretName(name string) string { return "vc-etcd-operation-" + name }
func (p *operation) save(ctx context.Context, phase string) error {
	p.s.Phase = phase
	b, err := json.Marshal(p.s)
	if err != nil {
		return err
	}
	next := p.secret.DeepCopy()
	next.Data["operation.json"] = b
	result, err := p.host.CoreV1().Secrets(p.s.Namespace).Update(ctx, next, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("persist operation phase %s; retry with --resume: %w", phase, err)
	}
	p.secret = result
	return nil
}
func (p *operation) run(ctx context.Context, mode string) error {
	journal, err := p.host.CoreV1().Secrets(p.o.namespace).Get(ctx, operationSecretName(p.o.name), metav1.GetOptions{})
	if err == nil && p.o.newOperation {
		if err := p.archiveJournal(ctx, journal); err != nil {
			return err
		}
		journal = nil
		err = kerrors.NewNotFound(corev1.Resource("secrets"), operationSecretName(p.o.name))
	}
	if err == nil {
		if !p.o.resume && !p.o.rollback {
			return errors.New("an operation journal already exists; use --resume")
		}
		p.secret = journal
		if err := json.Unmarshal(journal.Data["operation.json"], &p.s); err != nil || p.s.Version != 1 || p.s.ID == "" || p.s.Mode != mode || p.s.Name != p.o.name || p.s.Namespace != p.o.namespace || p.s.Access == nil {
			return errors.New("operation journal does not match request")
		}
		if p.o.root != "" {
			root, err := tenantbackup.NormalizeRoot(p.o.root)
			if err != nil || root != p.s.Target.Root {
				return errors.New("target root differs from journal")
			}
		}
		if p.o.tlsSecret != "" && p.o.tlsSecret != p.s.TLSName {
			return errors.New("TLS Secret differs from journal")
		}
		if p.o.restartTTL != p.s.RestartTTL {
			if !p.s.RestartTTL && p.o.restartTTL && p.s.Phase == "copy" && p.s.ArchiveSHA == "" {
				p.s.RestartTTL = true
				if err := p.save(ctx, p.s.Phase); err != nil {
					return err
				}
			} else {
				return errors.New("lease policy differs from journal")
			}
		}
	} else if kerrors.IsNotFound(err) {
		if p.o.resume || p.o.rollback {
			return errors.New("no operation journal exists")
		}
		if err := p.begin(ctx, mode); err != nil {
			return err
		}
	} else {
		return err
	}
	p.admin, err = archiveClient("https://"+p.s.Target.Endpoint, p.o.adminCA, p.o.adminCert, p.o.adminKey)
	if err != nil {
		return err
	}
	defer p.admin.Close()
	if !slices.Contains([]string{"prepare", "reserve", "copy", "verified", "cutover", "starting", "complete", "rollback", "rolledback"}, p.s.Phase) {
		return errors.New("unknown operation phase")
	}
	if p.s.Access.Root != p.s.Target.Root || p.s.Access.Legacy != (mode == "migrate") {
		return errors.New("access plan differs from operation")
	}
	if p.s.Phase == "complete" {
		return nil
	}
	if p.s.Phase == "rolledback" {
		return p.scale(ctx, p.s.Replicas)
	}
	if p.o.rollback {
		return p.rollback(ctx)
	}
	if p.s.Phase == "rollback" {
		return errors.New("rollback is in progress; use --rollback")
	}
	if p.s.Phase == "prepare" {
		if err := p.stop(ctx); err != nil {
			return err
		}

		if err := tenantbackup.PrepareAccessWithCheck(ctx, p.admin, p.s.Access, func() error {
			raw, err := p.recoveryClient()
			if err != nil {
				return err
			}
			defer raw.Close()
			_, err = raw.Get(ctx, p.s.Target.Root+tenantbackup.ClaimSuffix)
			return err
		}); err != nil {
			return err
		}
		if mode == "migrate" {
			// Treat a lost reservation response as resumable only after persisting
			// the intention. Resume checks both exact barriers before continuing.
			if err := p.save(ctx, "reserve"); err != nil {
				return err
			}
		} else {
			if err := p.save(ctx, "copy"); err != nil {
				return err
			}
		}
	}
	if p.s.Phase == "reserve" {
		if err := p.stopped(ctx, false); err != nil {
			return err
		}
		if err := tenantbackup.AuditAccess(ctx, p.admin, p.s.Access); err != nil {
			return err
		}
		if err := tenantbackup.ReserveMigration(ctx, p.admin, p.s.Target, p.s.ID, false); err != nil {
			if err := tenantbackup.ReserveMigration(ctx, p.admin, p.s.Target, p.s.ID, true); err != nil {
				return err
			}
		}
		if err := p.save(ctx, "copy"); err != nil {
			return err
		}
	}
	if p.s.Phase == "copy" || p.s.Phase == "verified" {
		if err := p.stopped(ctx, false); err != nil {
			return err
		}
		if err := tenantbackup.AuditAccess(ctx, p.admin, p.s.Access); err != nil {
			return err
		}
		if mode == "migrate" {
			if err := tenantbackup.ReserveMigration(ctx, p.admin, p.s.Target, p.s.ID, true); err != nil {
				return err
			}
			if _, err := os.Stat(p.o.archive); os.IsNotExist(err) {
				if p.s.ArchiveSHA != "" {
					return errors.New("journaled archive is missing")
				}
				if _, err := tenantbackup.ExportLegacy(ctx, p.admin, p.s.Target, p.s.ID, p.o.archive); err != nil {
					return err
				}
			} else if err != nil {
				return err
			}
		}
		sum, err := tenantbackup.Inspect(p.o.archive)
		if err != nil {
			return err
		}
		if sum.Ownership != p.s.Target {
			return errors.New("archive identity differs from operation")
		}
		if p.s.ArchiveSHA != "" && p.s.ArchiveSHA != sum.SHA256 {
			return errors.New("archive differs from journal")
		}
		if sum.LeasedEntries > 0 && !p.s.RestartTTL {
			return errors.New("leased data requires --restart-lease-ttl; operation remains safely stopped")
		}
		p.s.ArchiveSHA = sum.SHA256
		if err := p.save(ctx, p.s.Phase); err != nil {
			return err
		}
		recovery, err := p.recoveryClient()
		if err != nil {
			return err
		}
		defer recovery.Close()
		if _, err := recovery.Get(ctx, p.s.Target.Root+tenantbackup.ClaimSuffix); err != nil {
			return errors.New("replacement identity cannot authenticate/read target claim")
		}
		state, err := p.admin.Get(ctx, p.s.Target.Root+tenantbackup.RestoreSuffix)
		if err != nil {
			return err
		}
		receipt, err := p.admin.Get(ctx, p.s.Target.Root+tenantbackup.RestoreReceiptSuffix)
		if err != nil {
			return err
		}
		matchingReceipt := false
		if len(receipt.Kvs) == 1 {
			var r struct {
				Archive, RecoveryUser, Phase string
				Owner                        tenantbackup.Ownership
			}
			if json.Unmarshal(receipt.Kvs[0].Value, &r) == nil && r.Archive == sum.SHA256 && r.RecoveryUser == p.s.Access.Recovery && r.Phase == "complete" && r.Owner == p.s.Target {
				matchingReceipt = true
			}
		}
		opts := tenantbackup.RestoreOptions{ExpectedSHA: p.s.ArchiveSHA, Admin: p.admin, RecoveryUser: p.s.Access.Recovery, RetiredUsers: p.s.Access.Retired, CheckStopped: func(ctx context.Context) error { return p.stopped(ctx, false) }, Resume: len(state.Kvs) > 0 || matchingReceipt, Leases: recovery.Lease, RestartLeaseTTL: p.s.RestartTTL}
		if mode == "migrate" {
			_, err = tenantbackup.CopyMigration(ctx, p.admin, p.o.archive, opts, tenantbackup.MigrationGuard{Token: p.s.ID, Access: p.s.Access})
		} else {
			_, err = tenantbackup.Restore(ctx, recovery, p.o.archive, opts)
		}
		if err != nil {
			return err
		}
		if p.s.Phase != "verified" {
			if err := p.save(ctx, "verified"); err != nil {
				return err
			}
		}
		if !p.o.commit {
			return nil
		}
		if err := p.save(ctx, "cutover"); err != nil {
			return err
		}
	}
	if p.s.Phase == "cutover" {
		if !p.o.commit {
			return errors.New("cutover is in progress; resume with --commit")
		}
		if err := p.stopped(ctx, true); err != nil {
			return err
		}
		if err := tenantbackup.AuditAccess(ctx, p.admin, p.s.Access); err != nil {
			return err
		}
		if err := p.cutover(ctx); err != nil {
			return err
		}
		if mode == "migrate" {
			if err := tenantbackup.FinishMigration(ctx, p.admin, p.s.Target, p.s.ID); err != nil {
				return err
			}
		}
		if err := p.save(ctx, "starting"); err != nil {
			return err
		}
	}
	if p.s.Phase == "starting" {
		if !p.o.commit {
			return errors.New("startup is pending; resume with --commit")
		}
		if err := p.scale(ctx, p.s.Replicas); err != nil {
			return err
		}
		if err := p.waitReady(ctx); err != nil {
			return fmt.Errorf("cutover committed but workload readiness is pending; resume with --commit: %w", err)
		}
		return p.save(ctx, "complete")
	}

	return nil
}
func (p *operation) begin(ctx context.Context, mode string) error {
	if p.o.tlsSecret == "" {
		return errors.New("--tls-secret is required")
	}
	if p.o.caItem == p.o.certItem || p.o.caItem == p.o.keyItem || p.o.certItem == p.o.keyItem {
		return errors.New("TLS Secret item names must differ")
	}
	secret, err := p.host.CoreV1().Secrets(p.o.namespace).Get(ctx, "vc-config-"+p.o.name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if secret.UID == "" || len(secret.Data["config.yaml"]) == 0 || secret.Immutable != nil && *secret.Immutable {
		return errors.New("a mutable deployment configuration Secret with config.yaml and stable UID is required")
	}
	cfg, err := config.ParseConfigBytes(secret.Data["config.yaml"], p.o.name, nil)
	if err != nil {
		return errors.New("cannot parse deployed configuration")
	}
	sourceLayout, err := cfg.ResolveEtcdStorageLayout()
	if err != nil {
		return err
	}
	if mode == "migrate" {
		if sourceLayout.RootPrefix != "" {
			return errors.New("migration requires a legacy source")
		}
		cfg.ControlPlane.BackingStore.Etcd.External.StorePrefix = p.o.root
		if p.o.root == "" {
			return errors.New("migration requires --store-prefix")
		}
		// Legacy overrides cannot be inferred safely, including server overrides.
		for _, arg := range cfg.ControlPlane.Distro.K8S.APIServer.ExtraArgs {
			flag, _, _ := strings.Cut(strings.ReplaceAll(strings.TrimLeft(arg, "-"), "_", "-"), "=")
			if strings.HasPrefix(flag, "etcd-") {
				return errors.New("legacy etcd API-server overrides are unsupported for migration")
			}
		}
	} else if sourceLayout.RootPrefix == "" {
		return errors.New("recovery requires an existing tenant root")
	}
	target, err := cfg.ResolveEtcdStorageLayout()
	if err != nil {
		return err
	}
	endpoint := strings.TrimSuffix(strings.TrimPrefix(cfg.ControlPlane.BackingStore.Etcd.External.Endpoint, "https://"), "/")
	owner := tenantbackup.Ownership{Version: "1", Root: target.RootPrefix, Endpoint: endpoint, DeploymentUID: string(secret.UID)}
	old := owner
	old.Root = sourceLayout.RootPrefix
	if old.Root == "" {
		old.Endpoint = ""
	}
	marker, err := p.host.CoreV1().ConfigMaps(p.o.namespace).Get(ctx, setupconfig.StorageLayoutMarkerName(p.o.name), metav1.GetOptions{})
	if kerrors.IsNotFound(err) && mode == "migrate" {
		marker = nil
	} else if err != nil {
		return err
	}
	if marker != nil && !markerMatches(marker, old) {
		return errors.New("retained layout marker differs from deployment")
	}
	tlsSecret, err := p.host.CoreV1().Secrets(p.o.namespace).Get(ctx, p.o.tlsSecret, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if tlsSecret.UID == "" || tlsSecret.Name == secret.Name || tlsSecret.Immutable != nil && *tlsSecret.Immutable {
		return errors.New("a mutable dedicated TLS Secret is required")
	}
	pair, err := tls.X509KeyPair(tlsSecret.Data[p.o.certItem], tlsSecret.Data[p.o.keyItem])
	if err != nil {
		return errors.New("TLS Secret certificate/key are invalid")
	}
	cert, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil || cert.Subject.CommonName == "" || cert.Subject.CommonName == "root" {
		return errors.New("deployed writer must be a nonadministrator certificate identity")
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return err
	}
	id := hex.EncodeToString(random)
	user := "vc-recovery-" + id
	_, leaf, key, err := issueClient(p.o.issuerCert, p.o.issuerKey, user, p.o.validity)
	if err != nil {
		return err
	}
	// The provisioned client is authenticated before old writer access is revoked.
	if len(tlsSecret.Data[p.o.caItem]) == 0 {
		return errors.New("TLS Secret CA is missing")
	}
	retired := append([]string{cert.Subject.CommonName}, p.o.retired...)
	p.s = operationState{Version: 1, ID: id, Mode: mode, Phase: "prepare", Name: p.o.name, Namespace: p.o.namespace, Source: old, Target: owner, Config: slices.Clone(secret.Data["config.yaml"]), TLSName: tlsSecret.Name, TLSUID: string(tlsSecret.UID), OriginalTLS: tlsSecret.DeepCopy().Data, CAItem: p.o.caItem, CertItem: p.o.certItem, KeyItem: p.o.keyItem, RestartTTL: p.o.restartTTL}
	if err := p.selectWorkload(ctx, cfg); err != nil {
		return err
	}
	var recoverySummary *tenantbackup.Summary
	if mode == "recover" {
		sum, err := tenantbackup.Inspect(p.o.archive)
		if err != nil {
			return err
		}
		if sum.Ownership != owner {
			return errors.New("archive does not match deployed identity")
		}
		if sum.LeasedEntries > 0 && !p.o.restartTTL {
			return errors.New("leased archive requires --restart-lease-ttl")
		}
		p.s.ArchiveSHA = sum.SHA256
		recoverySummary = sum
	}
	admin, err := archiveClient("https://"+endpoint, p.o.adminCA, p.o.adminCert, p.o.adminKey)
	if err != nil {
		return err
	}
	defer admin.Close()
	if recoverySummary != nil {
		for _, suffix := range []string{tenantbackup.RestoreSuffix, tenantbackup.MigrationSuffix, tenantbackup.FenceSuffix} {
			barrier, err := admin.Get(ctx, owner.Root+suffix)
			if err != nil {
				return err
			}
			if len(barrier.Kvs) > 0 {
				return errors.New("existing lifecycle barrier requires its original recovery workflow")
			}
		}
		claim, err := admin.Get(ctx, owner.Root+tenantbackup.ClaimSuffix)
		if err != nil {
			return err
		}
		var actual tenantbackup.Ownership
		if claim.Header.ClusterId != recoverySummary.ClusterID || len(claim.Kvs) != 1 || json.Unmarshal(claim.Kvs[0].Value, &actual) != nil || actual != owner {
			return errors.New("archive cluster or etcd ownership differs from deployment")
		}
	}
	if mode == "migrate" {
		existing, err := admin.Get(ctx, owner.Root, clientv3.WithPrefix(), clientv3.WithLimit(1))
		if err != nil {
			return err
		}
		if len(existing.Kvs) != 0 {
			return errors.New("migration destination must be empty")
		}
		barrier, err := admin.Get(ctx, tenantbackup.LegacyMigrationKey)
		if err != nil {
			return err
		}
		if len(barrier.Kvs) > 0 {
			return errors.New("legacy source is already reserved or retired")
		}
	}
	p.s.Access, err = tenantbackup.PlanAccess(ctx, admin, owner.Root, user, retired, mode == "migrate")
	if err != nil {
		return err
	}
	b, err := json.Marshal(p.s)
	if err != nil {
		return err
	}
	// Keep the existing server-trust bundle. Issuer CA signs the client leaf;
	// it need not be the server's CA, and is included only with the leaf chain.
	p.secret, err = p.host.CoreV1().Secrets(p.o.namespace).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: operationSecretName(p.o.name), Labels: map[string]string{"app": "vcluster-etcd-operation", "release": p.o.name}}, Data: map[string][]byte{"operation.json": b, "ca.crt": slices.Clone(tlsSecret.Data[p.o.caItem]), "tls.crt": leaf, "tls.key": key}}, metav1.CreateOptions{})
	return err
}
func issueClient(certPath, keyPath, user string, validity time.Duration) ([]byte, []byte, []byte, error) {
	if validity <= 0 || validity > 7*24*time.Hour {
		return nil, nil, nil, errors.New("certificate validity must be positive and at most seven days")
	}
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, nil, nil, errors.New("cannot load authorized client-signing CA")
	}
	ca, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, nil, nil, err
	}
	signer, ok := pair.PrivateKey.(crypto.Signer)
	if !ok || !ca.IsCA || ca.KeyUsage&x509.KeyUsageCertSign == 0 || time.Now().Before(ca.NotBefore) || !time.Now().Before(ca.NotAfter) {
		return nil, nil, nil, errors.New("issuer is not a currently valid signing CA")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, nil, err
	}
	expiry := time.Now().Add(validity)
	if expiry.After(ca.NotAfter) {
		expiry = ca.NotAfter
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: user}, NotBefore: time.Now().Add(-time.Minute), NotAfter: expiry, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, signer)
	if err != nil {
		return nil, nil, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, nil, err
	}
	leaf := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	for _, chain := range pair.Certificate {
		leaf = append(leaf, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: chain})...)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}), leaf, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}
func (p *operation) recoveryClient() (*clientv3.Client, error) {
	pair, err := tls.X509KeyPair(p.secret.Data["tls.crt"], p.secret.Data["tls.key"])
	if err != nil {
		return nil, errors.New("journaled replacement credential is invalid")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(p.secret.Data["ca.crt"]) {
		return nil, errors.New("journaled CA is invalid")
	}
	return clientv3.New(clientv3.Config{Endpoints: []string{"https://" + p.s.Target.Endpoint}, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{pair}}, DialTimeout: 5 * time.Second, MaxCallRecvMsgSize: 64 << 20, Logger: zap.NewNop()})
}
func (p *operation) selectWorkload(ctx context.Context, cfg *config.VirtualClusterConfig) error {
	sets, err := p.host.AppsV1().StatefulSets(p.s.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	deps, err := p.host.AppsV1().Deployments(p.s.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	count := 0
	var spec *corev1.PodSpec
	for _, s := range sets.Items {
		if s.Name == p.s.Name {
			count++
			p.s.Kind = "StatefulSet"
			p.s.WorkloadUID = string(s.UID)
			if s.Spec.Replicas != nil {
				p.s.Replicas = *s.Spec.Replicas
			}
			spec = &s.Spec.Template.Spec
		}
	}
	for _, s := range deps.Items {
		if s.Name == p.s.Name {
			count++
			p.s.Kind = "Deployment"
			p.s.WorkloadUID = string(s.UID)
			if s.Spec.Replicas != nil {
				p.s.Replicas = *s.Spec.Replicas
			}
			spec = &s.Spec.Template.Spec
		}
	}
	if count != 1 || p.s.WorkloadUID == "" {
		return errors.New("exactly one named control-plane workload with a stable UID is required")
	}
	p.s.TemplateSHA = podSpecHash(spec)
	if p.s.Replicas > 1 {
		return errors.New("automated operations currently support single-replica deployments only")
	}
	// Only a direct dedicated Secret volume is supported. This excludes CSI,
	// projected sources and subPath mounts whose rotation semantics differ.
	files := map[string]string{p.s.CAItem: cfg.ControlPlane.BackingStore.Etcd.External.TLS.CaFile, p.s.CertItem: cfg.ControlPlane.BackingStore.Etcd.External.TLS.CertFile, p.s.KeyItem: cfg.ControlPlane.BackingStore.Etcd.External.TLS.KeyFile}
	matched := map[string]bool{}
	for _, c := range spec.Containers {
		if c.Name != "syncer" {
			continue
		}
		for _, mount := range c.VolumeMounts {
			if mount.SubPath != "" || mount.SubPathExpr != "" {
				continue
			}
			for _, v := range spec.Volumes {
				if v.Name != mount.Name || v.Secret == nil || v.Secret.SecretName != p.s.TLSName {
					continue
				}
				for item, want := range files {
					file := item
					if len(v.Secret.Items) > 0 {
						file = ""
						for _, it := range v.Secret.Items {
							if it.Key == item {
								file = it.Path
							}
						}
					}
					if file != "" && path.Join(mount.MountPath, file) == want {
						matched[item] = true
					}
				}
			}
		}
	}
	if len(matched) != 3 {
		return errors.New("configured etcd TLS paths must come from the selected dedicated Secret mounted directly in syncer")
	}
	// Reject another workload mounting the same TLS Secret; rotating a shared
	// mount would silently change an unrelated control plane's identity.
	for _, s := range sets.Items {
		if string(s.UID) != p.s.WorkloadUID {
			for _, v := range s.Spec.Template.Spec.Volumes {
				if v.Secret != nil && v.Secret.SecretName == p.s.TLSName {
					return errors.New("TLS Secret is shared with another workload")
				}
			}
		}
	}
	for _, s := range deps.Items {
		if string(s.UID) != p.s.WorkloadUID {
			for _, v := range s.Spec.Template.Spec.Volumes {
				if v.Secret != nil && v.Secret.SecretName == p.s.TLSName {
					return errors.New("TLS Secret is shared with another workload")
				}
			}
		}
	}
	return nil
}
func (p *operation) scale(ctx context.Context, replicas int32) error {
	if p.s.Kind == "Deployment" {
		d, err := p.host.AppsV1().Deployments(p.s.Namespace).Get(ctx, p.s.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if string(d.UID) != p.s.WorkloadUID || (p.s.TemplateSHA != "" && podSpecHash(&d.Spec.Template.Spec) != p.s.TemplateSHA) {
			return errors.New("workload identity changed")
		}
		if d.Spec.Replicas != nil && *d.Spec.Replicas == replicas {
			return nil
		}
		d.Spec.Replicas = &replicas
		_, err = p.host.AppsV1().Deployments(p.s.Namespace).Update(ctx, d, metav1.UpdateOptions{})
		return err
	}
	if p.s.Kind != "StatefulSet" {
		return errors.New("invalid workload kind")
	}
	d, err := p.host.AppsV1().StatefulSets(p.s.Namespace).Get(ctx, p.s.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(d.UID) != p.s.WorkloadUID || (p.s.TemplateSHA != "" && podSpecHash(&d.Spec.Template.Spec) != p.s.TemplateSHA) {
		return errors.New("workload identity changed")
	}
	if d.Spec.Replicas != nil && *d.Spec.Replicas == replicas {
		return nil
	}
	d.Spec.Replicas = &replicas
	_, err = p.host.AppsV1().StatefulSets(p.s.Namespace).Update(ctx, d, metav1.UpdateOptions{})
	return err
}
func markerMatches(m *corev1.ConfigMap, o tenantbackup.Ownership) bool {
	return m.Data["version"] == o.Version && m.Data["rootPrefix"] == o.Root && m.Data["deploymentUID"] == o.DeploymentUID && m.Data["etcdEndpoint"] == o.Endpoint
}
func (p *operation) stopped(ctx context.Context, cutover bool) error {
	// CAS every host identity/config access. A reconciler changing values must
	// be suspended; unrelated edits are rejected instead of overwritten.
	cfg, err := p.host.CoreV1().Secrets(p.s.Namespace).Get(ctx, "vc-config-"+p.s.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(cfg.UID) != p.s.Target.DeploymentUID {
		return errors.New("configuration Secret UID changed")
	}
	target, err := p.targetConfig()
	if err != nil {
		return err
	}
	if string(cfg.Data["config.yaml"]) != string(p.s.Config) && (!cutover || string(cfg.Data["config.yaml"]) != string(target)) {
		return errors.New("deployment configuration changed during operation")
	}
	m, err := p.host.CoreV1().ConfigMaps(p.s.Namespace).Get(ctx, setupconfig.StorageLayoutMarkerName(p.s.Name), metav1.GetOptions{})
	if kerrors.IsNotFound(err) {
		if !cutover && p.s.Source.Root != "" {
			return errors.New("layout marker disappeared")
		}
	} else if err != nil {
		return err
	} else if !markerMatches(m, p.s.Source) && (!cutover || !markerMatches(m, p.s.Target)) {
		return errors.New("layout marker changed")
	}
	tlsSecret, err := p.host.CoreV1().Secrets(p.s.Namespace).Get(ctx, p.s.TLSName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(tlsSecret.UID) != p.s.TLSUID || (!reflect.DeepEqual(tlsSecret.Data, p.s.OriginalTLS) && (!cutover || !reflect.DeepEqual(tlsSecret.Data, p.rotatedTLS()))) {
		return errors.New("TLS Secret identity or contents changed")
	}
	// Verify controller UID in addition to the existing strict stopped gate.
	if p.s.Kind == "Deployment" {
		d, err := p.host.AppsV1().Deployments(p.s.Namespace).Get(ctx, p.s.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if string(d.UID) != p.s.WorkloadUID || (p.s.TemplateSHA != "" && podSpecHash(&d.Spec.Template.Spec) != p.s.TemplateSHA) {
			return errors.New("workload UID changed")
		}
	} else {
		d, err := p.host.AppsV1().StatefulSets(p.s.Namespace).Get(ctx, p.s.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if string(d.UID) != p.s.WorkloadUID || (p.s.TemplateSHA != "" && podSpecHash(&d.Spec.Template.Spec) != p.s.TemplateSHA) {
			return errors.New("workload UID changed")
		}
	}
	return stoppedWorkload(ctx, p.host, p.s.Namespace, p.s.Name)
}
func (p *operation) targetConfig() ([]byte, error) {
	var all map[string]interface{}
	if err := yaml.Unmarshal(p.s.Config, &all); err != nil {
		return nil, err
	}
	current := all
	for _, key := range []string{"controlPlane", "backingStore", "etcd", "external"} {
		next, ok := current[key].(map[string]interface{})
		if !ok {
			next = map[string]interface{}{}
			current[key] = next
		}
		current = next
	}
	current["storePrefix"] = strings.TrimSuffix(p.s.Target.Root, "/")
	if p.s.Mode == "recover" {
		return slices.Clone(p.s.Config), nil
	}
	return yaml.Marshal(all)
}
func (p *operation) cutover(ctx context.Context) error {
	cfg, err := p.host.CoreV1().Secrets(p.s.Namespace).Get(ctx, "vc-config-"+p.s.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	target, err := p.targetConfig()
	if err != nil {
		return err
	}
	if string(cfg.UID) != p.s.Target.DeploymentUID {
		return errors.New("configuration identity changed")
	}
	if string(cfg.Data["config.yaml"]) != string(target) {
		if string(cfg.Data["config.yaml"]) != string(p.s.Config) {
			return errors.New("configuration changed")
		}
		cfg.Data["config.yaml"] = target
		if _, err := p.host.CoreV1().Secrets(p.s.Namespace).Update(ctx, cfg, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	tlsSecret, err := p.host.CoreV1().Secrets(p.s.Namespace).Get(ctx, p.s.TLSName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(tlsSecret.UID) != p.s.TLSUID {
		return errors.New("TLS Secret identity changed")
	}
	rotated := p.rotatedTLS()
	if !reflect.DeepEqual(tlsSecret.Data, rotated) {
		if !reflect.DeepEqual(tlsSecret.Data, p.s.OriginalTLS) {
			return errors.New("TLS Secret changed")
		}
		tlsSecret.Data = rotated
		if _, err := p.host.CoreV1().Secrets(p.s.Namespace).Update(ctx, tlsSecret, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	name := setupconfig.StorageLayoutMarkerName(p.s.Name)
	marker, err := p.host.CoreV1().ConfigMaps(p.s.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		if markerMatches(marker, p.s.Target) {
			return nil
		}
		if !markerMatches(marker, p.s.Source) {
			return errors.New("layout marker changed during cutover")
		}
		if err := p.host.CoreV1().ConfigMaps(p.s.Namespace).Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &marker.UID, ResourceVersion: &marker.ResourceVersion}}); err != nil {
			return err
		}
	} else if !kerrors.IsNotFound(err) {
		return err
	}
	immutable := true
	_, err = p.host.CoreV1().ConfigMaps(p.s.Namespace).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}, Immutable: &immutable, Data: map[string]string{"version": p.s.Target.Version, "rootPrefix": p.s.Target.Root, "deploymentUID": p.s.Target.DeploymentUID, "etcdEndpoint": p.s.Target.Endpoint}}, metav1.CreateOptions{})
	return err
}
func (p *operation) rollback(ctx context.Context) error {
	if p.s.Mode != "migrate" || !slices.Contains([]string{"prepare", "reserve", "copy", "verified", "rollback"}, p.s.Phase) {
		return errors.New("rollback is supported only for migration before cutover; recovery and post-cutover rollback are refused")
	}
	if err := p.stop(ctx); err != nil {
		return err
	}
	if err := p.save(ctx, "rollback"); err != nil {
		return err
	}
	source, err := p.admin.Get(ctx, tenantbackup.LegacyMigrationKey)
	if err != nil {
		return err
	}
	if len(source.Kvs) > 0 {
		if string(source.Kvs[0].Value) != p.s.ID {
			return errors.New("legacy reservation belongs to another operation")
		}
		if err := tenantbackup.AbortMigration(ctx, p.admin, p.s.Target, p.s.ID); err != nil {
			return err
		}
	} else {
		dst, err := p.admin.Get(ctx, p.s.Target.Root, clientv3.WithPrefix(), clientv3.WithLimit(1))
		if err != nil {
			return err
		}
		if len(dst.Kvs) > 0 {
			return errors.New("target data exists without the matching source barrier")
		}
	}
	if err := tenantbackup.RollbackAccess(ctx, p.admin, p.s.Access); err != nil {
		return err
	}
	// Source configuration, credentials and marker have not been changed.
	if err := p.save(ctx, "rolledback"); err != nil {
		return err
	}
	return p.scale(ctx, p.s.Replicas)
}

// waitReady checks the host controller's observed generation and ready count.
// Workload/data-plane acceptance beyond Kubernetes readiness is separate.
func (p *operation) waitReady(ctx context.Context) error {
	return wait.PollUntilContextCancel(ctx, 500*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		if p.s.Kind == "Deployment" {
			d, err := p.host.AppsV1().Deployments(p.s.Namespace).Get(ctx, p.s.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if string(d.UID) != p.s.WorkloadUID || (p.s.TemplateSHA != "" && podSpecHash(&d.Spec.Template.Spec) != p.s.TemplateSHA) {
				return false, errors.New("workload identity or Pod template changed")
			}
			return d.Spec.Replicas != nil && *d.Spec.Replicas == p.s.Replicas && d.Status.ObservedGeneration >= d.Generation && d.Status.ReadyReplicas == p.s.Replicas && d.Status.UpdatedReplicas == p.s.Replicas, nil
		}
		d, err := p.host.AppsV1().StatefulSets(p.s.Namespace).Get(ctx, p.s.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if string(d.UID) != p.s.WorkloadUID || (p.s.TemplateSHA != "" && podSpecHash(&d.Spec.Template.Spec) != p.s.TemplateSHA) {
			return false, errors.New("workload identity or Pod template changed")
		}
		return d.Spec.Replicas != nil && *d.Spec.Replicas == p.s.Replicas && d.Status.ObservedGeneration >= d.Generation && d.Status.ReadyReplicas == p.s.Replicas && d.Status.UpdatedReplicas == p.s.Replicas, nil
	})
}

func (p *operation) archiveJournal(ctx context.Context, journal *corev1.Secret) error {
	var old operationState
	if json.Unmarshal(journal.Data["operation.json"], &old) != nil || old.Version != 1 || len(old.ID) != 32 || (old.Phase != "complete" && old.Phase != "rolledback") {
		return errors.New("only a completed or rolled-back journal can be archived")
	}
	name := journal.Name + "-" + old.ID
	archived := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name}, Data: journal.DeepCopy().Data}
	immutable := true
	archived.Immutable = &immutable
	if _, err := p.host.CoreV1().Secrets(p.o.namespace).Create(ctx, archived, metav1.CreateOptions{}); err != nil {
		if !kerrors.IsAlreadyExists(err) {
			return err
		}
		existing, err := p.host.CoreV1().Secrets(p.o.namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(existing.Data, archived.Data) {
			return errors.New("archived operation differs from current journal")
		}
	}
	return p.host.CoreV1().Secrets(p.o.namespace).Delete(ctx, journal.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &journal.UID, ResourceVersion: &journal.ResourceVersion}})
}

func (p *operation) stop(ctx context.Context) error {
	if err := p.scale(ctx, 0); err != nil {
		return err
	}
	if err := wait.PollUntilContextCancel(ctx, 500*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		err := p.stopped(ctx, false)
		if errors.Is(err, errWorkloadActive) {
			return false, nil
		}
		return err == nil, err
	}); err != nil {
		return fmt.Errorf("wait for stopped control plane; suspend reconciliation and resume: %w", err)
	}
	return nil
}

func podSpecHash(spec *corev1.PodSpec) string {
	b, _ := json.Marshal(spec)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (p *operation) rotatedTLS() map[string][]byte {
	result := map[string][]byte{}
	for k, v := range p.s.OriginalTLS {
		result[k] = v
	}
	result[p.s.CAItem] = p.secret.Data["ca.crt"]
	result[p.s.CertItem] = p.secret.Data["tls.crt"]
	result[p.s.KeyItem] = p.secret.Data["tls.key"]
	return result
}
