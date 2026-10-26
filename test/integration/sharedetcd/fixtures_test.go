//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	rawconfig "github.com/loft-sh/vcluster/config"
	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"
)

// The harness only starts disposable local processes. It never loads a user's
// kubeconfig, connects to an existing cluster, or reads existing credentials.
func asset(t *testing.T, name string) string {
	t.Helper()
	dir := os.Getenv("VCLUSTER_SHARED_ETCD_ASSETS")
	if dir == "" {
		t.Skip("set VCLUSTER_SHARED_ETCD_ASSETS to verified local etcd and kube-apiserver binaries")
	}
	path := filepath.Join(dir, name)
	info, err := os.Stat(path)
	if err != nil || info.Mode()&0111 == 0 {
		t.Fatalf("required test asset %s is missing or not executable", name)
	}
	return path
}

type testCA struct {
	cert      *x509.Certificate
	key       *ecdsa.PrivateKey
	path, dir string
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "disposable-shared-etcd-test"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "ca.crt")
	writeFixture(t, path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, filepath.Join(dir, "ca.key"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	return &testCA{cert: cert, key: key, path: path, dir: dir}
}

func writeFixture(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func (ca *testCA) identity(t *testing.T, name string, server bool, organizations ...string) *etcd.Certificates {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name, Organization: organizations}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	if server {
		template.ExtKeyUsage = append(template.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
		template.DNSNames = []string{"localhost"}
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(ca.dir, name+".crt"), filepath.Join(ca.dir, name+".key")
	writeFixture(t, certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	writeFixture(t, keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	return &etcd.Certificates{CaCert: ca.path, ServerCert: certPath, ServerKey: keyPath}
}

func unusedPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

type testProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	err     error
	logPath string
	once    sync.Once
}

func startProcess(t *testing.T, name string, env []string, binary string, args ...string) *testProcess {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), name+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, args...)
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	p := &testProcess{cmd: cmd, done: make(chan struct{}), logPath: logPath}
	go func() { p.err = cmd.Wait(); _ = logFile.Close(); close(p.done) }()
	t.Cleanup(func() {
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			if len(data) > 8000 {
				data = data[len(data)-8000:]
			}
			t.Logf("%s process log tail before shutdown:\n%s", name, data)
		}
		p.stop()
	})
	return p
}

func (p *testProcess) stop() {
	p.once.Do(func() {
		// Child API servers inherit the process group and must not outlive a test.
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
		}
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		<-p.done
	})
}

func (p *testProcess) check() error {
	select {
	case <-p.done:
		return fmt.Errorf("test process exited: %v", p.err)
	default:
		return nil
	}
}

func rawEtcdClient(t *testing.T, endpoint string, certs *etcd.Certificates) *clientv3.Client {
	t.Helper()
	leaf, err := tls.LoadX509KeyPair(certs.ServerCert, certs.ServerKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(certs.CaCert)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("invalid test CA")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{leaf}}, DialTimeout: 3 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

type etcdLab struct {
	ca       *testCA
	endpoint string
	admin    *clientv3.Client
	certs    map[string]*etcd.Certificates
}

func newEtcdLab(t *testing.T) *etcdLab {
	t.Helper()
	requireLeaseTestVersion(t)
	binary := asset(t, "etcd")
	ca := newTestCA(t)
	server, root := ca.identity(t, "server", true), ca.identity(t, "root", false)
	endpoint := fmt.Sprintf("https://127.0.0.1:%d", unusedPort(t))
	peer := fmt.Sprintf("http://127.0.0.1:%d", unusedPort(t))
	process := startProcess(t, "etcd", os.Environ(), binary,
		"--name=shared-etcd", "--data-dir="+t.TempDir(), "--listen-client-urls="+endpoint, "--advertise-client-urls="+endpoint,
		"--listen-peer-urls="+peer, "--initial-advertise-peer-urls="+peer, "--initial-cluster=shared-etcd="+peer,
		"--cert-file="+server.ServerCert, "--key-file="+server.ServerKey, "--trusted-ca-file="+ca.path, "--client-cert-auth=true", "--log-level=error")
	admin := rawEtcdClient(t, endpoint, root)
	if err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, integrationTimeout(20*time.Second), true, func(ctx context.Context) (bool, error) {
		if err := process.check(); err != nil {
			return false, err
		}
		callCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, err := admin.Status(callCtx, endpoint)
		return err == nil, nil
	}); err != nil {
		t.Fatalf("etcd did not become ready: %v", err)
	}
	return initializeEtcdLab(t, ca, endpoint, admin, root)
}

func initializeEtcdLab(t *testing.T, ca *testCA, endpoint string, admin *clientv3.Client, root *etcd.Certificates) *etcdLab {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if _, err := admin.UserAddWithOptions(ctx, "root", "", &clientv3.UserAddOptions{NoPassword: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.UserGrantRole(ctx, "root", "root"); err != nil {
		t.Fatal(err)
	}
	certs := map[string]*etcd.Certificates{"root": root}
	for _, name := range []string{"tenant-a", "tenant-b"} {
		certs[name] = ca.identity(t, name, false)
		if _, err := admin.RoleAdd(ctx, name); err != nil {
			t.Fatal(err)
		}
		prefix := "/tenants/" + name + "/"
		if _, err := admin.RoleGrantPermission(ctx, name, prefix, clientv3.GetPrefixRangeEnd(prefix), clientv3.PermissionType(clientv3.PermReadWrite)); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.UserAddWithOptions(ctx, name, "", &clientv3.UserAddOptions{NoPassword: true}); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.UserGrantRole(ctx, name, name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := admin.AuthEnable(ctx); err != nil {
		t.Fatal(err)
	}
	return &etcdLab{ca: ca, endpoint: endpoint, admin: admin, certs: certs}
}

func (lab *etcdLab) config(t *testing.T, name string) *config.VirtualClusterConfig {
	t.Helper()
	c, err := rawconfig.NewDefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	c.Telemetry.Enabled = false
	c.ControlPlane.Distro.K8S.Enabled = true
	certs := lab.certs[name]
	c.ControlPlane.BackingStore.Etcd.External = rawconfig.EtcdExternal{Enabled: true, Endpoint: lab.endpoint, StorePrefix: "/tenants/" + name, TLS: rawconfig.EtcdExternalTLS{CaFile: certs.CaCert, CertFile: certs.ServerCert, KeyFile: certs.ServerKey}}
	return &config.VirtualClusterConfig{Config: *c, Name: name, HostNamespace: name}
}

func integrationTimeout(duration time.Duration) time.Duration {
	if os.Getenv("VCLUSTER_SHARED_ETCD_VM") == "1" {
		return duration * 5
	}
	return duration
}
