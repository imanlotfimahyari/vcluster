// Package tenantbackup implements bounded, tenant-scoped etcd archives. It
// deliberately never uses cluster-wide snapshot, status or compaction APIs.
package tenantbackup

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	MaxValueBytes              = 1400 * 1024
	MaxKeyBytes                = 8 * 1024
	MaxEntries                 = 1000000
	MaxLeaseGroups             = 1024
	MaxLeaseTTL          int64 = 7 * 24 * 60 * 60
	MaxArchiveBytes      int64 = 4 << 30
	maxLineBytes               = 2 << 20
	ClaimSuffix                = "vcluster/storage-layout"
	RestoreSuffix              = "vcluster/restore-state"
	RestoreReceiptSuffix       = "vcluster/restore-receipt"
	FenceSuffix                = "vcluster/writer-fence"
	MigrationSuffix            = "vcluster/migration-state"
)

type Ownership struct {
	DeploymentUID string `json:"deploymentUID"`
	Endpoint      string `json:"etcdEndpoint"`
	Root          string `json:"rootPrefix"`
	Version       string `json:"version"`
}

type Header struct {
	Type      string    `json:"type"`
	Version   int       `json:"version"`
	Root      string    `json:"root"`
	Revision  int64     `json:"revision"`
	ClusterID uint64    `json:"clusterID"`
	Ownership Ownership `json:"ownership"`
}

type record struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value []byte `json:"value"`
	Lease int64  `json:"lease,omitempty"`
	TTL   int64  `json:"ttl,omitempty"`
}

type footer struct {
	Type    string `json:"type"`
	Entries int    `json:"entries"`
	SHA256  string `json:"sha256"`
}

// Summary contains no Kubernetes object values, credentials or private keys.
type Summary struct {
	Header
	Entries       int    `json:"entries"`
	SHA256        string `json:"sha256"`
	DataSHA256    string `json:"dataSHA256"`
	LeasedEntries int    `json:"leasedEntries,omitempty"`
	LeaseGroups   int    `json:"leaseGroups,omitempty"`
}

func NormalizeRoot(root string) (string, error) {
	root = strings.TrimSuffix(root, "/")
	if root == "" || root == "/" || len(root) > MaxKeyBytes-128 || !utf8.ValidString(root) ||
		!strings.HasPrefix(root, "/") || path.Clean(root) != root || strings.Contains(root, "\\") ||
		strings.ContainsFunc(root, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return "", errors.New("tenant root must be a canonical absolute non-root path")
	}
	for _, reserved := range []string{"/registry", "/vcluster"} {
		if root == reserved || strings.HasPrefix(root, reserved+"/") || strings.HasPrefix(reserved, root+"/") {
			return "", errors.New("tenant root overlaps a legacy keyspace")
		}
	}
	return root + "/", nil
}

func validDataKey(key string) bool {
	return len(key) <= MaxKeyBytes && utf8.ValidString(key) && path.Clean(key) == key &&
		!strings.Contains(key, "\\") && !strings.ContainsFunc(key, unicode.IsControl) &&
		((strings.HasPrefix(key, "registry/") && len(key) > len("registry/")) ||
			(strings.HasPrefix(key, "mappings/") && len(key) > len("mappings/")))
}

func validateHeader(h Header) error {
	root, err := NormalizeRoot(h.Root)
	if err != nil || root != h.Root || h.Type != "header" || (h.Version != 1 && h.Version != 2) || h.Revision <= 0 || h.ClusterID == 0 ||
		h.Ownership.Root != root || h.Ownership.Version != "1" || h.Ownership.DeploymentUID == "" || len(h.Ownership.DeploymentUID) > 256 || h.Ownership.Endpoint == "" || len(h.Ownership.Endpoint) > MaxKeyBytes {
		return errors.New("invalid or unsupported tenant archive header")
	}
	return nil
}

func line(v any) []byte {
	b, _ := json.Marshal(v)
	return append(b, '\n')
}

// decodeCanonical rejects duplicate/unknown fields and ambiguous encodings as
// well as malformed JSON. Archives are written in this exact canonical format.
func decodeCanonical(b []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return errors.New("invalid archive JSON")
	}
	want, err := json.Marshal(dst)
	if err != nil || !bytes.Equal(b, want) {
		return errors.New("noncanonical archive JSON")
	}
	return nil
}

// Export reads a single MVCC revision, including ownership and operation
// barriers. Publication is atomic, mode 0600, and never replaces an existing file.
func Export(ctx context.Context, kv clientv3.KV, root, destination string) (*Summary, error) {
	root, err := NormalizeRoot(root)
	if err != nil {
		return nil, err
	}
	claim, err := kv.Get(ctx, root+ClaimSuffix)
	if err != nil {
		return nil, err
	}
	if len(claim.Kvs) != 1 {
		return nil, errors.New("tenant ownership claim is missing")
	}
	var owner Ownership
	if err := json.Unmarshal(claim.Kvs[0].Value, &owner); err != nil {
		return nil, errors.New("invalid ownership claim")
	}
	h := Header{Type: "header", Version: 2, Root: root, Revision: claim.Header.Revision, ClusterID: claim.Header.ClusterId, Ownership: owner}
	if err := validateHeader(h); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(filepath.Dir(destination), ".tenant-backup-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	gz := gzip.NewWriter(f)
	hash, dataHash := sha256.New(), sha256.New()
	writer := io.MultiWriter(gz, hash)
	written := int64(0)
	emit := func(b []byte) error {
		written += int64(len(b))
		if written > MaxArchiveBytes {
			return errors.New("tenant archive exceeds size limit")
		}
		_, err := writer.Write(b)
		return err
	}
	if err := emit(line(h)); err != nil {
		return nil, err
	}
	count, start, end := 0, root, clientv3.GetPrefixRangeEnd(root)
	leases := map[int64]int64{}
	leasedEntries := 0
	for {
		page, err := kv.Get(ctx, start, clientv3.WithRange(end), clientv3.WithRev(h.Revision), clientv3.WithLimit(32), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if err != nil {
			return nil, fmt.Errorf("read tenant archive revision: %w", err)
		}
		for _, item := range page.Kvs {
			key := strings.TrimPrefix(string(item.Key), root)
			if key == ClaimSuffix || key == RestoreReceiptSuffix {
				continue
			}
			if key == RestoreSuffix || key == FenceSuffix || key == MigrationSuffix {
				return nil, errors.New("tenant has an active recovery or migration barrier")
			}
			if !validDataKey(key) || len(item.Key) > MaxKeyBytes || len(item.Value) > MaxValueBytes {
				return nil, errors.New("unsupported tenant key or record size")
			}
			ttl := int64(0)
			if item.Lease != 0 {
				var found bool
				ttl, found = leases[item.Lease]
				if !found {
					if len(leases) >= MaxLeaseGroups {
						return nil, errors.New("tenant archive exceeds lease-group limit")
					}
					reader, ok := kv.(leaseReader)
					if !ok {
						return nil, errors.New("leased tenant data requires a lease-aware etcd client")
					}
					// WithAttachedKeys makes etcd authorize every key on the lease;
					// the TTL-only API does not perform that range authorization.
					response, err := reader.TimeToLive(ctx, clientv3.LeaseID(item.Lease), clientv3.WithAttachedKeys())
					if err != nil {
						return nil, fmt.Errorf("read tenant lease TTL: %w", err)
					}
					if response == nil || response.ID != clientv3.LeaseID(item.Lease) || response.TTL <= 0 || response.TTL > MaxLeaseTTL {
						return nil, errors.New("tenant lease expired or exceeds the supported TTL")
					}
					for _, attached := range response.Keys {
						if !strings.HasPrefix(string(attached), root) {
							return nil, errors.New("tenant lease has keys outside the tenant root")
						}
					}
					ttl = response.TTL
					leases[item.Lease] = ttl
				}
				leasedEntries++
			}
			count++
			if count > MaxEntries {
				return nil, errors.New("tenant archive exceeds entry limit")
			}
			b := line(record{Type: "kv", Key: key, Value: item.Value, Lease: item.Lease, TTL: ttl})
			if err := emit(b); err != nil {
				return nil, err
			}
			_, _ = dataHash.Write(b)
		}
		if !page.More {
			break
		}
		if len(page.Kvs) == 0 {
			return nil, errors.New("empty paginated tenant response")
		}
		start = string(page.Kvs[len(page.Kvs)-1].Key) + "\x00"
	}
	sum := &Summary{Header: h, Entries: count, SHA256: hex.EncodeToString(hash.Sum(nil)), DataSHA256: hex.EncodeToString(dataHash.Sum(nil)), LeasedEntries: leasedEntries, LeaseGroups: len(leases)}
	if _, err := gz.Write(line(footer{Type: "footer", Entries: count, SHA256: sum.SHA256})); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := os.Link(f.Name(), destination); err != nil {
		return nil, fmt.Errorf("publish backup without replacement: %w", err)
	}
	dir, err := os.Open(filepath.Dir(destination))
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return nil, err
	}
	return sum, nil
}

// Inspect verifies the entire archive, including gzip CRC, footer, bounds and
// trailing bytes. It never contacts etcd and never returns object values.
func Inspect(filename string) (*Summary, error) { return inspect(filename, nil) }

func inspect(filename string, spool io.Writer) (*Summary, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	compressed := bufio.NewReader(f)
	gz, err := gzip.NewReader(compressed)
	if err != nil {
		return nil, errors.New("invalid gzip archive")
	}
	defer gz.Close()
	gz.Multistream(false)
	limited := &io.LimitedReader{R: gz, N: MaxArchiveBytes + 1}
	scan := bufio.NewScanner(limited)
	scan.Buffer(make([]byte, 64*1024), maxLineBytes)
	if !scan.Scan() {
		return nil, errors.New("missing archive header")
	}
	var h Header
	if err := decodeCanonical(scan.Bytes(), &h); err != nil {
		return nil, err
	}
	if err := validateHeader(h); err != nil {
		return nil, err
	}
	hash, dataHash := sha256.New(), sha256.New()
	_, _ = hash.Write(append(bytes.Clone(scan.Bytes()), '\n'))
	count, previous, finished := 0, "", false
	leases := map[int64]int64{}
	leasedEntries := 0
	var foot footer
	for scan.Scan() {
		b := bytes.Clone(scan.Bytes())
		var tag struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(b, &tag) != nil {
			return nil, errors.New("invalid archive record")
		}
		if tag.Type == "footer" {
			if err := decodeCanonical(b, &foot); err != nil {
				return nil, err
			}
			finished = true
			break
		}
		var rec record
		if err := decodeCanonical(b, &rec); err != nil {
			return nil, err
		}
		if rec.Type != "kv" || !validDataKey(rec.Key) || len(h.Root)+len(rec.Key) > MaxKeyBytes || len(rec.Value) > MaxValueBytes || rec.Key <= previous {
			return nil, errors.New("invalid, duplicate or unordered tenant record")
		}
		if (rec.Lease == 0 && rec.TTL != 0) || (rec.Lease != 0 && (h.Version != 2 || rec.TTL <= 0 || rec.TTL > MaxLeaseTTL)) {
			return nil, errors.New("invalid or unsupported archive lease metadata")
		}
		if rec.Lease != 0 {
			if ttl, exists := leases[rec.Lease]; exists && ttl != rec.TTL {
				return nil, errors.New("inconsistent TTL for shared archive lease")
			}
			leases[rec.Lease] = rec.TTL
			leasedEntries++
			if len(leases) > MaxLeaseGroups {
				return nil, errors.New("tenant archive exceeds lease-group limit")
			}
		}
		count++
		if count > MaxEntries {
			return nil, errors.New("tenant archive exceeds entry limit")
		}
		previous = rec.Key
		b = append(b, '\n')
		_, _ = hash.Write(b)
		_, _ = dataHash.Write(b)
		if spool != nil {
			if _, err := spool.Write(b); err != nil {
				return nil, err
			}
		}
	}
	if !finished || scan.Scan() || scan.Err() != nil || limited.N <= 1 {
		return nil, errors.New("truncated, oversized or trailing archive data")
	}
	if _, err := compressed.Peek(1); err != io.EOF {
		return nil, errors.New("trailing compressed archive data")
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if foot.Type != "footer" || foot.Entries != count || foot.SHA256 != digest {
		return nil, errors.New("archive checksum or entry count mismatch")
	}
	return &Summary{Header: h, Entries: count, SHA256: digest, DataSHA256: hex.EncodeToString(dataHash.Sum(nil)), LeasedEntries: leasedEntries, LeaseGroups: len(leases)}, nil
}

type leaseReader interface {
	TimeToLive(context.Context, clientv3.LeaseID, ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error)
}
