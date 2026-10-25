package tenantbackup

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testHeader() Header {
	return Header{Type: "header", Version: 1, Root: "/tenants/test/", Revision: 7, ClusterID: 12, Ownership: Ownership{Version: "1", Root: "/tenants/test/", Endpoint: "etcd.example.com:2379", DeploymentUID: "test-owner"}}
}

func archiveBytes(h Header, records []record) []byte {
	data := line(h)
	for _, rec := range records {
		data = append(data, line(rec)...)
	}
	digest := sha256.Sum256(data)
	return append(data, line(footer{Type: "footer", Entries: len(records), SHA256: hex.EncodeToString(digest[:])})...)
}

func compressed(data []byte) []byte {
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	_, _ = w.Write(data)
	_ = w.Close()
	return out.Bytes()
}

func inspectBytes(t *testing.T, data []byte) (*Summary, error) {
	t.Helper()
	f := filepath.Join(t.TempDir(), "archive.gz")
	if err := os.WriteFile(f, data, 0600); err != nil {
		t.Fatal(err)
	}
	return Inspect(f)
}

func TestInspectStrictArchiveContract(t *testing.T) {
	recs := []record{{Type: "kv", Key: "mappings/v1/example", Value: []byte{0, 0xff, 1}}, {Type: "kv", Key: "registry/configmaps/default/test", Value: []byte("raw-value")}}
	data := archiveBytes(testHeader(), recs)
	summary, err := inspectBytes(t, compressed(data))
	if err != nil {
		t.Fatal(err)
	}
	if summary.Entries != 2 || summary.Revision != 7 || summary.Ownership.DeploymentUID != "test-owner" || len(summary.DataSHA256) != 64 {
		t.Fatalf("invalid summary: %+v", summary)
	}
	cases := map[string][]byte{
		"truncated gzip":         compressed(data)[:10],
		"truncated records":      compressed(data[:len(data)/2]),
		"trailing record":        compressed(append(bytes.Clone(data), []byte("{}\n")...)),
		"trailing raw bytes":     append(compressed(data), 'x'),
		"trailing gzip member":   append(compressed(data), compressed([]byte("hidden"))...),
		"wrong value checksum":   compressed(bytes.Replace(data, []byte("cmF3LXZhbHVl"), []byte("bmV3LXZhbHVl"), 1)),
		"duplicate header field": compressed(bytes.Replace(data, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)),
		"unknown header field":   compressed(bytes.Replace(data, []byte(`"version":1`), []byte(`"version":1,"unknown":1`), 1)),
	}
	badCRC := compressed(data)
	badCRC[len(badCRC)-8] ^= 0xff
	cases["bad gzip CRC"] = badCRC
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := inspectBytes(t, b); err == nil {
				t.Fatal("unsafe archive accepted")
			}
		})
	}
}

func TestInspectRejectsUnsafeRecordsBeforeMutation(t *testing.T) {
	for _, key := range []string{"../registry/test", "registry/../escape", "/registry/test", "registry/", "registry//test", "vcluster/storage-layout", "mappings/test\\escape", "registry/test\x00", "registry/test\n"} {
		t.Run(key, func(t *testing.T) {
			if _, err := inspectBytes(t, compressed(archiveBytes(testHeader(), []record{{Type: "kv", Key: key, Value: []byte("value")}}))); err == nil {
				t.Fatal("unsafe key accepted")
			}
		})
	}
	for name, recs := range map[string][]record{
		"duplicate":       {{Type: "kv", Key: "registry/a"}, {Type: "kv", Key: "registry/a"}},
		"unordered":       {{Type: "kv", Key: "registry/b"}, {Type: "kv", Key: "registry/a"}},
		"oversized value": {{Type: "kv", Key: "registry/a", Value: bytes.Repeat([]byte("v"), MaxValueBytes+1)}},
		"oversized key":   {{Type: "kv", Key: "registry/" + strings.Repeat("k", MaxKeyBytes)}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := inspectBytes(t, compressed(archiveBytes(testHeader(), recs))); err == nil {
				t.Fatal("invalid records accepted")
			}
		})
	}
	for _, change := range []func(*Header){func(h *Header) { h.Version = 3 }, func(h *Header) { h.ClusterID = 0 }, func(h *Header) { h.Ownership.Root = "/tenants/other/" }, func(h *Header) { h.Ownership.DeploymentUID = "" }} {
		h := testHeader()
		change(&h)
		if _, err := inspectBytes(t, compressed(archiveBytes(h, nil))); err == nil {
			t.Fatal("invalid ownership/header accepted")
		}
	}
}

func TestNormalizeRoot(t *testing.T) {
	for _, root := range []string{"/", "", "//", "relative", "/registry", "/registry/nested", "/vcluster", "/vcluster/nested", "/a/../b", "/a//b", "/a//", "/a\\b", "/a b", "/a\x00", "/a\xff"} {
		if _, err := NormalizeRoot(root); err == nil {
			t.Fatalf("unsafe root accepted: %q", root)
		}
	}
	for _, root := range []string{"/tenants/a", "/tenants/a/", "/registry-other/a"} {
		if _, err := NormalizeRoot(root); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPermissionRangeBoundaries(t *testing.T) {
	root := "/tenants/a/"
	end := "/tenants/a0"
	for _, r := range [][2]string{{root, end}, {"/", "\x00"}, {root + "registry/", root + "registrz"}, {root + "one", ""}, {"/tenants/", "/tenants/b"}} {
		if !rangesIntersect(r[0], r[1], root, end) {
			t.Fatalf("missed overlap: %q", r)
		}
	}
	for _, r := range [][2]string{{"/tenants/b/", "/tenants/b0"}, {"/tenants/a", ""}, {"/tenants/a", "/tenants/a/"}} {
		if rangesIntersect(r[0], r[1], root, end) {
			t.Fatalf("false overlap: %q", r)
		}
	}
	if !rangeInside(root, end, root) || rangeInside("/", "\x00", root) || rangeInside(root, "\x00", root) || rangeInside("/tenants/a", "", root) {
		t.Fatal("invalid range containment")
	}
}

func TestRestorePrevalidatesBeforeAnyEtcdOrHostCall(t *testing.T) {
	file := filepath.Join(t.TempDir(), "corrupt.gz")
	if err := os.WriteFile(file, []byte("corrupt archive"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Restore(t.Context(), nil, file, RestoreOptions{CheckStopped: func(context.Context) error { t.Fatal("host was contacted before archive validation"); return nil }})
	if err == nil {
		t.Fatal("corrupt archive accepted")
	}
}

func TestArchiveLeaseMetadataContract(t *testing.T) {
	h := testHeader()
	h.Version = 2
	valid := []record{{Type: "kv", Key: "registry/events/a", Value: []byte("a"), Lease: 123, TTL: 60}, {Type: "kv", Key: "registry/events/b", Value: []byte("b"), Lease: 123, TTL: 60}}
	summary, err := inspectBytes(t, compressed(archiveBytes(h, valid)))
	if err != nil || summary.LeasedEntries != 2 || summary.LeaseGroups != 1 {
		t.Fatalf("valid lease group rejected: %v", err)
	}
	for name, recs := range map[string][]record{
		"ttl without lease":      {{Type: "kv", Key: "registry/a", TTL: 60}},
		"lease without ttl":      {{Type: "kv", Key: "registry/a", Lease: 123}},
		"negative ttl":           {{Type: "kv", Key: "registry/a", Lease: 123, TTL: -1}},
		"oversized ttl":          {{Type: "kv", Key: "registry/a", Lease: 123, TTL: MaxLeaseTTL + 1}},
		"inconsistent group ttl": {{Type: "kv", Key: "registry/a", Lease: 123, TTL: 60}, {Type: "kv", Key: "registry/b", Lease: 123, TTL: 90}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := inspectBytes(t, compressed(archiveBytes(h, recs))); err == nil {
				t.Fatal("invalid lease metadata accepted")
			}
		})
	}
	h.Version = 1
	if _, err := inspectBytes(t, compressed(archiveBytes(h, valid))); err == nil {
		t.Fatal("version 1 accepted lease metadata")
	}
}
