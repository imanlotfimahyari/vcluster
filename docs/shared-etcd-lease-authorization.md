# Stock etcd lease authorization qualification

The isolated external-server baseline is **etcd 3.6.16**, Linux amd64, upstream
commit `0b065e61c679ea81f6862bb24c9815f7fc673966`. The official archive is verified
against its release `SHA256SUMS`:

`c8c0d96d8292ef085deabbf735e9561d3d396768e4d606df005aa25964f21911`

The harness and process fixtures reject a different reported version. They do
not download executables automatically or upgrade an existing cluster. The
vendored Go etcd modules remain at 3.6.8; this checkpoint changes the external
process used by the integration suite, not the embedded-server distribution or
all library dependencies. Those dependencies still need a separately tested
security update before production packaging.

The previous 3.6.8 test that expected a retired identity's keep-alive to succeed
is replaced by a denial assertion. That older observation must not be described
as a universal limit of current stock etcd.

## Verified contract

| Operation or condition | Stock 3.6.16 result |
| --- | --- |
| Writer renews a lease attached only to its writable tenant keys | Allowed |
| Foreign tenant or read-only identity renews that lease | Denied |
| Retired identity sends another message on its already-open keep-alive stream | Rejected; a fresh request is denied too |
| Readable lease TTL with attached keys requested | Allowed |
| Foreign lease TTL with attached keys requested | Denied |
| Global listing when a lease contains unreadable foreign keys | Denied; listing is not automatically filtered per tenant |
| Administrator lists leases | Allowed |
| Foreign/read-only identity revokes a protected lease | Denied |
| Foreign lease attached by Put, a transaction, or a nested transaction | Denied without creating the requested key |
| Renewal requests overlapping permission removal/regrant | Rejected after revocation; new streams work after regrant |
| Leader and follower RPC endpoints, controlled leader transfer, leader loss and restart | Protected renewal remains enforced |
| Full three-member stop/start with preserved data directories | Revocation persists; lease attachment and value survive; renewal works after explicit regrant |

Tests use only generated identities and disposable local processes. The
three-member fixture uses a separate peer CA and authenticated TLS peer links.
Assertions after cluster auth changes wait until every active member reports the
committed authorization revision. An in-flight request authorized before a
change is not claimed to be retroactively cancelled.

## Boundaries that still matter

**TTL-only metadata:** when callers omit attached keys, stock 3.6.16 still returns
lease ID/lifetime metadata without attached-key read authorization. No key names
or values are returned. The archive exporter intentionally continues using
`WithAttachedKeys()` and rejects cross-root attachments. This behavior was
already documented in the previous recovery implementation; the new suite makes
the distinction explicit. It is not complete lease metadata confidentiality.

**Empty leases are not creator-owned:** an authenticated tenant can renew or
revoke an empty lease created by another identity, or attach its own writable key
to it. Once keys are attached, authorization follows those keys, not the CN that
originally called Grant. Strict permanent tenant ownership of every lease ID
would require a different ownership model; this checkpoint does not implement
or claim it. A shared service also retains global resource/availability limits.

The qualified outcome is therefore: stock 3.6.16 closes the demonstrated
unauthorized renewal of **protected, nonempty leases**, including retired writer
access. It does not implement permanently tenant-owned lease objects or isolate
all metadata/capacity. Choosing stock etcd is reasonable only when those residual
semantics fit the deployment's stated threat model.

A minimal TTL-inspection hardening candidate was evaluated outside the
public implementation branch: the stricter TCP/mTLS regression fails on stock
and passes with the candidate. Existing-stream and three-member restart checks
also pass. Upstream fixture execution is blocked here by its Unix socket
requirement; complete upstream CI and compatibility review remain required. It is not part of the deployed/test baseline,
is not an accepted upstream fix, and does not add empty-lease ownership. No
security report, maintainer message or upstream PR is sent automatically.

## Reproduce the qualification

```sh
export VCLUSTER_SHARED_ETCD_ASSETS=/absolute/path/to/verified-assets
bash hack/test-shared-etcd.sh
```

To run just the lease tests, provide the assets variable and use:

```sh
go vet -mod=vendor -tags=integration ./test/integration/sharedetcd
go test -mod=vendor -tags=integration -count=1 -timeout=4m -v \
  -run '^(TestLease|TestEmptyLease|TestTenantLeasedBackup)' \
  ./test/integration/sharedetcd
```

This is a native process qualification, not a worker/CNI/CSI acceptance result.
Leader loss uses process shutdown, not packet partitions, WAL corruption or
power-loss injection. Existing three-member testing is for the shared etcd
service; it does not establish vCluster control-plane HA support.
