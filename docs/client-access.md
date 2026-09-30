# Data-plane client access

The daemon's data listener requires TLS 1.3 and a client certificate. There is
no plaintext or anonymous fallback. The health listener remains a separate
HTTP endpoint and does not execute data requests.

Health connections have an 8 KiB request-line limit and a two-second deadline
covering request reading and response writing. Accepted sockets explicitly enter
blocking mode with bounded I/O, independent of the listener's host-specific socket
inheritance. A rejected or disconnected health client is logged to stderr without
counting as a daemon runtime failure; listener accept failures still reach the
runtime supervisor.

## Configure server trust

After `rsfd init`, install these PEM files under the cluster state directory:

| File | Contents |
| --- | --- |
| `tls/server.pem` | Server leaf certificate followed by any intermediate certificates |
| `tls/server-key.pem` | Matching private key, readable only by the daemon account |
| `tls/client-ca.pem` | CA certificates trusted to issue client certificates |

The server certificate must cover the hostname clients connect to. Clients
must validate that hostname and the server's issuing CA, and present their own
certificate and private key. The daemon verifies the client chain, validity,
and proof of private-key possession using rustls. Merely trusting a client CA
does not grant its clients database access.

The listener refuses to start when its identity or trust configuration is
missing or invalid. Trust roots and the server identity load at listener
startup; restart the listener after replacing them. TLS resumption is disabled
so every connection presents and validates a current client certificate.

## Register a namespace and principal

These are local administration commands. Access to the state directory is the
operator's authority; do not give data clients filesystem access to that
directory. Concurrent security administration commands are serialized, and
accepted changes are written to the durable operator audit log.

For an initialized state directory at `/srv/rsf`, create a database namespace:

```sh
rsfd namespace create --state /srv/rsf --tenant acme --database main \
  --namespace app --region iad --surfaces sql,kv,object,blob
rsfd auth register --state /srv/rsf --principal app-client
rsfd auth bind --state /srv/rsf --principal app-client \
  --certificate /secure/app-client.pem
```

`bind` stores the SHA-256 fingerprint of the PEM file's first (leaf)
certificate. It cannot overwrite a binding to another principal. The daemon
compares that fingerprint with the leaf certificate verified by the live TLS
handshake, then checks that the wire request's principal matches the binding.
No private client key is sent to the daemon or stored in its authorization log.

Grant only the needed capabilities, optionally restricted to one surface:

```sh
rsfd auth grant --state /srv/rsf --principal app-client --namespace app \
  --surface kv --capability namespace-read
rsfd auth grant --state /srv/rsf --principal app-client --namespace app \
  --surface kv --capability namespace-write
```

| Surface | Read capability | Write capability |
| --- | --- | --- |
| KV | `namespace-read` | `namespace-write` |
| SQL | `query-execute` and `namespace-read` | `query-execute` and `namespace-write` |
| Object | `object-read` | `object-write` |
| Blob | `blob-read` | `blob-write` |

Omitting `--surface` creates a namespace-wide grant for that capability. SQL
read/write classification uses the execution parser. A database must also
enable the requested surface, and the namespace must belong to a registered
tenant/database. Certificates and grants are recovered and checked on every
request, before any per-surface data store opens.

Start the configured listener:

```sh
rsfd serve --state /srv/rsf --data-listen 0.0.0.0:9000
```

Inside TLS, clients send the existing `rsf-wire::WireCodec` frame preceded by
its four-byte little-endian length. The maximum encoded frame is 64 KiB. Each
connection carries one request and receives the `RSF/1` response followed by a
TLS close notification. The protocol labels do not make this listener a
PostgreSQL, HTTP, or S3 wire-compatible endpoint.

## Revoke and rotate access

```sh
rsfd auth revoke-grant --state /srv/rsf --principal app-client \
  --namespace app --surface kv --capability namespace-write
rsfd auth revoke-certificate --state /srv/rsf \
  --certificate /secure/app-client.pem
```

Revocation applies to the next request authorization; a request already
executing is not rolled back. To rotate a certificate without interrupting
clients, bind the replacement certificate to the same principal, switch
clients to it, then revoke the old certificate. Restart is not needed for
these binding and grant changes. CA rotation requires updating the trust file
and restarting the listener.

## Namespace storage and upgrades

Namespaces have separate SQL, KV, object, and blob roots beneath
`namespaces/<SHA-256-of-namespace-name>/`. The namespace catalogue prevents
rebinding a name to another tenant/database. Storage names are derived only
after authentication, authorization, and catalogue validation.

Older daemon builds stored all namespaces in shared top-level `sql`, `kv`,
`object`, and `blob` directories. The secured daemon refuses data execution
when any of those directories exists. It does not guess which tenant owns
legacy data or silently expose it to a newly registered namespace. An explicit
migration preserving data and request receipts remains a release blocker;
back up existing state and do not rename those directories as a workaround.

Authentication and namespace isolation do not establish distributed durability.
The current default replication transport is still local; independent peer
replication and the remaining production gates are tracked in `WORK.md`.
