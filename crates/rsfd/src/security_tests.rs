use std::path::{Path, PathBuf};
use std::sync::Arc;

use rsf_auth::{Capability, DurableAuthEngine, Grant, PrincipalId, Resource};
use rsf_core::Epoch;
use rsf_tenancy::{DatabaseSurface, DurableTenancyCatalog};
use rsf_wire::{RawFrame, WireCodec, WireProtocol};

use crate::{data::DataPlaneListener, test_support, tests};

fn setup(name: &str) -> (PathBuf, DataPlaneListener) {
    let state = tests::test_path(name);
    tests::bootstrap_three_nodes(state.to_str().unwrap());
    let listener = DataPlaneListener::bind("127.0.0.1:0".parse().unwrap(), &state).unwrap();
    (state, listener)
}

fn request(
    listener: &DataPlaneListener,
    frame: RawFrame,
    config: Arc<rustls::ClientConfig>,
) -> std::io::Result<String> {
    let addr = listener.local_addr();
    let encoded = WireCodec.encode(&frame).unwrap();
    let client = std::thread::spawn(move || test_support::send(addr, &encoded, config));
    tests::wait_for_data_request(listener, &mut Vec::new(), "secure request");
    client.join().unwrap()
}

fn frame(
    protocol: WireProtocol,
    namespace: &str,
    operation: &str,
    key: &str,
    body: &[u8],
) -> RawFrame {
    RawFrame::with_body(
        protocol,
        "app-client",
        Some(namespace.into()),
        operation,
        Some(key.into()),
        body.to_vec(),
    )
}

fn exchange(
    listener: &DataPlaneListener,
    protocol: WireProtocol,
    namespace: &str,
    operation: &str,
    key: &str,
    body: &[u8],
) -> String {
    request(
        listener,
        frame(protocol, namespace, operation, key, body),
        test_support::pki().client.clone(),
    )
    .unwrap()
}

#[test]
fn unauthenticated_unregistered_and_spoofed_clients_never_open_data_storage() {
    let (state, listener) = setup("mtls_denial");
    let get = frame(WireProtocol::Kv, "app", "GET secret", "r", &[]);
    assert!(
        request(
            &listener,
            get.clone(),
            test_support::pki().anonymous.clone()
        )
        .is_err()
    );
    assert!(request(&listener, get.clone(), test_support::pki().expired.clone()).is_err());
    assert!(
        request(
            &listener,
            get.clone(),
            test_support::pki().untrusted.clone()
        )
        .is_err()
    );
    let addr = listener.local_addr();
    let plaintext = std::thread::spawn(move || {
        use std::io::{Read, Write};
        let mut stream = std::net::TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(3)))
            .unwrap();
        let _ = stream.write_all(b"plaintext is not a TLS client");
        let mut response = Vec::new();
        let _ = stream.read_to_end(&mut response);
        assert!(!response.windows(5).any(|bytes| bytes == b"RSF/1"));
    });
    tests::wait_for_data_request(&listener, &mut Vec::new(), "plaintext denial");
    plaintext.join().unwrap();
    let result = request(
        &listener,
        get.clone(),
        test_support::pki().unregistered.clone(),
    )
    .unwrap();
    assert!(result.contains("not bound"), "{result}");
    let mut forged = get;
    forged.principal = "administrator".into();
    assert!(
        request(&listener, forged, test_support::pki().client.clone())
            .unwrap()
            .contains("not bound")
    );
    assert!(
        exchange(
            &listener,
            WireProtocol::Kv,
            "private",
            "GET secret",
            "r",
            &[]
        )
        .contains("no matching grant")
    );
    assert!(!state.join("namespaces").exists());
    // A bad peer must not stop the listener from handling the next valid client.
    assert!(
        exchange(
            &listener,
            WireProtocol::Kv,
            "app",
            "SET secret",
            "w",
            b"value"
        )
        .contains("RSF/1 executed")
    );
    let _ = std::fs::remove_dir_all(state);
}

#[test]
fn revocation_is_effective_on_the_next_connection_without_daemon_restart() {
    let (state, listener) = setup("mtls_revocation");
    assert!(
        exchange(&listener, WireProtocol::Kv, "app", "SET key", "w", b"value")
            .contains("RSF/1 executed")
    );
    let mut auth = DurableAuthEngine::open(state.join("auth"), Epoch(1)).unwrap();
    auth.revoke_certificate(test_support::pki().fingerprint)
        .unwrap();
    drop(auth);
    assert!(
        exchange(&listener, WireProtocol::Kv, "app", "GET key", "r", &[]).contains("not bound")
    );
    let _ = std::fs::remove_dir_all(state);
}

#[test]
fn read_grants_cannot_mutate_sql_or_kv_and_disabled_surfaces_stay_closed() {
    let (state, listener) = setup("capability_denial");
    let mut auth = DurableAuthEngine::open(state.join("auth"), Epoch(1)).unwrap();
    auth.revoke_grant(Grant {
        principal: PrincipalId("app-client".into()),
        capability: Capability::NamespaceWrite,
        resource: Resource::Namespace("app".into()),
    })
    .unwrap();
    drop(auth);
    for (protocol, operation) in [
        (WireProtocol::Kv, "SET key"),
        (
            WireProtocol::Sql,
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
        ),
    ] {
        assert!(
            exchange(&listener, protocol, "app", operation, "w", b"").contains("no matching grant")
        );
    }
    assert!(!state.join("namespaces").exists());
    let mut tenancy = DurableTenancyCatalog::open(state.join("tenancy"), Epoch(1)).unwrap();
    let tenant = tenancy.create_tenant("limited", "iad").unwrap();
    let database = tenancy
        .create_database(&tenant.name, "limited", [DatabaseSurface::Kv])
        .unwrap();
    tenancy
        .bind_namespace(&tenant.name, &database.name, "limited")
        .unwrap();
    drop(tenancy);
    let mut auth = DurableAuthEngine::open(state.join("auth"), Epoch(1)).unwrap();
    auth.grant(Grant {
        principal: PrincipalId("app-client".into()),
        capability: Capability::BlobRead,
        resource: Resource::Namespace("limited".into()),
    })
    .unwrap();
    drop(auth);
    let result = exchange(
        &listener,
        WireProtocol::Blob,
        "limited",
        "GET bad-id",
        "r",
        &[],
    );
    assert!(result.contains("does not enable blob"), "{result}");
    assert!(!state.join("namespaces").exists());
    let _ = std::fs::remove_dir_all(state);
}

fn add_namespace(state: &Path, namespace: &str) {
    let mut tenancy = DurableTenancyCatalog::open(state.join("tenancy"), Epoch(1)).unwrap();
    let tenant = tenancy.create_tenant(namespace, "iad").unwrap();
    let database = tenancy
        .create_database(
            &tenant.name,
            namespace,
            [
                DatabaseSurface::Relational,
                DatabaseSurface::Kv,
                DatabaseSurface::Object,
                DatabaseSurface::Blob,
            ],
        )
        .unwrap();
    tenancy
        .bind_namespace(&tenant.name, &database.name, namespace)
        .unwrap();
    drop(tenancy);
    let mut auth = DurableAuthEngine::open(state.join("auth"), Epoch(1)).unwrap();
    for capability in [
        Capability::NamespaceRead,
        Capability::NamespaceWrite,
        Capability::QueryExecute,
        Capability::ObjectRead,
        Capability::ObjectWrite,
        Capability::BlobRead,
        Capability::BlobWrite,
    ] {
        auth.grant(Grant {
            principal: PrincipalId("app-client".into()),
            capability,
            resource: Resource::Namespace(namespace.into()),
        })
        .unwrap();
    }
}

#[test]
fn independent_namespace_data_survives_reopen_on_every_surface() {
    let (state, listener) = setup("namespace_isolation");
    add_namespace(&state, "other");
    for (namespace, value) in [("app", "alice"), ("other", "bob")] {
        for (protocol, operation, key, body) in [
            (
                WireProtocol::Kv,
                "SET shared".to_string(),
                "kv",
                value.as_bytes(),
            ),
            (
                WireProtocol::Sql,
                "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)".to_string(),
                "schema",
                b"".as_slice(),
            ),
            (
                WireProtocol::Sql,
                format!("INSERT INTO users (id, name) VALUES (1, '{value}')"),
                "row",
                b"".as_slice(),
            ),
            (
                WireProtocol::Object,
                "CREATE_BUCKET bucket".into(),
                "bucket",
                b"".as_slice(),
            ),
            (
                WireProtocol::Object,
                "PUT bucket/shared".into(),
                "object",
                value.as_bytes(),
            ),
            (
                WireProtocol::Blob,
                "PUT payload".into(),
                "blob",
                value.as_bytes(),
            ),
        ] {
            let result = exchange(&listener, protocol, namespace, &operation, key, body);
            assert!(
                result.contains("RSF/1 executed"),
                "{namespace} {operation}: {result}"
            );
        }
    }
    drop(listener);
    let listener = DataPlaneListener::bind("127.0.0.1:0".parse().unwrap(), &state).unwrap();
    for (namespace, value) in [("app", "alice"), ("other", "bob")] {
        assert_eq!(
            tests::response_payload(&exchange(
                &listener,
                WireProtocol::Kv,
                namespace,
                "GET shared",
                "r",
                &[]
            )),
            value.as_bytes()
        );
        let rows = tests::response_payload(&exchange(
            &listener,
            WireProtocol::Sql,
            namespace,
            "SELECT * FROM users",
            "r",
            &[],
        ));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&rows).unwrap(),
            serde_json::json!([{"id":"1","name":value}])
        );
        assert_eq!(
            tests::response_payload(&exchange(
                &listener,
                WireProtocol::Object,
                namespace,
                "GET bucket/shared",
                "r",
                &[]
            )),
            value.as_bytes()
        );
    }
    let id = rsf_core::blob::ContentId::for_bytes(b"alice");
    let command = format!(
        "GET blake3:{}",
        id.as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    assert_eq!(
        tests::response_payload(&exchange(
            &listener,
            WireProtocol::Blob,
            "app",
            &command,
            "r",
            &[]
        )),
        b"alice"
    );
    assert!(
        exchange(&listener, WireProtocol::Blob, "other", &command, "r", &[])
            .contains("RSF/1 rejected")
    );
    let _ = std::fs::remove_dir_all(state);
}

#[test]
fn local_access_commands_persist_audited_grants_and_revocation() {
    let (state, listener) = setup("access_cli");
    let run = |command: &str, rest: &[&str]| {
        let args = rest
            .iter()
            .copied()
            .chain(["--state", state.to_str().unwrap()])
            .map(str::to_string)
            .collect::<Vec<_>>();
        crate::security::run_cli(command, &args)
    };
    run(
        "namespace",
        &[
            "create",
            "--tenant",
            "cli",
            "--database",
            "cli",
            "--namespace",
            "cli",
            "--region",
            "iad",
            "--surfaces",
            "kv",
        ],
    )
    .unwrap();
    run("auth", &["register", "--principal", "cli-client"]).unwrap();
    // A certificate cannot silently change ownership.
    let cert = state.join("tls/client.pem");
    assert!(
        run(
            "auth",
            &[
                "bind",
                "--principal",
                "cli-client",
                "--certificate",
                cert.to_str().unwrap()
            ]
        )
        .is_err()
    );
    run(
        "auth",
        &[
            "grant",
            "--principal",
            "app-client",
            "--namespace",
            "cli",
            "--surface",
            "kv",
            "--capability",
            "namespace-write",
        ],
    )
    .unwrap();
    run(
        "auth",
        &[
            "grant",
            "--principal",
            "app-client",
            "--namespace",
            "cli",
            "--surface",
            "kv",
            "--capability",
            "namespace-read",
        ],
    )
    .unwrap();
    assert!(
        exchange(&listener, WireProtocol::Kv, "cli", "SET key", "w", b"value")
            .contains("RSF/1 executed")
    );
    run(
        "auth",
        &[
            "revoke-grant",
            "--principal",
            "app-client",
            "--namespace",
            "cli",
            "--surface",
            "kv",
            "--capability",
            "namespace-write",
        ],
    )
    .unwrap();
    assert!(
        exchange(
            &listener,
            WireProtocol::Kv,
            "cli",
            "SET key",
            "w2",
            b"changed"
        )
        .contains("no matching grant")
    );
    assert_eq!(
        tests::response_payload(&exchange(
            &listener,
            WireProtocol::Kv,
            "cli",
            "GET key",
            "r",
            &[]
        )),
        b"value"
    );
    run(
        "auth",
        &[
            "revoke-certificate",
            "--certificate",
            cert.to_str().unwrap(),
        ],
    )
    .unwrap();
    assert!(
        exchange(&listener, WireProtocol::Kv, "cli", "GET key", "r", &[]).contains("not bound")
    );
    run(
        "auth",
        &[
            "bind",
            "--principal",
            "app-client",
            "--certificate",
            cert.to_str().unwrap(),
        ],
    )
    .unwrap();
    assert_eq!(
        tests::response_payload(&exchange(
            &listener,
            WireProtocol::Kv,
            "cli",
            "GET key",
            "r",
            &[]
        )),
        b"value"
    );
    let audit = rsf_observability::DurableAuditLog::recover(state.join("audit")).unwrap();
    for action in [
        "namespace.create",
        "auth.register",
        "auth.grant",
        "auth.revoke-grant",
        "auth.revoke-certificate",
        "auth.bind",
    ] {
        assert!(audit.iter().any(|record| record.event.action == action));
    }
    let _ = std::fs::remove_dir_all(state);
}

#[test]
fn missing_tls_identity_and_legacy_global_storage_fail_closed() {
    let state = tests::test_path("missing_tls");
    assert!(DataPlaneListener::bind("127.0.0.1:0".parse().unwrap(), &state).is_err());
    assert!(!state.exists());
    let (state, listener) = setup("legacy_isolation");
    std::fs::create_dir(state.join("kv")).unwrap();
    assert!(
        exchange(&listener, WireProtocol::Kv, "app", "GET key", "r", &[])
            .contains("explicit namespace migration")
    );
    assert!(!state.join("namespaces").exists());
    let _ = std::fs::remove_dir_all(state);
}
