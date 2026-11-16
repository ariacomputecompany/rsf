//! Explicit unprivileged Unix host qualification, not a simulated fsync EIO.
use super::*;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use std::fs;
use std::os::unix::fs::PermissionsExt;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "rsfd-owner-directory-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        )))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "native owner directory fixture retained: {}",
                self.0.display()
            );
        } else {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}
struct NoDirectoryRead {
    path: PathBuf,
    previous: fs::Permissions,
}
impl NoDirectoryRead {
    fn enter(path: &Path) -> Self {
        let previous = fs::metadata(path).unwrap().permissions();
        fs::set_permissions(path, fs::Permissions::from_mode(0o111)).unwrap();
        Self {
            path: path.to_path_buf(),
            previous,
        }
    }
    fn require_enforced(&self) {
        let error = File::open(&self.path).expect_err(
            "host qualification requires an unprivileged Unix user whose directory read permissions are enforced",
        );
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    }
}
impl Drop for NoDirectoryRead {
    fn drop(&mut self) {
        if let Err(error) = fs::set_permissions(&self.path, self.previous.clone()) {
            eprintln!("cannot restore {}: {error}", self.path.display());
        }
    }
}

fn provision(root: &Path, state: &Path) -> PathBuf {
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate().unwrap();
    let ca = params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(params, ca_key);
    let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ClientAuth,
        ExtendedKeyUsagePurpose::ServerAuth,
    ];
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    let key = KeyPair::generate().unwrap();
    let certificate = params.signed_by(&key, &issuer).unwrap();
    let tls = root.join("tls");
    fs::create_dir_all(&tls).unwrap();
    fs::write(tls.join("certificate.pem"), certificate.pem()).unwrap();
    fs::write(tls.join("key.pem"), key.serialize_pem()).unwrap();
    fs::write(tls.join("ca.pem"), ca.pem()).unwrap();
    let identity = PeerIdentity::load(&tls).unwrap();
    let fingerprint = identity
        .fingerprint
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let config = serde_json::json!({
        "cluster":"owner-directory-host-qualification", "node":0, "state":state,
        "tls":tls, "listen":"127.0.0.1:0", "timeout_ms":800,
        "peers":[{"node":0,"address":"127.0.0.1:1","server_name":"localhost","fingerprint":fingerprint}],
        "cells":[{"pg":1,"view":1,"primary":0,"voters":[0]}]
    });
    let path = root.join("peer.json");
    fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    path
}

#[test]
fn peer_open_ancestor_barrier_failure_precedes_identity_lock_or_listener() {
    let fixture = Fixture::new();
    let ancestor = fixture.0.join("outer");
    let state = ancestor.join("nested/state");
    fs::create_dir_all(&state).unwrap();
    let config = provision(&fixture.0, &state);
    // Separate config/TLS validation from the fault: neither lives beneath the
    // unreadable owner ancestor, and both must already be valid.
    load_config(&config).unwrap();
    {
        let fault = NoDirectoryRead::enter(&ancestor);
        fault.require_enforced();
        fs::create_dir_all(&state).unwrap();
        let error = match PeerService::open(&config) {
            Err(error) => error,
            Ok(_) => panic!("native owner escaped its failed ancestor directory barrier"),
        };
        assert!(!error.is_empty());
        assert!(
            fs::read_dir(&state).unwrap().next().is_none(),
            "native startup reached identity/lock/role provisioning before its outer barrier"
        );
    }
    let service = PeerService::open(&config).unwrap();
    assert_ne!(service.listener.local_addr().unwrap().port(), 0);
    assert!(state.join(".peer.lock").is_file());
    assert!(state.join("pg-1/MANIFEST").is_file());
    let identity = fs::read(state.join("IDENTITY.json")).unwrap();
    drop(service);
    // Existing-root restart must not infer that the previous visible path proves
    // completion. An actual parent-barrier error still prevents a usable owner.
    {
        let fault = NoDirectoryRead::enter(&ancestor);
        fault.require_enforced();
        assert!(PeerService::open(&config).is_err());
        assert_eq!(fs::read(state.join("IDENTITY.json")).unwrap(), identity);
    }
    let service = PeerService::open(&config).unwrap();
    assert_eq!(fs::read(state.join("IDENTITY.json")).unwrap(), identity);
    assert_ne!(service.listener.local_addr().unwrap().port(), 0);
    drop(service);
}
