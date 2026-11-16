use std::path::Path;
use std::sync::{Arc, OnceLock};

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rsf_auth::{
    Capability, DurableAuthEngine, Grant, Principal, PrincipalId, PrincipalKind, Resource,
};
use rsf_core::Epoch;
use rsf_tenancy::{DatabaseSurface, DurableTenancyCatalog};
use rustls::pki_types::PrivatePkcs8KeyDer;

pub struct TestPki {
    pub server_pem: String,
    pub server_key: String,
    pub ca_pem: String,
    pub client_pem: String,
    pub fingerprint: [u8; 32],
    pub client: Arc<rustls::ClientConfig>,
    pub anonymous: Arc<rustls::ClientConfig>,
    pub unregistered: Arc<rustls::ClientConfig>,
    pub expired: Arc<rustls::ClientConfig>,
    pub untrusted: Arc<rustls::ClientConfig>,
}

pub fn pki() -> &'static TestPki {
    static PKI: OnceLock<TestPki> = OnceLock::new();
    PKI.get_or_init(|| {
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let key = KeyPair::generate().unwrap();
        let ca = params.self_signed(&key).unwrap();
        let issuer = Issuer::new(params, key);
        let leaf = |usage| {
            let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
            params.extended_key_usages = vec![usage];
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            let key = KeyPair::generate().unwrap();
            let cert = params.signed_by(&key, &issuer).unwrap();
            (cert, key)
        };
        let (server, server_key) = leaf(ExtendedKeyUsagePurpose::ServerAuth);
        let (client, client_key) = leaf(ExtendedKeyUsagePurpose::ClientAuth);
        let (unregistered, unregistered_key) = leaf(ExtendedKeyUsagePurpose::ClientAuth);
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca.der().clone()).unwrap();
        let builder = || {
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots.clone())
        };
        let config = builder()
            .with_client_auth_cert(
                vec![client.der().clone()],
                PrivatePkcs8KeyDer::from(client_key.serialize_der()).into(),
            )
            .unwrap();
        let unknown_config = builder()
            .with_client_auth_cert(
                vec![unregistered.der().clone()],
                PrivatePkcs8KeyDer::from(unregistered_key.serialize_der()).into(),
            )
            .unwrap();
        let mut expired_params = CertificateParams::new(vec!["expired".into()]).unwrap();
        expired_params.not_before = rcgen::date_time_ymd(1999, 1, 1);
        expired_params.not_after = rcgen::date_time_ymd(2000, 1, 1);
        expired_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let expired_key = KeyPair::generate().unwrap();
        let expired_cert = expired_params.signed_by(&expired_key, &issuer).unwrap();
        let expired = builder()
            .with_client_auth_cert(
                vec![expired_cert.der().clone()],
                PrivatePkcs8KeyDer::from(expired_key.serialize_der()).into(),
            )
            .unwrap();
        let unknown_key = KeyPair::generate().unwrap();
        let unknown_cert = CertificateParams::new(vec!["untrusted".into()])
            .unwrap()
            .self_signed(&unknown_key)
            .unwrap();
        let untrusted = builder()
            .with_client_auth_cert(
                vec![unknown_cert.der().clone()],
                PrivatePkcs8KeyDer::from(unknown_key.serialize_der()).into(),
            )
            .unwrap();
        TestPki {
            server_pem: server.pem(),
            server_key: server_key.serialize_pem(),
            ca_pem: ca.pem(),
            client_pem: client.pem(),
            fingerprint: crate::tls::certificate_fingerprint(client.der().as_ref()),
            client: Arc::new(config),
            anonymous: Arc::new(builder().with_no_client_auth()),
            unregistered: Arc::new(unknown_config),
            expired: Arc::new(expired),
            untrusted: Arc::new(untrusted),
        }
    })
}

pub fn provision(state: &Path) {
    let pki = pki();
    let root = state.join("tls");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("server.pem"), &pki.server_pem).unwrap();
    std::fs::write(root.join("server-key.pem"), &pki.server_key).unwrap();
    std::fs::write(root.join("client-ca.pem"), &pki.ca_pem).unwrap();
    std::fs::write(root.join("client.pem"), &pki.client_pem).unwrap();
    let mut tenancy = DurableTenancyCatalog::open(state.join("tenancy"), Epoch(1)).unwrap();
    let tenant = tenancy.create_tenant("test", "iad").unwrap();
    let database = tenancy
        .create_database(
            &tenant.name,
            "test",
            [
                DatabaseSurface::Relational,
                DatabaseSurface::Kv,
                DatabaseSurface::Object,
                DatabaseSurface::Blob,
            ],
        )
        .unwrap();
    tenancy
        .bind_namespace(&tenant.name, &database.name, "app")
        .unwrap();
    let mut auth = DurableAuthEngine::open(state.join("auth"), Epoch(1)).unwrap();
    let principal = PrincipalId("app-client".into());
    auth.register_principal(Principal {
        id: principal.clone(),
        kind: PrincipalKind::Service,
    })
    .unwrap();
    auth.bind_certificate(pki.fingerprint, principal.clone())
        .unwrap();
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
            principal: principal.clone(),
            capability,
            resource: Resource::Namespace("app".into()),
        })
        .unwrap();
    }
}

pub fn send(
    addr: std::net::SocketAddr,
    frame: &[u8],
    config: Arc<rustls::ClientConfig>,
) -> std::io::Result<String> {
    use std::io::{Read, Write};
    let socket = std::net::TcpStream::connect(addr)?;
    socket.set_nodelay(true)?;
    socket.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
    socket.set_write_timeout(Some(std::time::Duration::from_secs(3)))?;
    let conn = rustls::ClientConnection::new(config, "localhost".try_into().unwrap()).unwrap();
    let mut stream = rustls::StreamOwned::new(conn, socket);
    stream.write_all(&(frame.len() as u32).to_le_bytes())?;
    stream.write_all(frame)?;
    stream.flush()?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}
