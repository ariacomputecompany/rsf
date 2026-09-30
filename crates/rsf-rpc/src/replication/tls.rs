use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::{ClientConfig, RootCertStore, ServerConfig, server::WebPkiClientVerifier};
use sha2::{Digest, Sha256};

/// A dedicated peer identity with both serverAuth and clientAuth usages.
/// CA validation and exact leaf fingerprint binding are both required.
#[derive(Clone, Debug)]
pub struct PeerIdentity {
    pub(super) server: Arc<ServerConfig>,
    pub(super) client: Arc<ClientConfig>,
    pub fingerprint: [u8; 32],
}

pub fn fingerprint(der: &[u8]) -> [u8; 32] {
    Sha256::digest(der).into()
}

impl PeerIdentity {
    pub fn load(directory: &Path) -> Result<Self, String> {
        let read_certs = |name: &str| {
            let file = File::open(directory.join(name))
                .map_err(|err| format!("cannot open peer {name}: {err}"))?;
            rustls_pemfile::certs(&mut BufReader::new(file))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| format!("invalid peer {name}: {err}"))
        };
        let certificates = read_certs("certificate.pem")?;
        let leaf = certificates
            .first()
            .ok_or("peer certificate chain is empty")?;
        let fingerprint = fingerprint(leaf.as_ref());
        let file = File::open(directory.join("key.pem"))
            .map_err(|err| format!("cannot open peer private key: {err}"))?;
        let key = rustls_pemfile::private_key(&mut BufReader::new(file))
            .map_err(|err| format!("invalid peer private key: {err}"))?
            .ok_or("peer private key is missing")?;
        let mut roots = RootCertStore::empty();
        for certificate in read_certs("ca.pem")? {
            roots
                .add(certificate)
                .map_err(|err| format!("invalid peer CA: {err}"))?;
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots.clone()), provider.clone())
                .build()
                .map_err(|err| err.to_string())?;
        let mut server = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|err| err.to_string())?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates.clone(), key.clone_key())
            .map_err(|err| err.to_string())?;
        server.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        server.send_tls13_tickets = 0;
        let mut client = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|err| err.to_string())?
            .with_root_certificates(roots)
            .with_client_auth_cert(certificates, key)
            .map_err(|err| err.to_string())?;
        client.resumption = rustls::client::Resumption::disabled();
        Ok(Self {
            server: Arc::new(server),
            client: Arc::new(client),
            fingerprint,
        })
    }
}
