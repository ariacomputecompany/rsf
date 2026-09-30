use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier};
use sha2::{Digest, Sha256};

pub fn certificate_fingerprint(certificate: &[u8]) -> [u8; 32] {
    Sha256::digest(certificate).into()
}

pub fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let mut reader = BufReader::new(
        File::open(path)
            .map_err(|err| format!("cannot read TLS certificate {}: {err}", path.display()))?,
    );
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| format!("invalid PEM certificate in {}", path.display()))?;
    if certificates.is_empty() {
        return Err(format!("no certificates in {}", path.display()));
    }
    Ok(certificates)
}

pub fn server_config(state: &Path) -> Result<Arc<ServerConfig>, String> {
    let root = state.join("tls");
    let chain = certificates(&root.join("server.pem"))?;
    let mut key_reader = BufReader::new(
        File::open(root.join("server-key.pem"))
            .map_err(|err| format!("cannot read TLS server private key: {err}"))?,
    );
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|_| "invalid TLS server private key PEM")?
        .ok_or("TLS server private key is missing")?;
    let mut roots = RootCertStore::empty();
    for certificate in certificates(&root.join("client-ca.pem"))? {
        roots
            .add(certificate)
            .map_err(|_| "invalid client CA certificate")?;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|err| format!("invalid client certificate trust configuration: {err}"))?;
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|err| err.to_string())?
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain, key)
        .map_err(|err| format!("invalid TLS server identity: {err}"))?;
    // Every connection presents and validates a current client certificate.
    config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    config.send_tls13_tickets = 0;
    Ok(Arc::new(config))
}
