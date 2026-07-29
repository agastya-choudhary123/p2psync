//! Transport: raw TCP, optionally wrapped in TLS.
//!
//! Peers are symmetric, so each one is both a rustls server and a rustls
//! client. Certificates are self-signed per process and the peer's identity
//! comes from the `Hello` frame, not from the certificate chain — see the note
//! on `AcceptAny` below.

use anyhow::{Context, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, ServerConfig, SignatureScheme};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// Either a plain or TLS-wrapped stream, so the rest of the code is transport
/// agnostic.
pub enum Stream {
    Plain(TcpStream),
    ServerTls(Box<tokio_rustls::server::TlsStream<TcpStream>>, [u8; 32]),
    ClientTls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

/// SHA-256 of a DER-encoded certificate.
pub fn fingerprint(der: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(der);
    h.finalize().into()
}

impl Stream {
    /// SHA-256 of the listener's TLS certificate, for authentication channel
    /// binding. `None` over plaintext, where there is nothing to bind to.
    pub fn channel_binding(&self) -> Option<[u8; 32]> {
        match self {
            Stream::Plain(_) => None,
            // We are the listener: bind to the certificate we presented.
            Stream::ServerTls(_, fp) => Some(*fp),
            // We dialed: bind to the certificate we were shown. A MITM must
            // present its own here, which is exactly what we want to detect.
            Stream::ClientTls(s) => {
                let (_, conn) = s.get_ref();
                conn.peer_certificates()
                    .and_then(|certs| certs.first())
                    .map(|cert| fingerprint(cert.as_ref()))
            }
        }
    }

    pub fn split(
        self,
    ) -> (
        Box<dyn AsyncRead + Send + Unpin>,
        Box<dyn AsyncWrite + Send + Unpin>,
    ) {
        match self {
            Stream::Plain(s) => {
                let (r, w) = tokio::io::split(s);
                (Box::new(r), Box::new(w))
            }
            Stream::ServerTls(s, _) => {
                let (r, w) = tokio::io::split(*s);
                (Box::new(r), Box::new(w))
            }
            Stream::ClientTls(s) => {
                let (r, w) = tokio::io::split(*s);
                (Box::new(r), Box::new(w))
            }
        }
    }
}

#[derive(Clone)]
pub struct Tls {
    acceptor: TlsAcceptor,
    connector: TlsConnector,
    /// SHA-256 of our own certificate, used as the channel binding when we are
    /// the listener (the dialing side derives the same value from the
    /// certificate it was presented).
    own_fingerprint: [u8; 32],
}

/// Accepts any certificate.
///
/// Certificates are self-signed and ephemeral, so validating a chain would be
/// meaningless. Peer identity comes from the authentication handshake instead:
/// with `--secret`, both sides prove knowledge of a shared key bound to *this*
/// certificate, which is what rules out an active MITM (see `crate::auth`).
/// Without a secret, TLS protects only against a passive observer.
#[derive(Debug)]
struct AcceptAny;

impl ServerCertVerifier for AcceptAny {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

impl Tls {
    /// Generate a self-signed certificate and build both halves of the config.
    pub fn self_signed() -> Result<Self> {
        let cert = rcgen::generate_simple_self_signed(vec!["p2psync.local".to_string()])
            .context("generating self-signed certificate")?;
        let cert_der = CertificateDer::from(cert.cert.der().to_vec());
        let own_fingerprint = fingerprint(cert_der.as_ref());
        let key_der = PrivateKeyDer::try_from(cert.key_pair.serialize_der())
            .map_err(|e| anyhow::anyhow!("serializing key: {e}"))?;

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)?;
        let client = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAny))
            .with_no_client_auth();

        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(server)),
            connector: TlsConnector::from(Arc::new(client)),
            own_fingerprint,
        })
    }
}

pub async fn accept(tcp: TcpStream, tls: Option<&Tls>) -> Result<Stream> {
    tcp.set_nodelay(true)?;
    match tls {
        None => Ok(Stream::Plain(tcp)),
        Some(t) => Ok(Stream::ServerTls(
            Box::new(t.acceptor.accept(tcp).await?),
            t.own_fingerprint,
        )),
    }
}

pub async fn connect(addr: &str, tls: Option<&Tls>) -> Result<Stream> {
    let tcp = TcpStream::connect(addr).await.with_context(|| format!("connecting to {addr}"))?;
    tcp.set_nodelay(true)?;
    match tls {
        None => Ok(Stream::Plain(tcp)),
        Some(t) => {
            let name = ServerName::try_from("p2psync.local")?;
            Ok(Stream::ClientTls(Box::new(t.connector.connect(name, tcp).await?)))
        }
    }
}
