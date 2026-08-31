//! TLS / mutual TLS setup (feature `tls`). Broker verification (`--cafile`,
//! defaulting to the OS trust store), client certificates for mutual TLS
//! (`--cert`/`--key`), and `--insecure` to skip verification entirely.

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::{
    ClientConfig, DigitallySignedStruct, Error as TlsError, RootCertStore, SignatureScheme,
};
use tokio_rustls::TlsConnector;

use crate::cli::ConnectionArgs;
use crate::error::{Error, Result};

/// Wrap an already-connected TCP stream in TLS, per `args`.
pub async fn wrap(stream: TcpStream, args: &ConnectionArgs) -> Result<TlsStream<TcpStream>> {
    let config = build_config(args)?;
    let name = ServerName::try_from(args.broker.clone()).map_err(|e| {
        Error::Usage(format!(
            "--broker {:?} is not a valid TLS server name: {e}",
            args.broker
        ))
    })?;
    let connector = TlsConnector::from(Arc::new(config));
    Ok(connector.connect(name, stream).await?)
}

fn build_config(args: &ConnectionArgs) -> Result<ClientConfig> {
    let builder = ClientConfig::builder();

    let builder = if args.insecure {
        // Fires every connection, deliberately unlike the plaintext-password
        // warning: disabling certificate verification is the dangerous case,
        // and a warning that can be trained away by seeing it once defeats
        // the point.
        eprintln!("wispmq-cli: warning: --insecure disables TLS server certificate verification");
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoServerVerification))
    } else {
        let roots = load_root_store(args.cafile.as_deref())?;
        builder.with_root_certificates(roots)
    };

    Ok(match (&args.cert, &args.key) {
        (Some(cert), Some(key)) => {
            let certs = load_certs(cert)?;
            let key = load_key(key)?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| Error::Usage(format!("--cert/--key: {e}")))?
        }
        // clap's `requires_all` on --cert/--key means this arm is exactly
        // "neither given" — `requires_all` on each rules out exactly one set.
        _ => builder.with_no_client_auth(),
    })
}

/// `--cafile`, or the OS trust store when it's omitted — so a broker with a
/// certificate from a public CA works without any extra flags, and a private
/// deployment supplies its own bundle.
fn load_root_store(cafile: Option<&Path>) -> Result<RootCertStore> {
    let mut store = RootCertStore::empty();
    match cafile {
        Some(path) => {
            for cert in load_certs(path)? {
                store
                    .add(cert)
                    .map_err(|e| Error::Usage(format!("--cafile {}: {e}", path.display())))?;
            }
        }
        None => {
            let result = rustls_native_certs::load_native_certs();
            for err in &result.errors {
                eprintln!("wispmq-cli: warning: reading a native root certificate: {err}");
            }
            let (added, _ignored) = store.add_parsable_certificates(result.certs);
            if added == 0 {
                return Err(Error::Usage(
                    "no usable certificates found in the OS trust store; pass --cafile".into(),
                ));
            }
        }
    }
    Ok(store)
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file =
        std::fs::File::open(path).map_err(|e| Error::Usage(format!("{}: {e}", path.display())))?;
    let certs: std::result::Result<Vec<_>, _> =
        rustls_pemfile::certs(&mut BufReader::new(file)).collect();
    let certs = certs.map_err(|e| Error::Usage(format!("{}: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(Error::Usage(format!(
            "{}: no PEM certificates found",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file =
        std::fs::File::open(path).map_err(|e| Error::Usage(format!("{}: {e}", path.display())))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|e| Error::Usage(format!("{}: {e}", path.display())))?
        .ok_or_else(|| Error::Usage(format!("{}: no private key found", path.display())))
}

/// `--insecure`'s implementation: accept any certificate, any signature.
/// This is the standard "opt out of verification" `ServerCertVerifier` —
/// every method unconditionally succeeds; there is nothing else it could do
/// and still be "insecure" rather than "insecure except when it forgets to
/// be."
#[derive(Debug)]
struct NoServerVerification;

impl ServerCertVerifier for NoServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // Every scheme rustls itself knows how to name — verification is a
        // no-op above, so there is no narrower "supported" set to report.
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_cafile_is_a_usage_error_not_a_panic() {
        let err = load_certs(Path::new("/nonexistent/wispmq-cli-no-such-cafile.pem"))
            .expect_err("missing file");
        assert!(matches!(err, Error::Usage(_)));
    }

    #[test]
    fn an_empty_cafile_is_a_usage_error() {
        let path =
            std::env::temp_dir().join(format!("wispmq-cli-empty-cafile-{}", std::process::id()));
        std::fs::write(&path, b"not a certificate\n").unwrap();
        let err = load_certs(&path).expect_err("no PEM certs in file");
        assert!(matches!(err, Error::Usage(_)));
        let _ = std::fs::remove_file(&path);
    }
}
