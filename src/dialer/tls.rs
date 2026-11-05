// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! TLS connector builder based on Rustls.
//! All comments and docstrings must be in English.

use rustls::client::danger::{ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::TlsConnector;

use crate::config::model::TlsConfig;
use crate::core::error::TunnelError;

/// Dangerous certificate verifier that skips all validation checks.
#[derive(Debug)]
struct NoVerifyVerifier;

impl ServerCertVerifier for NoVerifyVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}

static ALL_VERSIONS: &[&rustls::SupportedProtocolVersion] =
    &[&rustls::version::TLS13, &rustls::version::TLS12];
static TLS13_ONLY: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS13];
static TLS12_ONLY: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS12];

/// Helper for constructing client TLS configurations from `TlsConfig`.
pub struct TlsConfigBuilder;

impl TlsConfigBuilder {
    /// Resolves and validates protocol versions for TLS client and server configurations.
    pub fn parse_protocol_versions(
        min_version: Option<&str>,
        max_version: Option<&str>,
    ) -> Result<&'static [&'static rustls::SupportedProtocolVersion], TunnelError> {
        match (min_version, max_version) {
            (None, None) => Ok(ALL_VERSIONS),
            (None, Some("1.3")) => Ok(ALL_VERSIONS),
            (None, Some("1.2")) => Ok(TLS12_ONLY),
            (Some("1.2"), None | Some("1.3")) => Ok(ALL_VERSIONS),
            (Some("1.2"), Some("1.2")) => Ok(TLS12_ONLY),
            (Some("1.3"), None | Some("1.3")) => Ok(TLS13_ONLY),
            (Some(min_v), Some(max_v)) if min_v == "1.3" && max_v == "1.2" => {
                Err(TunnelError::Config(format!(
                    "invalid TLS version range: min_version \"{min_v}\" > max_version \"{max_v}\""
                )))
            }
            (Some(v), _) | (_, Some(v)) => Err(TunnelError::Config(format!(
                "unsupported TLS version: \"{v}\" (expected \"1.2\" or \"1.3\")"
            ))),
        }
    }

    /// Builds a `rustls::ClientConfig` from the given configuration.
    pub fn build_client_config(config: &TlsConfig) -> Result<ClientConfig, TunnelError> {
        let mut root_store = RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };

        // Load custom CA certificate if configured
        if let Some(ref ca_path) = config.ca_certificate {
            let file = File::open(ca_path).map_err(|e| {
                TunnelError::Tls(format!(
                    "Failed to open CA certificate {:?}: {}",
                    ca_path, e
                ))
            })?;
            let mut reader = BufReader::new(file);
            for cert in rustls_pemfile::certs(&mut reader) {
                let cert = cert.map_err(|e| TunnelError::Tls(format!("Invalid CA cert: {}", e)))?;
                root_store
                    .add(cert)
                    .map_err(|e| TunnelError::Tls(format!("Failed to add CA cert: {}", e)))?;
            }
        }

        let versions = Self::parse_protocol_versions(
            config.min_version.as_deref(),
            config.max_version.as_deref(),
        )?;

        let builder = ClientConfig::builder_with_protocol_versions(versions);

        let builder = if config.is_insecure() {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifyVerifier))
        } else {
            builder.with_root_certificates(root_store)
        };

        // Configure client authentication (mTLS) if client certificate & key are provided
        let mut client_config = match (&config.client_certificate, &config.client_key) {
            (Some(cert_path), Some(key_path)) => {
                let cert_file = File::open(cert_path).map_err(|e| {
                    TunnelError::Tls(format!("Failed to open client cert {:?}: {}", cert_path, e))
                })?;
                let mut cert_reader = BufReader::new(cert_file);
                let client_certs: Vec<CertificateDer<'static>> =
                    rustls_pemfile::certs(&mut cert_reader)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| TunnelError::Tls(format!("Invalid client cert: {}", e)))?;

                let key_file = File::open(key_path).map_err(|e| {
                    TunnelError::Tls(format!("Failed to open client key {:?}: {}", key_path, e))
                })?;
                let mut key_reader = BufReader::new(key_file);
                let key = rustls_pemfile::private_key(&mut key_reader)
                    .map_err(|e| TunnelError::Tls(format!("Failed to read client key: {}", e)))?
                    .ok_or_else(|| {
                        TunnelError::Tls("No private key found in client key file".to_string())
                    })?;

                builder
                    .with_client_auth_cert(client_certs, key)
                    .map_err(|e| TunnelError::Tls(format!("Failed to configure mTLS: {}", e)))?
            }
            (Some(_), None) => {
                return Err(TunnelError::Tls(
                    "client_certificate is specified but client_key is missing for mTLS".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(TunnelError::Tls(
                    "client_key is specified but client_certificate is missing for mTLS".to_string(),
                ));
            }
            (None, None) => builder.with_no_client_auth(),
        };

        if let Some(alpn_list) = &config.alpn {
            client_config.alpn_protocols =
                alpn_list.iter().map(|s| s.as_bytes().to_vec()).collect();
        }

        Ok(client_config)
    }

    /// Builds a `tokio_rustls::TlsConnector` from the given configuration.
    pub fn build_connector(config: &TlsConfig) -> Result<TlsConnector, TunnelError> {
        let client_config = Self::build_client_config(config)?;
        Ok(TlsConnector::from(Arc::new(client_config)))
    }

    /// Loads certificates and private key from files, or generates a zero-config self-signed certificate.
    pub fn resolve_server_cert_and_key(
        cert_path: Option<&str>,
        key_path: Option<&str>,
    ) -> Result<
        (
            Vec<CertificateDer<'static>>,
            rustls::pki_types::PrivateKeyDer<'static>,
        ),
        TunnelError,
    > {
        if let (Some(cp), Some(kp)) = (cert_path, key_path) {
            let cert_file = File::open(cp).map_err(|e| {
                TunnelError::Tls(format!("Failed to open server cert {:?}: {}", cp, e))
            })?;
            let mut cert_reader = BufReader::new(cert_file);
            let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| TunnelError::Tls(format!("Invalid server cert: {}", e)))?;

            if certs.is_empty() {
                return Err(TunnelError::Tls(format!(
                    "No certificates found in {:?}",
                    cp
                )));
            }

            let key_file = File::open(kp).map_err(|e| {
                TunnelError::Tls(format!("Failed to open server key {:?}: {}", kp, e))
            })?;
            let mut key_reader = BufReader::new(key_file);
            let key = rustls_pemfile::private_key(&mut key_reader)
                .map_err(|e| TunnelError::Tls(format!("Failed to read server key: {}", e)))?
                .ok_or_else(|| {
                    TunnelError::Tls(format!("No private key found in server key file {:?}", kp))
                })?;

            Ok((certs, key))
        } else if cert_path.is_some() || key_path.is_some() {
            Err(TunnelError::Tls(
                "Both server certificate and private key must be provided together".to_string(),
            ))
        } else {
            // Generate an ephemeral self-signed certificate for zero-configuration fallback
            let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
            let cert_key = rcgen::generate_simple_self_signed(subject_alt_names).map_err(|e| {
                TunnelError::Tls(format!("Failed to generate self-signed cert: {}", e))
            })?;

            let cert_der = cert_key.cert.der().to_vec();
            let key_der = cert_key.signing_key.serialize_der();

            let cert_chain = vec![CertificateDer::from(cert_der)];
            let private_key = rustls::pki_types::PrivateKeyDer::Pkcs8(
                rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
            );

            Ok((cert_chain, private_key))
        }
    }

    /// Builds a `rustls::ServerConfig` from certificate and private key paths or self-signed fallback,
    /// with validated protocol version bounds.
    pub fn build_server_config(
        cert_path: Option<&str>,
        key_path: Option<&str>,
        alpn: Option<Vec<Vec<u8>>>,
        min_version: Option<&str>,
        max_version: Option<&str>,
    ) -> Result<rustls::ServerConfig, TunnelError> {
        let versions = Self::parse_protocol_versions(min_version, max_version)?;
        let (cert_chain, private_key) = Self::resolve_server_cert_and_key(cert_path, key_path)?;
        let mut server_config = rustls::ServerConfig::builder_with_protocol_versions(versions)
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| TunnelError::Tls(format!("Failed to build server TLS config: {}", e)))?;

        if let Some(alpn_protocols) = alpn {
            server_config.alpn_protocols = alpn_protocols;
        }

        Ok(server_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_protocol_versions_matrix() {
        // 1. (None, None) -> 1.2 + 1.3
        let v1 = TlsConfigBuilder::parse_protocol_versions(None, None).unwrap();
        assert_eq!(v1.len(), 2);

        // 2. (None, Some("1.3")) -> 1.2 + 1.3 (default min is 1.2)
        let v2 = TlsConfigBuilder::parse_protocol_versions(None, Some("1.3")).unwrap();
        assert_eq!(v2.len(), 2);

        // 3. (None, Some("1.2")) -> 1.2 only
        let v3 = TlsConfigBuilder::parse_protocol_versions(None, Some("1.2")).unwrap();
        assert_eq!(v3.len(), 1);
        assert_eq!(v3[0].version, rustls::version::TLS12.version);

        // 4. (Some("1.2"), None) -> 1.2 + 1.3
        let v4 = TlsConfigBuilder::parse_protocol_versions(Some("1.2"), None).unwrap();
        assert_eq!(v4.len(), 2);

        // 5. (Some("1.2"), Some("1.3")) -> 1.2 + 1.3
        let v5 = TlsConfigBuilder::parse_protocol_versions(Some("1.2"), Some("1.3")).unwrap();
        assert_eq!(v5.len(), 2);

        // 6. (Some("1.2"), Some("1.2")) -> 1.2 only (previously rejected as unsupported)
        let v6 = TlsConfigBuilder::parse_protocol_versions(Some("1.2"), Some("1.2")).unwrap();
        assert_eq!(v6.len(), 1);
        assert_eq!(v6[0].version, rustls::version::TLS12.version);

        // 7. (Some("1.3"), None) -> 1.3 only
        let v7 = TlsConfigBuilder::parse_protocol_versions(Some("1.3"), None).unwrap();
        assert_eq!(v7.len(), 1);
        assert_eq!(v7[0].version, rustls::version::TLS13.version);

        // 8. (Some("1.3"), Some("1.3")) -> 1.3 only
        let v8 = TlsConfigBuilder::parse_protocol_versions(Some("1.3"), Some("1.3")).unwrap();
        assert_eq!(v8.len(), 1);
        assert_eq!(v8[0].version, rustls::version::TLS13.version);

        // 9. Inverted range: min 1.3 > max 1.2 -> Err
        assert!(TlsConfigBuilder::parse_protocol_versions(Some("1.3"), Some("1.2")).is_err());

        // 10. Unsupported versions -> Err
        assert!(TlsConfigBuilder::parse_protocol_versions(Some("1.1"), None).is_err());
        assert!(TlsConfigBuilder::parse_protocol_versions(None, Some("1.4")).is_err());
        assert!(TlsConfigBuilder::parse_protocol_versions(Some("TLS1.2"), None).is_err());
    }

    #[test]
    fn test_build_server_config_with_versions() {
        let alpn = vec![b"h3".to_vec()];
        let cfg = TlsConfigBuilder::build_server_config(
            None,
            None,
            Some(alpn.clone()),
            Some("1.3"),
            Some("1.3"),
        )
        .unwrap();
        assert_eq!(cfg.alpn_protocols, alpn);
    }
}
