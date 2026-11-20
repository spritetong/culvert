// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! TLS connector builder based on Rustls.
//! All comments and docstrings must be in English.

use rustls::client::danger::{ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::TlsConnector;

use crate::config::model::{ClientAuthMode, TlsConfig};
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
            let custom_roots = crate::cert::CertReader::read_root_store(ca_path)?;
            for root in custom_roots.roots {
                let _ = root_store.roots.push(root);
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
                let client_certs = crate::cert::CertReader::read_cert_chain(cert_path)?;
                let key = crate::cert::CertReader::read_private_key(key_path)?;
                builder
                    .with_client_auth_cert(client_certs, key)
                    .map_err(|e| TunnelError::Tls(format!("Failed to configure mTLS: {}", e)))?
            }
            (Some(_), None) => {
                return Err(TunnelError::Tls(
                    "client_certificate is specified but client_key is missing for mTLS"
                        .to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(TunnelError::Tls(
                    "client_key is specified but client_certificate is missing for mTLS"
                        .to_string(),
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
            let certs = crate::cert::CertReader::read_cert_chain(cp)?;
            let key = crate::cert::CertReader::read_private_key(kp)?;
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

    /// Loads trusted CA certificates from a file into a `RootCertStore`.
    pub fn load_root_certs<P: AsRef<Path>>(ca_path: P) -> Result<RootCertStore, TunnelError> {
        crate::cert::CertReader::read_root_store(ca_path)
    }

    /// Builds a `rustls::ServerConfig` with dynamic certificate resolution and hot-reloading support.
    #[allow(clippy::too_many_arguments)]
    pub fn build_dynamic_server_config(
        cert_manager: &crate::cert::CertManager,
        cert_path: Option<&str>,
        key_path: Option<&str>,
        client_ca_path: Option<&str>,
        client_auth: Option<ClientAuthMode>,
        alpn: Option<Vec<Vec<u8>>>,
        min_version: Option<&str>,
        max_version: Option<&str>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<rustls::ServerConfig, TunnelError> {
        let versions = Self::parse_protocol_versions(min_version, max_version)?;
        let resolver = cert_manager.resolve_server_cert(cert_path, key_path, cancel.clone())?;

        let effective_auth = match (client_auth, client_ca_path) {
            (Some(mode), _) => mode,
            (None, Some(_)) => ClientAuthMode::Required,
            (None, None) => ClientAuthMode::None,
        };

        let builder = rustls::ServerConfig::builder_with_protocol_versions(versions);

        let builder_with_client_auth = match effective_auth {
            ClientAuthMode::None => builder.with_no_client_auth(),
            ClientAuthMode::Required | ClientAuthMode::Optional => {
                let ca_path = client_ca_path.ok_or_else(|| {
                    TunnelError::Tls(
                        "Client certificate authentication requires a client CA certificate"
                            .to_string(),
                    )
                })?;
                let client_verifier = cert_manager.register_client_ca(
                    std::path::PathBuf::from(ca_path),
                    effective_auth,
                    cancel,
                )?;
                builder.with_client_cert_verifier(client_verifier)
            }
        };

        let mut server_config = builder_with_client_auth.with_cert_resolver(resolver);

        if let Some(alpn_protocols) = alpn {
            server_config.alpn_protocols = alpn_protocols;
        }

        Ok(server_config)
    }

    /// Builds a `rustls::ServerConfig` from certificate and private key paths or self-signed fallback,
    /// optional client CA authentication (mTLS), and validated protocol version bounds.
    pub fn build_server_config(
        cert_path: Option<&str>,
        key_path: Option<&str>,
        client_ca_path: Option<&str>,
        client_auth: Option<ClientAuthMode>,
        alpn: Option<Vec<Vec<u8>>>,
        min_version: Option<&str>,
        max_version: Option<&str>,
    ) -> Result<rustls::ServerConfig, TunnelError> {
        Self::build_dynamic_server_config(
            &crate::cert::CertManager::new(),
            cert_path,
            key_path,
            client_ca_path,
            client_auth,
            alpn,
            min_version,
            max_version,
            tokio_util::sync::CancellationToken::new(),
        )
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
            None,
            None,
            Some(alpn.clone()),
            Some("1.3"),
            Some("1.3"),
        )
        .unwrap();
        assert_eq!(cfg.alpn_protocols, alpn);
    }

    #[test]
    fn test_build_server_config_with_client_auth() {
        let dir = tempfile::tempdir().unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_params = rcgen::CertificateParams::default();
        let ca = rcgen::CertifiedIssuer::self_signed(ca_params, &ca_key).unwrap();
        let ca_cert_pem = ca.as_ref().pem();
        let ca_path = dir.path().join("ca.crt");
        std::fs::write(&ca_path, &ca_cert_pem).unwrap();

        // Required mode
        let cfg_req = TlsConfigBuilder::build_server_config(
            None,
            None,
            ca_path.to_str(),
            Some(ClientAuthMode::Required),
            None,
            None,
            None,
        );
        assert!(cfg_req.is_ok());

        // Default to Required when client_ca_path is present without explicit mode
        let cfg_auto = TlsConfigBuilder::build_server_config(
            None,
            None,
            ca_path.to_str(),
            None,
            None,
            None,
            None,
        );
        assert!(cfg_auto.is_ok());

        // Optional mode
        let cfg_opt = TlsConfigBuilder::build_server_config(
            None,
            None,
            ca_path.to_str(),
            Some(ClientAuthMode::Optional),
            None,
            None,
            None,
        );
        assert!(cfg_opt.is_ok());

        // Missing client CA when required -> error
        let cfg_err = TlsConfigBuilder::build_server_config(
            None,
            None,
            None,
            Some(ClientAuthMode::Required),
            None,
            None,
            None,
        );
        assert!(cfg_err.is_err());
    }
}
