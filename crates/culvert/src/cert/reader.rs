// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Unified certificate and private key reader, PEM decoder, and validator.
//! All comments and docstrings must be in English.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::sign::CertifiedKey;

use crate::core::error::TunnelError;

/// Centralized reader for TLS certificates, private keys, and CA trust stores.
pub struct CertReader;

impl CertReader {
    /// Reads and parses an X.509 certificate chain from a PEM file.
    pub fn read_cert_chain<P: AsRef<Path>>(
        path: P,
    ) -> Result<Vec<CertificateDer<'static>>, TunnelError> {
        let p = path.as_ref();
        if !p.exists() {
            return Err(TunnelError::Config(format!(
                "Certificate file not found: {:?}",
                p
            )));
        }
        let file = File::open(p).map_err(|e| {
            TunnelError::Config(format!("Failed to open certificate file {:?}: {}", p, e))
        })?;
        let mut reader = BufReader::new(file);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                TunnelError::Config(format!(
                    "Certificate file {:?} contains invalid PEM: {}",
                    p, e
                ))
            })?;
        if certs.is_empty() {
            return Err(TunnelError::Config(format!(
                "Certificate file {:?} contains no certificates",
                p
            )));
        }
        Ok(certs)
    }

    /// Reads and parses a private key from a PEM file (PKCS#1, PKCS#8, or SEC1).
    pub fn read_private_key<P: AsRef<Path>>(
        path: P,
    ) -> Result<PrivateKeyDer<'static>, TunnelError> {
        let p = path.as_ref();
        if !p.exists() {
            return Err(TunnelError::Config(format!(
                "Private key file not found: {:?}",
                p
            )));
        }
        let file = File::open(p).map_err(|e| {
            TunnelError::Config(format!("Failed to open private key file {:?}: {}", p, e))
        })?;
        let mut reader = BufReader::new(file);
        let key = rustls_pemfile::private_key(&mut reader).map_err(|e| {
            TunnelError::Config(format!(
                "Private key file {:?} contains invalid private key: {}",
                p, e
            ))
        })?;
        key.ok_or_else(|| {
            TunnelError::Config(format!("Private key file {:?} contains no private key", p))
        })
    }

    /// Reads and parses trusted CA certificates from a PEM file into a `RootCertStore`.
    pub fn read_root_store<P: AsRef<Path>>(path: P) -> Result<RootCertStore, TunnelError> {
        let p = path.as_ref();
        if !p.exists() {
            return Err(TunnelError::Config(format!(
                "CA certificate file not found: {:?}",
                p
            )));
        }
        let file = File::open(p).map_err(|e| {
            TunnelError::Tls(format!("Failed to open CA certificate {:?}: {}", p, e))
        })?;
        let mut reader = BufReader::new(file);
        let mut root_store = RootCertStore::empty();
        let mut count = 0;
        for cert in rustls_pemfile::certs(&mut reader) {
            let cert =
                cert.map_err(|e| TunnelError::Tls(format!("Invalid CA cert in {:?}: {}", p, e)))?;
            root_store.add(cert).map_err(|e| {
                TunnelError::Tls(format!("Failed to add CA cert to root store: {}", e))
            })?;
            count += 1;
        }
        if count == 0 {
            return Err(TunnelError::Tls(format!(
                "No certificates found in CA file {:?}",
                p
            )));
        }
        Ok(root_store)
    }

    /// Reads a server certificate chain and private key, validates that the private key
    /// matches the end-entity certificate, and constructs an `Arc<CertifiedKey>`.
    pub fn read_server_credentials<P: AsRef<Path>, Q: AsRef<Path>>(
        cert_path: P,
        key_path: Q,
    ) -> Result<Arc<CertifiedKey>, TunnelError> {
        let cert_p = cert_path.as_ref();
        let key_p = key_path.as_ref();

        let certs = Self::read_cert_chain(cert_p)?;
        let key = Self::read_private_key(key_p)?;

        // Build signing key using ring crypto provider
        let signing_key = rustls::crypto::ring::sign::any_supported_type(&key).map_err(|e| {
            TunnelError::Tls(format!(
                "Unsupported private key format in {:?}: {:?}",
                key_p, e
            ))
        })?;

        let certified_key = CertifiedKey::new(certs, signing_key);
        if let Err(e) = certified_key.keys_match() {
            return Err(TunnelError::Tls(format!(
                "Certificate {:?} does not match private key {:?}: {:?}",
                cert_p, key_p, e
            )));
        }
        Ok(Arc::new(certified_key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_test_cert_and_key() -> (String, String) {
        let cert_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_pem = cert_key.cert.pem();
        let key_pem = cert_key.signing_key.serialize_pem();
        (cert_pem, key_pem)
    }

    #[test]
    fn test_cert_reader_success() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_pem, key_pem) = generate_test_cert_and_key();

        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("server.key");
        std::fs::write(&cert_path, &cert_pem).unwrap();
        std::fs::write(&key_path, &key_pem).unwrap();

        let chain = CertReader::read_cert_chain(&cert_path).unwrap();
        assert_eq!(chain.len(), 1);

        let key = CertReader::read_private_key(&key_path).unwrap();
        assert!(matches!(key, rustls::pki_types::PrivateKeyDer::Pkcs8(_)));

        let creds = CertReader::read_server_credentials(&cert_path, &key_path).unwrap();
        assert_eq!(creds.cert.len(), 1);
    }

    #[test]
    fn test_cert_reader_mismatched_key_fails() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_pem, _) = generate_test_cert_and_key();
        let (_, mismatched_key_pem) = generate_test_cert_and_key();

        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("mismatched.key");
        std::fs::write(&cert_path, &cert_pem).unwrap();
        std::fs::write(&key_path, &mismatched_key_pem).unwrap();

        let res = CertReader::read_server_credentials(&cert_path, &key_path);
        assert!(res.is_err());
        let err_str = res.unwrap_err().to_string();
        assert!(err_str.contains("does not match private key"));
    }

    #[test]
    fn test_cert_reader_missing_file_fails() {
        let res = CertReader::read_cert_chain("non_existent_file.crt");
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("not found"));

        let res_key = CertReader::read_private_key("non_existent_file.key");
        assert!(res_key.is_err());
        assert!(res_key.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_cert_reader_invalid_pem_fails() {
        let dir = tempfile::tempdir().unwrap();
        let corrupt_path = dir.path().join("corrupt.pem");
        std::fs::write(&corrupt_path, "NOT A VALID PEM").unwrap();

        let res = CertReader::read_cert_chain(&corrupt_path);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("no certificates"));

        let res_key = CertReader::read_private_key(&corrupt_path);
        assert!(res_key.is_err());
        assert!(res_key.unwrap_err().to_string().contains("no private key"));
    }

    #[test]
    fn test_cert_reader_root_store() {
        let dir = tempfile::tempdir().unwrap();
        let (ca_pem, _) = generate_test_cert_and_key();
        let ca_path = dir.path().join("ca.crt");
        std::fs::write(&ca_path, &ca_pem).unwrap();

        let store = CertReader::read_root_store(&ca_path).unwrap();
        assert_eq!(store.roots.len(), 1);
    }
}
