use crate::{Config, Tls};
use std::io::{BufReader, Result};
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::TlsConnector;

pub struct Connector {
    inner: ConnectorImpl,
}

enum ConnectorImpl {
    Tcp,
    Tls(TlsConnectorState),
}

struct TlsConnectorState {
    connector: TlsConnector,
    use_sni: bool,
}

impl Connector {
    pub fn new(config: &Config) -> Result<Self> {
        if let Some(tls_config) = config.tls() {
            Self::tls(tls_config)
        } else {
            Self::plaintext()
        }
    }

    fn plaintext() -> Result<Self> {
        Ok(Connector {
            inner: ConnectorImpl::Tcp,
        })
    }

    pub fn tls(tls_config: &Tls) -> Result<Self> {
        let mut root_store = rustls::RootCertStore::empty();

        if let Some(ca_file) = tls_config.ca_file() {
            let file = std::fs::File::open(ca_file)?;
            let mut reader = BufReader::new(file);
            let certs = rustls_pemfile::certs(&mut reader)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for cert in certs {
                root_store.add(cert).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
            }
        } else {
            let native_certs = rustls_native_certs::load_native_certs();
            for cert in native_certs.certs {
                root_store.add(cert).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
            }
        }

        let builder = if tls_config.verify_hostname() {
            rustls::ClientConfig::builder().with_root_certificates(root_store)
        } else {
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
                .with_no_client_auth();
            // Fall through to mTLS check below won't work with this path,
            // so handle both cases explicitly
            let config = if has_mtls_config(tls_config) {
                let (certs, key) = load_client_identity(tls_config)?;
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(NoVerifier))
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
            } else {
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(NoVerifier))
                    .with_no_client_auth()
            };

            return Ok(Connector {
                inner: ConnectorImpl::Tls(TlsConnectorState {
                    connector: TlsConnector::from(Arc::new(config)),
                    use_sni: tls_config.use_sni(),
                }),
            });
        };

        let config = if has_mtls_config(tls_config) {
            let (certs, key) = load_client_identity(tls_config)?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
        } else {
            builder.with_no_client_auth()
        };

        Ok(Connector {
            inner: ConnectorImpl::Tls(TlsConnectorState {
                connector: TlsConnector::from(Arc::new(config)),
                use_sni: tls_config.use_sni(),
            }),
        })
    }

    pub async fn connect(&self, addr: &str) -> Result<Stream> {
        match &self.inner {
            ConnectorImpl::Tcp => {
                let s = tokio::net::TcpStream::connect(addr).await?;
                let res = s.set_nodelay(true);
                if res.is_err() {
                    eprintln!("Error setting TCP_NODELAY: {:?}", res.err());
                }
                Ok(Stream {
                    inner: StreamImpl::Tcp(s),
                })
            }
            ConnectorImpl::Tls(state) => {
                let stream = tokio::net::TcpStream::connect(addr).await?;
                let _ = stream.set_nodelay(true);

                let domain = addr.split(':').next().unwrap_or(addr);
                let server_name = if state.use_sni {
                    ServerName::try_from(domain.to_owned()).map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string())
                    })?
                } else {
                    ServerName::try_from(domain.to_owned()).map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string())
                    })?
                };

                let tls_stream = state.connector.connect(server_name, stream).await?;
                Ok(Stream {
                    inner: StreamImpl::Tls(tls_stream),
                })
            }
        }
    }
}

fn has_mtls_config(tls_config: &Tls) -> bool {
    tls_config.private_key().is_some()
        && (tls_config.certificate().is_some() || tls_config.certificate_chain().is_some())
}

fn load_client_identity(
    tls_config: &Tls,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let key_path = tls_config.private_key().unwrap();
    let key_file = std::fs::File::open(key_path)?;
    let mut key_reader = BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "no private key found in PEM file"))?;

    let certs = match (tls_config.certificate(), tls_config.certificate_chain()) {
        (Some(cert), Some(chain)) => {
            let cert_file = std::fs::File::open(cert)?;
            let mut cert_reader = BufReader::new(cert_file);
            let mut certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            let chain_file = std::fs::File::open(chain)?;
            let mut chain_reader = BufReader::new(chain_file);
            let chain_certs: Vec<CertificateDer<'static>> =
                rustls_pemfile::certs(&mut chain_reader)
                    .collect::<std::result::Result<Vec<_>, _>>()?;
            certs.extend(chain_certs);
            certs
        }
        (Some(cert_or_chain), None) | (None, Some(cert_or_chain)) => {
            let file = std::fs::File::open(cert_or_chain)?;
            let mut reader = BufReader::new(file);
            rustls_pemfile::certs(&mut reader)
                .collect::<std::result::Result<Vec<_>, _>>()?
        }
        (None, None) => unreachable!(),
    };

    Ok((certs, key))
}

/// Certificate verifier that accepts any certificate.
/// Used when `verify_hostname: false` is set in the TLS config.
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub struct Stream {
    inner: StreamImpl,
}

enum StreamImpl {
    Tcp(tokio::net::TcpStream),
    Tls(tokio_rustls::client::TlsStream<tokio::net::TcpStream>),
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            StreamImpl::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            StreamImpl::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_flush(cx),
            StreamImpl::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            StreamImpl::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl hyper::rt::Read for Stream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        mut rbc: hyper::rt::ReadBufCursor<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => {
                let mut buf = ReadBuf::uninit(unsafe { rbc.as_mut() });
                std::pin::Pin::new(s).poll_read(cx, &mut buf)
            }
            StreamImpl::Tls(s) => {
                let mut buf = ReadBuf::uninit(unsafe { rbc.as_mut() });
                std::pin::Pin::new(s).poll_read(cx, &mut buf)
            }
        }
    }
}

impl hyper::rt::Write for Stream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            StreamImpl::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_flush(cx),
            StreamImpl::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            StreamImpl::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}
