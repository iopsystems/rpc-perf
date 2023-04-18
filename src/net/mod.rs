// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use crate::Config;
use boring::ssl::{SslFiletype, SslMethod};
use boring::x509::X509;
use std::io::{Error, ErrorKind, Result};
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;

pub struct Connector {
    inner: ConnectorImpl,
}

impl Connector {
    pub fn new(config: &Config) -> Result<Self> {
        if config.tls().is_none() {
            return Ok(Connector {
                inner: ConnectorImpl::Tcp,
            });
        }

        let tls_config = config.tls().unwrap();

        let private_key = tls_config.private_key();
        let certificate = tls_config.certificate();
        let certificate_chain = tls_config.certificate_chain();

        let mut ssl_connector = boring::ssl::SslConnector::builder(SslMethod::tls_client())?;

        if let Some(ca_file) = tls_config.ca_file() {
            ssl_connector.set_ca_file(ca_file)?;
        }

        // mTLS configuration
        if private_key.is_some() && (certificate.is_some() || certificate_chain.is_some()) {
            ssl_connector.set_private_key_file(private_key.unwrap(), SslFiletype::PEM)?;

            match (certificate, certificate_chain) {
                (Some(cert), Some(chain)) => {
                    // assume cert is just a leaf and that we need to append the
                    // certs in the chain file after loading the leaf cert

                    ssl_connector.set_certificate_file(cert, SslFiletype::PEM)?;
                    let pem = std::fs::read(chain)?;
                    let chain = X509::stack_from_pem(&pem)?;
                    for cert in chain {
                        ssl_connector.add_extra_chain_cert(cert)?;
                    }
                }
                (Some(cert), None) => {
                    // treat cert file like it's a chain for convenience
                    ssl_connector.set_certificate_chain_file(cert)?;
                }
                (None, Some(chain)) => {
                    // load all certs from chain
                    ssl_connector.set_certificate_chain_file(chain)?;
                }
                (None, None) => unreachable!(),
            }
        }

        let ssl_connector = ssl_connector.build();

        Ok(Connector {
            inner: ConnectorImpl::TlsTcp(TlsTcpConnector {
                inner: ssl_connector,
                verify_hostname: tls_config.verify_hostname(),
                use_sni: tls_config.use_sni(),
            }),
        })
    }

    pub async fn connect(&self, addr: &str) -> Result<Stream> {
        match &self.inner {
            ConnectorImpl::Tcp => Ok(Stream {
                inner: StreamImpl::Tcp(tokio::net::TcpStream::connect(addr).await?),
            }),
            ConnectorImpl::TlsTcp(connector) => {
                let stream = tokio::net::TcpStream::connect(addr).await?;
                let domain = addr.split(':').next().unwrap().to_owned();

                let config = connector
                    .inner
                    .configure()?
                    .verify_hostname(connector.verify_hostname)
                    .use_server_name_indication(connector.use_sni);

                match tokio_boring::connect(config, &domain, stream).await {
                    Ok(stream) => Ok(Stream {
                        inner: StreamImpl::TlsTcp(stream),
                    }),
                    Err(e) => match e.as_io_error() {
                        Some(e) => Err(Error::new(e.kind(), e.to_string())),
                        None => Err(Error::new(ErrorKind::Other, e.to_string())),
                    },
                }
            }
        }
    }
}

enum ConnectorImpl {
    Tcp,
    TlsTcp(TlsTcpConnector),
}

pub struct TlsTcpConnector {
    inner: boring::ssl::SslConnector,
    verify_hostname: bool,
    use_sni: bool,
}

pub struct Stream {
    inner: StreamImpl,
}

enum StreamImpl {
    Tcp(tokio::net::TcpStream),
    TlsTcp(tokio_boring::SslStream<tokio::net::TcpStream>),
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            StreamImpl::TlsTcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
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
            StreamImpl::TlsTcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_flush(cx),
            StreamImpl::TlsTcp(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            StreamImpl::TlsTcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}
