use crate::error;
use crate::{Config, Tls};

use std::io::Result;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub enum SslProvider {
    #[cfg(feature = "boringssl")]
    Boringssl,
    #[cfg(feature = "openssl")]
    Openssl,
    Unknown,
}

// clippy doesn't realize that the default changes depending on enabled features
// so we suppress this warning
#[allow(clippy::derivable_impls)]
#[allow(unreachable_code)]
impl Default for SslProvider {
    fn default() -> Self {
        #[cfg(feature = "boringssl")]
        {
            return SslProvider::Boringssl;
        }

        #[cfg(feature = "openssl")]
        {
            return SslProvider::Openssl;
        }

        SslProvider::Unknown
    }
}

pub struct Connector {
    inner: ConnectorImpl,
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
        match SslProvider::default() {
            #[cfg(feature = "boringssl")]
            SslProvider::Boringssl => Self::boringssl(tls_config),
            #[cfg(feature = "openssl")]
            SslProvider::Openssl => Self::openssl(tls_config),
            SslProvider::Unknown => {
                error!("no TLS/SSL provider could be found. Check that rpc-perf was built with either boringssl or openssl support");
                std::process::exit(1);
            }
        }
    }

    #[cfg(feature = "boringssl")]
    fn boringssl(tls_config: &Tls) -> Result<Self> {
        let private_key = tls_config.private_key();
        let certificate = tls_config.certificate();
        let certificate_chain = tls_config.certificate_chain();

        let mut ssl_connector =
            boring::ssl::SslConnector::builder(boring::ssl::SslMethod::tls_client())?;

        if let Some(ca_file) = tls_config.ca_file() {
            ssl_connector.set_ca_file(ca_file)?;
        }

        // mTLS configuration
        if private_key.is_some() && (certificate.is_some() || certificate_chain.is_some()) {
            ssl_connector
                .set_private_key_file(private_key.unwrap(), boring::ssl::SslFiletype::PEM)?;

            match (certificate, certificate_chain) {
                (Some(cert), Some(chain)) => {
                    // assume cert is just a leaf and that we need to append the
                    // certs in the chain file after loading the leaf cert

                    ssl_connector.set_certificate_file(cert, boring::ssl::SslFiletype::PEM)?;
                    let pem = std::fs::read(chain)?;
                    let chain = boring::x509::X509::stack_from_pem(&pem)?;
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
            inner: ConnectorImpl::BoringsslTlsTcp(BoringsslTlsTcp {
                inner: ssl_connector,
                verify_hostname: tls_config.verify_hostname(),
                use_sni: tls_config.use_sni(),
            }),
        })
    }

    #[cfg(feature = "openssl")]
    fn openssl(tls_config: &Tls) -> Result<Self> {
        let private_key = tls_config.private_key();
        let certificate = tls_config.certificate();
        let certificate_chain = tls_config.certificate_chain();

        let mut ssl_connector =
            openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls_client())?;

        if let Some(ca_file) = tls_config.ca_file() {
            ssl_connector.set_ca_file(ca_file)?;
        }

        // mTLS configuration
        if let Some(key) = private_key {
            if certificate.is_some() || certificate_chain.is_some() {
                ssl_connector.set_private_key_file(key, openssl::ssl::SslFiletype::PEM)?;

                match (certificate, certificate_chain) {
                    (Some(cert), Some(chain)) => {
                        // assume cert is just a leaf and that we need to append the
                        // certs in the chain file after loading the leaf cert

                        ssl_connector.set_certificate_file(cert, openssl::ssl::SslFiletype::PEM)?;
                        let pem = std::fs::read(chain)?;
                        let chain = openssl::x509::X509::stack_from_pem(&pem)?;
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
        }

        let ssl_connector = ssl_connector.build();

        Ok(Connector {
            inner: ConnectorImpl::OpensslTlsTcp(OpensslTlsTcp {
                inner: ssl_connector,
                verify_hostname: tls_config.verify_hostname(),
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
            #[cfg(feature = "boringssl")]
            ConnectorImpl::BoringsslTlsTcp(connector) => {
                let stream = tokio::net::TcpStream::connect(addr).await?;
                let domain = addr.split(':').next().unwrap().to_owned();

                let config = connector
                    .inner
                    .configure()?
                    .verify_hostname(connector.verify_hostname)
                    .use_server_name_indication(connector.use_sni);

                match tokio_boring::connect(config, &domain, stream).await {
                    Ok(stream) => Ok(Stream {
                        inner: StreamImpl::BoringsslTlsTcp(stream),
                    }),
                    Err(e) => match e.as_io_error() {
                        Some(e) => Err(std::io::Error::new(e.kind(), e.to_string())),
                        None => Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e.to_string(),
                        )),
                    },
                }
            }
            #[cfg(feature = "openssl")]
            ConnectorImpl::OpensslTlsTcp(connector) => {
                let stream = tokio::net::TcpStream::connect(addr).await?;
                let domain = addr.split(':').next().unwrap().to_owned();

                let config = connector
                    .inner
                    .configure()?
                    .verify_hostname(connector.verify_hostname)
                    .use_server_name_indication(connector.use_sni);

                let mut ssl = tokio_openssl::SslStream::new(config.into_ssl(&domain)?, stream)?;

                match tokio_openssl::SslStream::connect(std::pin::Pin::new(&mut ssl)).await {
                    Ok(_) => Ok(Stream {
                        inner: StreamImpl::OpensslTlsTcp(ssl),
                    }),
                    Err(e) => match e.io_error() {
                        Some(e) => Err(std::io::Error::new(e.kind(), e.to_string())),
                        None => Err(std::io::Error::other(e.to_string())),
                    },
                }
            }
        }
    }
}

enum ConnectorImpl {
    Tcp,
    #[cfg(feature = "boringssl")]
    BoringsslTlsTcp(BoringsslTlsTcp),
    #[cfg(feature = "openssl")]
    OpensslTlsTcp(OpensslTlsTcp),
}

#[cfg(feature = "boringssl")]
pub struct BoringsslTlsTcp {
    inner: boring::ssl::SslConnector,
    verify_hostname: bool,
    use_sni: bool,
}

#[cfg(feature = "openssl")]
pub struct OpensslTlsTcp {
    inner: openssl::ssl::SslConnector,
    verify_hostname: bool,
    use_sni: bool,
}

pub struct Stream {
    inner: StreamImpl,
}

enum StreamImpl {
    Tcp(tokio::net::TcpStream),
    #[cfg(feature = "boringssl")]
    BoringsslTlsTcp(tokio_boring::SslStream<tokio::net::TcpStream>),
    #[cfg(feature = "openssl")]
    OpensslTlsTcp(tokio_openssl::SslStream<tokio::net::TcpStream>),
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "boringssl")]
            StreamImpl::BoringsslTlsTcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "openssl")]
            StreamImpl::OpensslTlsTcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
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
            #[cfg(feature = "boringssl")]
            StreamImpl::BoringsslTlsTcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "openssl")]
            StreamImpl::OpensslTlsTcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_flush(cx),
            #[cfg(feature = "boringssl")]
            StreamImpl::BoringsslTlsTcp(s) => std::pin::Pin::new(s).poll_flush(cx),
            #[cfg(feature = "openssl")]
            StreamImpl::OpensslTlsTcp(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "boringssl")]
            StreamImpl::BoringsslTlsTcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "openssl")]
            StreamImpl::OpensslTlsTcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
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
            #[cfg(feature = "boringssl")]
            StreamImpl::BoringsslTlsTcp(s) => {
                let mut buf = ReadBuf::uninit(unsafe { rbc.as_mut() });
                std::pin::Pin::new(s).poll_read(cx, &mut buf)
            }
            #[cfg(feature = "openssl")]
            StreamImpl::OpensslTlsTcp(s) => {
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
            #[cfg(feature = "boringssl")]
            StreamImpl::BoringsslTlsTcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "openssl")]
            StreamImpl::OpensslTlsTcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_flush(cx),
            #[cfg(feature = "boringssl")]
            StreamImpl::BoringsslTlsTcp(s) => std::pin::Pin::new(s).poll_flush(cx),
            #[cfg(feature = "openssl")]
            StreamImpl::OpensslTlsTcp(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "boringssl")]
            StreamImpl::BoringsslTlsTcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "openssl")]
            StreamImpl::OpensslTlsTcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}
