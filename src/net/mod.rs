use tokio::io::AsyncWrite;
use tokio::io::AsyncRead;
use tokio::net::ToSocketAddrs;
use crate::Config;

pub struct Connector {
    inner: ConnectorImpl,
}

impl Connector {
    pub fn new(config: &Config) -> Self {
        let private_key = config.tls().private_key();
        let certificate = config.tls().certificate();
        let certificate_chain = config.tls().certificate_chain();
        let _ca_file = config.tls().ca_file();

        if private_key.is_some() && (certificate.is_some() || certificate_chain.is_some()) {
            eprintln!("TLS is not implemented.");
            Connector {
                inner: ConnectorImpl::Tcp,
            }
        } else {
            Connector {
                inner: ConnectorImpl::Tcp,
            }
        }
    }

    pub async fn connect<A: ToSocketAddrs>(&self, addr: A) -> Result<Stream, std::io::Error> {
        Ok(Stream {
            inner: StreamImpl::Tcp(tokio::net::TcpStream::connect(addr).await?)
        })
    } 
}

enum ConnectorImpl {
    Tcp,
}

pub struct Stream {
    inner: StreamImpl,
}

impl Stream {
    pub async fn writable(&self) -> Result<(), std::io::Error> {
        match &self.inner {
            StreamImpl::Tcp(s) => s.writable().await,
            _ => unimplemented!(),
        }
    }

    pub fn try_write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        match &self.inner {
            StreamImpl::Tcp(s) => s.try_write(buf),
            _ => unimplemented!(),
        }
    }
}

enum StreamImpl {
    Tcp(tokio::net::TcpStream),
    TlsTcp(tokio_boring::SslStream<tokio::net::TcpStream>),
}

impl AsyncRead for Stream {
    fn poll_read(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &mut tokio::io::ReadBuf<'_>) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => {
                std::pin::Pin::new(s).poll_read(cx, buf)
            }
            _ => {
                unimplemented!()
            }
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &[u8]) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => {
                std::pin::Pin::new(s).poll_write(cx, buf)
            }
            _ => {
                unimplemented!()
            }
        }
    }
    fn poll_flush(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => {
                std::pin::Pin::new(s).poll_flush(cx)
            }
            _ => {
                unimplemented!()
            }
        }
    }
    fn poll_shutdown(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut self.inner {
            StreamImpl::Tcp(s) => {
                std::pin::Pin::new(s).poll_shutdown(cx)
            }
            _ => {
                unimplemented!()
            }
        }
    }
}