use http::uri::Authority;

use std::io::Error;

#[derive(Clone)]
pub struct Queue<T> {
    tx: async_channel::Sender<T>,
    rx: async_channel::Receiver<T>,
}

impl<T> Queue<T> {
    pub fn new(size: usize) -> Self {
        let (tx, rx) = async_channel::bounded::<T>(size);

        Self { tx, rx }
    }

    pub async fn send(&self, item: T) -> std::result::Result<(), async_channel::SendError<T>> {
        self.tx.send(item).await
    }

    pub async fn recv(&self) -> std::result::Result<T, async_channel::RecvError> {
        self.rx.recv().await
    }
}

pub async fn resolve(uri: &str) -> Result<(std::net::SocketAddr, Authority), std::io::Error> {
    let uri = uri
        .parse::<http::Uri>()
        .map_err(|_| Error::other("failed to parse uri"))?;

    let auth = uri
        .authority()
        .ok_or(Error::other("uri has no authority"))?
        .clone();

    let port = auth.port_u16().unwrap_or(443);

    let addr = tokio::net::lookup_host((auth.host(), port))
        .await?
        .next()
        .ok_or(Error::other("dns found no addresses"))?;

    Ok((addr, auth))
}
