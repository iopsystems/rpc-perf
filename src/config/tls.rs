use super::*;

#[derive(Clone, Deserialize)]
pub struct Tls {
    private_key: Option<String>,
    certificate: Option<String>,
    certificate_chain: Option<String>,
    ca_file: Option<String>,
}

impl Tls {
    pub fn private_key(&self) -> Option<&str> {
        self.private_key.as_deref()
    }

    pub fn certificate(&self) -> Option<&str> {
        self.certificate.as_deref()
    }

    pub fn certificate_chain(&self) -> Option<&str> {
        self.certificate_chain.as_deref()
    }

    pub fn ca_file(&self) -> Option<&str> {
        self.ca_file.as_deref()
    }
}
