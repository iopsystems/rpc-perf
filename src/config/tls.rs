use super::*;

fn default_true() -> bool {
    true
}

#[derive(Clone, Deserialize)]
pub struct Tls {
    private_key: Option<String>,
    private_key_password: Option<String>,
    certificate: Option<String>,
    certificate_chain: Option<String>,
    ca_file: Option<String>,
    #[serde(default = "default_true")]
    verify_hostname: bool,
    #[serde(default = "default_true")]
    use_sni: bool,
}

impl Tls {
    pub fn private_key(&self) -> Option<&str> {
        self.private_key.as_deref()
    }

    pub fn private_key_password(&self) -> Option<&str> {
        self.private_key_password.as_deref()
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

    pub fn verify_hostname(&self) -> bool {
        self.verify_hostname
    }

    pub fn use_sni(&self) -> bool {
        self.use_sni
    }
}
