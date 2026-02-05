use super::*;

#[derive(Clone, Deserialize)]
pub struct Target {
    /// A list of target endpoints (`IP:PORT`).
    endpoints: Vec<String>,
    /// A cache name
    cache_name: Option<String>,
    object_store_name: Option<String>,
    /// Manual endpoint override for protosocket clients
    endpoint_override: Option<String>,
    /// Configure protosocket clients to use private endpoints
    use_private_endpoints: Option<bool>,
}

impl Target {
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    pub fn cache_name(&self) -> Option<&str> {
        self.cache_name.as_deref()
    }

    pub fn object_store_name(&self) -> Option<&str> {
        self.object_store_name.as_deref()
    }

    pub fn endpoint_override(&self) -> Option<&str> {
        self.endpoint_override.as_deref()
    }

    pub fn use_private_endpoints(&self) -> Option<bool> {
        self.use_private_endpoints
    }
}
