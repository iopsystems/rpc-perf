use super::*;

#[derive(Clone, Deserialize)]
pub struct Target {
    /// A list of target endpoints (`IP:PORT`).
    endpoints: Vec<String>,
    /// A cache name
    cache_name: Option<String>,
}

impl Target {
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    pub fn cache_name(&self) -> Option<&str> {
        self.cache_name.as_deref()
    }
}
