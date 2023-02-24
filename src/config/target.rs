use super::*;

#[derive(Clone, Deserialize)]
pub struct Target {
    /// A list of target endpoints (`IP:PORT`).
    endpoints: Vec<String>,
}

impl Target {
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }
}
