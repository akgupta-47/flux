use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProxySettings {
    pub buffer_size: Option<usize>,
    pub max_connections: Option<usize>,
    pub keepalive_secs: Option<u64>,
    pub idle_timeout_secs: Option<u64>,
    pub metrics: Option<String>,
    pub send_buffer_size: Option<usize>,
    pub recv_buffer_size: Option<usize>,
}

pub async fn load_proxy_settings(path: &str) -> Result<ProxySettings> {
    let contents = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("read config file {path}"))?;
    serde_yaml::from_str(&contents).with_context(|| format!("parse config file {path}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_proxy_resource_settings_and_rejects_unknown_keys() {
        let cfg: ProxySettings =
            serde_yaml::from_str("buffer_size: 65536\nmax_connections: 50\nkeepalive_secs: 30\n")
                .unwrap();
        assert_eq!(cfg.buffer_size, Some(65536));
        assert_eq!(cfg.max_connections, Some(50));
        assert!(serde_yaml::from_str::<ProxySettings>("unbounded: true\n").is_err());
    }
}
