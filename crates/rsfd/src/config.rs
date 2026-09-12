use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::daemon::{DaemonConfig, OperatorProfile};

pub const ENV_CONFIG: &str = "RSFD_CONFIG";
pub const ENV_STATE: &str = "RSFD_STATE";
pub const ENV_ONCE: &str = "RSFD_ONCE";
pub const ENV_TICK_MS: &str = "RSFD_TICK_MS";
pub const ENV_LISTEN: &str = "RSFD_LISTEN";
pub const ENV_DATA_LISTEN: &str = "RSFD_DATA_LISTEN";
pub const ENV_PROFILE: &str = "RSFD_PROFILE";
pub const ENV_MAX_CONSECUTIVE_ERRORS: &str = "RSFD_MAX_CONSECUTIVE_ERRORS";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ServeConfigOverrides {
    pub config: Option<PathBuf>,
    pub state: Option<PathBuf>,
    pub once: Option<bool>,
    pub tick_ms: Option<u64>,
    pub listen: Option<SocketAddr>,
    pub data_listen: Option<SocketAddr>,
    pub profile: Option<OperatorProfile>,
    pub max_consecutive_errors: Option<u32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ServeConfig {
    state: Option<PathBuf>,
    once: Option<bool>,
    tick_ms: Option<u64>,
    listen: Option<SocketAddr>,
    data_listen: Option<SocketAddr>,
    profile: Option<OperatorProfile>,
    max_consecutive_errors: Option<u32>,
}

pub fn resolve_daemon_config(
    cli: ServeConfigOverrides,
    env: impl IntoIterator<Item = (String, String)>,
) -> Result<DaemonConfig, String> {
    let env = env.into_iter().collect::<BTreeMap<_, _>>();
    let config_path = cli
        .config
        .clone()
        .or_else(|| env.get(ENV_CONFIG).map(PathBuf::from));
    let mut resolved = if let Some(path) = config_path {
        load_config_file(&path)?
    } else {
        ServeConfig::default()
    };
    apply_env(&mut resolved, &env)?;
    apply_cli(&mut resolved, cli);
    let state = resolved
        .state
        .ok_or_else(|| "serve requires --state, RSFD_STATE, or state in config file".to_string())?;
    let tick_ms = resolved.tick_ms.unwrap_or(1_000);
    if tick_ms == 0 {
        return Err("daemon tick interval must be greater than zero".to_string());
    }
    Ok(DaemonConfig {
        state,
        once: resolved.once.unwrap_or(false),
        tick: Duration::from_millis(tick_ms),
        listen: resolved.listen,
        data_listen: resolved.data_listen,
        profile: resolved.profile.unwrap_or_default(),
        max_consecutive_errors: resolved.max_consecutive_errors.unwrap_or(3),
    })
}

fn load_config_file(path: &Path) -> Result<ServeConfig, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|err| format!("failed to read config {}: {err}", path.display()))?;
    parse_config(&raw)
}

fn parse_config(raw: &str) -> Result<ServeConfig, String> {
    let mut config = ServeConfig::default();
    for (line_no, line) in raw.lines().enumerate() {
        let line = line
            .split_once('#')
            .map_or(line, |(before, _)| before)
            .trim();
        if line.is_empty() {
            continue;
        }
        let (key, value) = line.split_once('=').ok_or_else(|| {
            format!(
                "config line {} must use key = value syntax",
                line_no.saturating_add(1)
            )
        })?;
        apply_pair(&mut config, key.trim(), value.trim(), "config")?;
    }
    Ok(config)
}

fn apply_env(config: &mut ServeConfig, env: &BTreeMap<String, String>) -> Result<(), String> {
    if let Some(value) = env.get(ENV_STATE) {
        config.state = Some(PathBuf::from(value));
    }
    if let Some(value) = env.get(ENV_ONCE) {
        config.once = Some(parse_bool(value, ENV_ONCE)?);
    }
    if let Some(value) = env.get(ENV_TICK_MS) {
        config.tick_ms = Some(parse_u64(value, ENV_TICK_MS)?);
    }
    if let Some(value) = env.get(ENV_LISTEN) {
        config.listen = Some(parse_socket_addr(value, ENV_LISTEN)?);
    }
    if let Some(value) = env.get(ENV_DATA_LISTEN) {
        config.data_listen = Some(parse_socket_addr(value, ENV_DATA_LISTEN)?);
    }
    if let Some(value) = env.get(ENV_PROFILE) {
        config.profile = Some(OperatorProfile::parse(value)?);
    }
    if let Some(value) = env.get(ENV_MAX_CONSECUTIVE_ERRORS) {
        config.max_consecutive_errors =
            Some(parse_u32_positive(value, ENV_MAX_CONSECUTIVE_ERRORS)?);
    }
    Ok(())
}

fn apply_cli(config: &mut ServeConfig, cli: ServeConfigOverrides) {
    if let Some(state) = cli.state {
        config.state = Some(state);
    }
    if let Some(once) = cli.once {
        config.once = Some(once);
    }
    if let Some(tick_ms) = cli.tick_ms {
        config.tick_ms = Some(tick_ms);
    }
    if let Some(listen) = cli.listen {
        config.listen = Some(listen);
    }
    if let Some(data_listen) = cli.data_listen {
        config.data_listen = Some(data_listen);
    }
    if let Some(profile) = cli.profile {
        config.profile = Some(profile);
    }
    if let Some(max_consecutive_errors) = cli.max_consecutive_errors {
        config.max_consecutive_errors = Some(max_consecutive_errors);
    }
}

fn apply_pair(
    config: &mut ServeConfig,
    key: &str,
    value: &str,
    source: &str,
) -> Result<(), String> {
    match key {
        "state" => config.state = Some(PathBuf::from(unquote(value))),
        "once" => config.once = Some(parse_bool(unquote(value), "once")?),
        "tick_ms" => config.tick_ms = Some(parse_u64(unquote(value), "tick_ms")?),
        "listen" => config.listen = Some(parse_socket_addr(unquote(value), "listen")?),
        "data_listen" => {
            config.data_listen = Some(parse_socket_addr(unquote(value), "data_listen")?)
        }
        "profile" => config.profile = Some(OperatorProfile::parse(unquote(value))?),
        "max_consecutive_errors" => {
            config.max_consecutive_errors = Some(parse_u32_positive(
                unquote(value),
                "max_consecutive_errors",
            )?)
        }
        "" => return Err(format!("{source} contains an empty key")),
        other => return Err(format!("{source} contains unknown key {other}")),
    }
    Ok(())
}

fn unquote(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
}

fn parse_bool(value: &str, name: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("{name} must be true or false")),
    }
}

fn parse_u64(value: &str, name: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a non-negative integer"))
}

fn parse_u32_positive(value: &str, name: &str) -> Result<u32, String> {
    let parsed = value
        .parse::<u32>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be greater than zero"));
    }
    Ok(parsed)
}

fn parse_socket_addr(value: &str, name: &str) -> Result<SocketAddr, String> {
    value
        .parse::<SocketAddr>()
        .map_err(|_| format!("{name} must be an IP socket address like 127.0.0.1:8080"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn config_file_env_and_cli_apply_in_precedence_order() {
        let path = test_path("config_file_env_and_cli_apply_in_precedence_order");
        std::fs::write(
            &path,
            format!(
                "state = \"{}\"\nonce = false\ntick_ms = 10\n",
                path.parent().expect("parent").display()
            ),
        )
        .expect("write config");

        let config = resolve_daemon_config(
            ServeConfigOverrides {
                config: Some(path.clone()),
                state: Some(PathBuf::from("/cli/state")),
                once: Some(true),
                tick_ms: None,
                listen: None,
                data_listen: None,
                profile: None,
                max_consecutive_errors: None,
            },
            [
                (ENV_STATE.to_string(), "/env/state".to_string()),
                (ENV_TICK_MS.to_string(), "25".to_string()),
            ],
        )
        .expect("resolve config");

        assert_eq!(config.state, PathBuf::from("/cli/state"));
        assert!(config.once);
        assert_eq!(config.tick, Duration::from_millis(25));
        assert_eq!(config.listen, None);
        assert_eq!(config.data_listen, None);
        assert_eq!(config.profile, OperatorProfile::Standalone);
        assert_eq!(config.max_consecutive_errors, 3);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn config_file_rejects_unknown_keys() {
        let path = test_path("config_file_rejects_unknown_keys");
        std::fs::write(&path, "state = \"/tmp/rsf\"\nmagic = true\n").expect("write config");

        let err = resolve_daemon_config(
            ServeConfigOverrides {
                config: Some(path.clone()),
                ..ServeConfigOverrides::default()
            },
            [],
        )
        .expect_err("unknown key rejected");

        assert!(err.contains("unknown key magic"));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn missing_state_is_rejected_after_overrides() {
        let err = resolve_daemon_config(ServeConfigOverrides::default(), [])
            .expect_err("missing state rejected");

        assert!(err.contains("requires --state"));
    }

    #[test]
    fn zero_tick_is_rejected() {
        let err = resolve_daemon_config(
            ServeConfigOverrides {
                state: Some(PathBuf::from("/tmp/rsf")),
                tick_ms: Some(0),
                ..ServeConfigOverrides::default()
            },
            [],
        )
        .expect_err("zero tick rejected");

        assert!(err.contains("greater than zero"));
    }

    #[test]
    fn listener_address_resolves_from_config_env_and_cli() {
        let path = test_path("listener_address_resolves_from_config_env_and_cli");
        std::fs::write(
            &path,
            "state = \"/config/state\"\nonce = true\nlisten = \"127.0.0.1:7100\"\ndata_listen = \"127.0.0.1:8100\"\n",
        )
        .expect("write config");

        let config = resolve_daemon_config(
            ServeConfigOverrides {
                config: Some(path.clone()),
                listen: Some("127.0.0.1:7300".parse().expect("cli addr")),
                data_listen: Some("127.0.0.1:8300".parse().expect("cli data addr")),
                ..ServeConfigOverrides::default()
            },
            [
                (ENV_STATE.to_string(), "/env/state".to_string()),
                (ENV_LISTEN.to_string(), "127.0.0.1:7200".to_string()),
                (ENV_DATA_LISTEN.to_string(), "127.0.0.1:8200".to_string()),
            ],
        )
        .expect("resolve listener");

        assert_eq!(config.state, PathBuf::from("/env/state"));
        assert_eq!(
            config.listen,
            Some("127.0.0.1:7300".parse().expect("resolved addr"))
        );
        assert_eq!(
            config.data_listen,
            Some("127.0.0.1:8300".parse().expect("resolved data addr"))
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn profile_resolves_from_config_env_and_cli() {
        let path = test_path("profile_resolves_from_config_env_and_cli");
        std::fs::write(
            &path,
            "state = \"/config/state\"\nprofile = \"standalone\"\n",
        )
        .expect("write config");

        let config = resolve_daemon_config(
            ServeConfigOverrides {
                config: Some(path.clone()),
                profile: Some(OperatorProfile::MultiRegion),
                ..ServeConfigOverrides::default()
            },
            [
                (ENV_STATE.to_string(), "/env/state".to_string()),
                (ENV_PROFILE.to_string(), "regional".to_string()),
            ],
        )
        .expect("resolve profile");

        assert_eq!(config.state, PathBuf::from("/env/state"));
        assert_eq!(config.profile, OperatorProfile::MultiRegion);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn invalid_profile_is_rejected() {
        let err = resolve_daemon_config(
            ServeConfigOverrides {
                state: Some(PathBuf::from("/tmp/rsf")),
                ..ServeConfigOverrides::default()
            },
            [(ENV_PROFILE.to_string(), "globalish".to_string())],
        )
        .expect_err("invalid profile rejected");

        assert!(err.contains("operator profile"));
    }

    #[test]
    fn supervision_error_limit_resolves_from_config_env_and_cli() {
        let path = test_path("supervision_error_limit_resolves_from_config_env_and_cli");
        std::fs::write(
            &path,
            "state = \"/config/state\"\nmax_consecutive_errors = 4\n",
        )
        .expect("write config");

        let config = resolve_daemon_config(
            ServeConfigOverrides {
                config: Some(path.clone()),
                max_consecutive_errors: Some(6),
                ..ServeConfigOverrides::default()
            },
            [
                (ENV_STATE.to_string(), "/env/state".to_string()),
                (ENV_MAX_CONSECUTIVE_ERRORS.to_string(), "5".to_string()),
            ],
        )
        .expect("resolve supervision");

        assert_eq!(config.state, PathBuf::from("/env/state"));
        assert_eq!(config.max_consecutive_errors, 6);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn zero_supervision_error_limit_is_rejected() {
        let err = resolve_daemon_config(
            ServeConfigOverrides {
                state: Some(PathBuf::from("/tmp/rsf")),
                ..ServeConfigOverrides::default()
            },
            [(ENV_MAX_CONSECUTIVE_ERRORS.to_string(), "0".to_string())],
        )
        .expect_err("zero supervision limit rejected");

        assert!(err.contains("greater than zero"));
    }

    #[test]
    fn invalid_listener_address_is_rejected() {
        let err = resolve_daemon_config(
            ServeConfigOverrides {
                state: Some(PathBuf::from("/tmp/rsf")),
                listen: None,
                ..ServeConfigOverrides::default()
            },
            [(ENV_LISTEN.to_string(), "localhost:8080".to_string())],
        )
        .expect_err("dns listener names are rejected");

        assert!(err.contains("IP socket address"));
    }

    fn test_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsfd-{name}-{}-{nonce}.conf", std::process::id()))
    }
}
