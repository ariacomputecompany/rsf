//! Peer identity also retains monotonic recovery-only client admission policy.
//! This is not a membership completion record or a voting authority proof.
use super::*;

const MAX_GROUPS: usize = 4096;
const MAX_IDENTITY_BYTES: u64 = 128 * 1024;

// Typed decoding rejects duplicate keys as well as unknown fields. An explicit
// null managed set is invalid; only an absent field denotes a V1 identity.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredIdentity {
    version: u64,
    cluster: String,
    node: u64,
    #[serde(default, deserialize_with = "managed_groups")]
    managed_membership: Option<Vec<u64>>,
}

fn managed_groups<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<u64>>, D::Error> {
    Vec::<u64>::deserialize(deserializer).map(Some)
}

pub(super) struct StateIdentity {
    cluster: String,
    node: NodeId,
    pub managed: BTreeSet<PlacementGroupId>,
}

impl StateIdentity {
    pub fn bind(root: &Path, cluster: &ClusterId, node: NodeId) -> Result<Self, String> {
        let path = root.join("IDENTITY.json");
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if !metadata.is_file() || metadata.len() > MAX_IDENTITY_BYTES {
                    return Err("peer state identity is not a bounded regular file".into());
                }
                let bytes = std::fs::read(&path)
                    .map_err(|error| format!("cannot read peer state identity: {error}"))?;
                let stored = Self::decode(&bytes)?;
                if stored.cluster != cluster.as_str() || stored.node != node {
                    return Err("peer state belongs to a different cluster or node".into());
                }
                Ok(stored)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                for entry in std::fs::read_dir(root).map_err(|error| error.to_string())? {
                    let name = entry.map_err(|error| error.to_string())?.file_name();
                    if name != ".peer.lock" && name != "IDENTITY.pending" {
                        return Err(
                            "unbound peer state contains existing data; migrate it explicitly"
                                .into(),
                        );
                    }
                }
                let identity = Self {
                    cluster: cluster.as_str().to_owned(),
                    node,
                    managed: BTreeSet::new(),
                };
                identity.persist(root)?;
                Ok(identity)
            }
            Err(error) => Err(format!("cannot inspect peer state identity: {error}")),
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        let value: StoredIdentity =
            serde_json::from_slice(bytes).map_err(|_| "peer state identity is corrupt")?;
        ClusterId::new(&value.cluster).map_err(|error| error.message)?;
        let mut managed = BTreeSet::new();
        match (value.version, value.managed_membership) {
            (1, None) => {}
            (2, Some(groups)) if !groups.is_empty() && groups.len() <= MAX_GROUPS => {
                let mut previous = None;
                for group in groups {
                    if previous.is_some_and(|previous| previous >= group) {
                        return Err("managed peer groups must be unique and sorted".into());
                    }
                    managed.insert(PlacementGroupId(group));
                    previous = Some(group);
                }
            }
            _ => return Err("invalid peer identity schema".into()),
        }
        Ok(Self {
            cluster: value.cluster,
            node: NodeId(value.node),
            managed,
        })
    }

    /// Call only after every newly enrolled group's existing transition custody
    /// and recovery-only RPC policy have been validated, before listening.
    pub fn enroll(
        &mut self,
        root: &Path,
        configured: &BTreeSet<PlacementGroupId>,
    ) -> Result<(), String> {
        if !self.managed.is_subset(configured) || configured.len() > MAX_GROUPS {
            return Err("managed peer groups cannot be omitted or removed".into());
        }
        if self.managed == *configured {
            return Ok(());
        }
        let next = Self {
            cluster: self.cluster.clone(),
            node: self.node,
            managed: configured.clone(),
        };
        next.persist(root)?;
        *self = next;
        Ok(())
    }

    fn persist(&self, root: &Path) -> Result<(), String> {
        let mut value = serde_json::json!({
            "version": 1, "cluster": self.cluster, "node": self.node.0,
        });
        if !self.managed.is_empty() {
            value["version"] = 2.into();
            value["managed_membership"] = self
                .managed
                .iter()
                .map(|group| group.0)
                .collect::<Vec<_>>()
                .into();
        }
        let temporary = root.join("IDENTITY.pending");
        match std::fs::symlink_metadata(&temporary) {
            Ok(metadata) if metadata.is_file() => {
                std::fs::remove_file(&temporary).map_err(|error| error.to_string())?;
            }
            Ok(_) => return Err("peer identity temporary path is not a regular file".into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "cannot inspect peer identity temporary path: {error}"
                ));
            }
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| error.to_string())?;
        file.write_all(&serde_json::to_vec(&value).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        std::fs::rename(&temporary, root.join("IDENTITY.json"))
            .map_err(|error| error.to_string())?;
        File::open(root)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn root() -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "rsfd-managed-identity-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }
    #[test]
    fn managed_identity_is_monotonic_and_binding_never_downgrades_policy() {
        let root = root();
        let cluster = ClusterId::new("managed").unwrap();
        let mut identity = StateIdentity::bind(&root, &cluster, NodeId(2)).unwrap();
        let groups = [PlacementGroupId(1), PlacementGroupId(9)].into();
        identity.enroll(&root, &groups).unwrap();
        let before = std::fs::read(root.join("IDENTITY.json")).unwrap();
        bind_state(&root, &cluster, NodeId(2)).unwrap();
        let mut reopened = StateIdentity::bind(&root, &cluster, NodeId(2)).unwrap();
        assert_eq!(reopened.managed, groups);
        assert!(
            reopened
                .enroll(&root, &[PlacementGroupId(1)].into())
                .is_err()
        );
        assert_eq!(std::fs::read(root.join("IDENTITY.json")).unwrap(), before);
        assert!(StateIdentity::bind(&root, &cluster, NodeId(3)).is_err());
        reopened
            .enroll(
                &root,
                &[
                    PlacementGroupId(1),
                    PlacementGroupId(9),
                    PlacementGroupId(10),
                ]
                .into(),
            )
            .unwrap();
        assert_eq!(
            StateIdentity::bind(&root, &cluster, NodeId(2))
                .unwrap()
                .managed
                .len(),
            3
        );
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn managed_identity_rejects_duplicate_unknown_and_noncanonical_policy_fields() {
        for bytes in [
            r#"{"version":1,"version":2,"cluster":"managed","node":2,"managed_membership":[1]}"#,
            r#"{"version":2,"cluster":"managed","node":2,"managed_membership":[1],"managed_membership":[9]}"#,
            r#"{"version":1,"cluster":"managed","node":2,"managed_membership":null}"#,
            r#"{"version":2,"cluster":"managed","node":2,"managed_membership":[]}"#,
            r#"{"version":2,"cluster":"managed","node":2,"managed_membership":[1,1]}"#,
            r#"{"version":2,"cluster":"managed","node":2,"managed_membership":[9,1]}"#,
            r#"{"version":1,"cluster":"managed","node":2,"extra":0}"#,
            r#"{"version":2,"cluster":"managed","node":2}"#,
            r#"{"version":2,"cluster":"managed","node":2,"managed_membership":[1]"#,
        ] {
            assert!(StateIdentity::decode(bytes.as_bytes()).is_err(), "{bytes}");
        }
    }
    #[test]
    #[cfg(unix)]
    fn managed_identity_rejects_foreign_temporary_paths_without_touching_policy() {
        let root = root();
        let cluster = ClusterId::new("managed").unwrap();
        let mut identity = StateIdentity::bind(&root, &cluster, NodeId(2)).unwrap();
        let before = std::fs::read(root.join("IDENTITY.json")).unwrap();
        let foreign = root.join("foreign");
        std::fs::write(&foreign, b"not an identity temporary file").unwrap();
        std::os::unix::fs::symlink(&foreign, root.join("IDENTITY.pending")).unwrap();
        assert!(
            identity
                .enroll(&root, &[PlacementGroupId(1)].into())
                .is_err()
        );
        assert_eq!(
            std::fs::read(&foreign).unwrap(),
            b"not an identity temporary file"
        );
        assert_eq!(std::fs::read(root.join("IDENTITY.json")).unwrap(), before);
        assert!(identity.managed.is_empty());
        std::fs::remove_file(root.join("IDENTITY.pending")).unwrap();
        std::fs::create_dir(root.join("IDENTITY.pending")).unwrap();
        assert!(
            identity
                .enroll(&root, &[PlacementGroupId(1)].into())
                .is_err()
        );
        assert_eq!(std::fs::read(root.join("IDENTITY.json")).unwrap(), before);
        std::fs::remove_dir_all(root).unwrap();
    }
}
