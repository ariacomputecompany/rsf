//! Durable target identity is published before creating any generation data.
use super::*;
const MAGIC: &[u8] = b"VSRLP1";
pub(super) const NAME: &str = "VSR_PROMOTION";
pub(super) const TEMPORARY: &str = ".VSR_PROMOTION.tmp";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Identity {
    pub pg: PlacementGroupId,
    pub local: NodeId,
    pub plan: [u8; 32],
    pub bootstrap: [u8; 32],
}
impl Identity {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&self.pg.0.to_le_bytes());
        bytes.extend_from_slice(&self.local.0.to_le_bytes());
        bytes.extend_from_slice(&self.plan);
        bytes.extend_from_slice(&self.bootstrap);
        bytes.extend_from_slice(Digest::blake3_bytes(&bytes).as_bytes());
        bytes
    }
    pub fn digest(&self) -> [u8; 32] {
        Digest::blake3_bytes(&self.encode()).bytes
    }
    pub fn read(root: &Path) -> RsfResult<Option<Self>> {
        Self::read_with(root, &mut publication::read_fixed_file)
    }
    pub(super) fn read_with(
        root: &Path,
        read: &mut impl FnMut(&Path, usize) -> std::io::Result<Vec<u8>>,
    ) -> RsfResult<Option<Self>> {
        let path = root.join(NAME);
        match fs::symlink_metadata(&path) {
            Ok(meta) if meta.is_file() && !meta.file_type().is_symlink() => {}
            Ok(_) => return Err(corrupt("promotion identity is not a regular owned file")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error(e)),
        }
        let bytes = read(&path, 118).map_err(io_error)?;
        if bytes.len() != 118
            || !bytes.starts_with(MAGIC)
            || bytes[86..] != Digest::blake3_bytes(&bytes[..86]).bytes
        {
            return Err(corrupt("invalid learner promotion identity"));
        }
        Ok(Some(Self {
            pg: PlacementGroupId(u64::from_le_bytes(bytes[6..14].try_into().unwrap())),
            local: NodeId(u64::from_le_bytes(bytes[14..22].try_into().unwrap())),
            plan: bytes[22..54].try_into().unwrap(),
            bootstrap: bytes[54..86].try_into().unwrap(),
        }))
    }
    pub fn claim(&self, root: &Path) -> RsfResult<()> {
        if let Some(existing) = Self::read(root)? {
            return if existing == *self {
                Ok(())
            } else {
                Err(invalid_input(
                    "promotion target belongs to another node, bootstrap or plan",
                ))
            };
        }
        for entry in fs::read_dir(root).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            if entry.file_type().map_err(io_error)?.is_symlink()
                || (entry.file_name() != ".vsr-writer.lock" && entry.file_name() != TEMPORARY)
            {
                return Err(invalid_input("unowned nonempty promotion target"));
            }
        }
        // A torn provisional temporary file has never acknowledged ownership.
        // No generation or other content can exist before the durable claim.
        atomic_write(root, NAME, &self.encode())
    }
}
