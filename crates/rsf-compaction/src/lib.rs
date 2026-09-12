#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use rsf_core::{Bytes, CommitVersion, Digest, ErrorCode, RsfError, RsfResult};
use rsf_range::{CompactionPlan, SstableManifest};

mod ledger;

pub use ledger::{
    CompactionOperation, CompactionOperationId, CompactionOperationState, DurableCompactionLedger,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionRun {
    pub manifest: SstableManifest,
    pub entries: Vec<CompactionEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionEntry {
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub version: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionOutput {
    pub manifest: SstableManifest,
    pub live_entries: Vec<CompactionEntry>,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub reclaimed_bytes: u64,
    pub tombstones: u64,
}

#[derive(Clone, Debug)]
pub struct CompactionExecutor {
    pub gc_before: CommitVersion,
}

impl CompactionExecutor {
    pub fn compact(
        &self,
        plan: &CompactionPlan,
        runs: &[CompactionRun],
    ) -> RsfResult<CompactionOutput> {
        if plan.inputs.len() < 2 {
            return Err(invalid("compaction requires at least two planned inputs"));
        }
        if runs.len() != plan.inputs.len() {
            return Err(invalid(format!(
                "compaction expected {} runs, got {}",
                plan.inputs.len(),
                runs.len()
            )));
        }

        let mut input_bytes = 0;
        let mut latest = BTreeMap::<Bytes, CompactionEntry>::new();
        let mut tombstones = 0;

        for manifest in &plan.inputs {
            let run = runs
                .iter()
                .find(|run| run.manifest == *manifest)
                .ok_or_else(|| invalid("missing compaction run for planned manifest"))?;
            validate_run(manifest, run)?;
            input_bytes += manifest.bytes;
            for entry in &run.entries {
                if entry.value.is_none() {
                    tombstones += 1;
                }
                latest
                    .entry(entry.key.clone())
                    .and_modify(|known| {
                        if known.version < entry.version {
                            *known = entry.clone();
                        }
                    })
                    .or_insert_with(|| entry.clone());
            }
        }

        let live_entries = latest
            .into_values()
            .filter(|entry| entry.value.is_some() || entry.version > self.gc_before)
            .filter(|entry| entry.value.is_some())
            .collect::<Vec<_>>();
        if live_entries.is_empty() {
            return Err(invalid(
                "compaction output must retain at least one live entry",
            ));
        }

        let output_bytes = live_entries
            .iter()
            .map(|entry| {
                entry.key.len() as u64 + entry.value.as_ref().map_or(0, |value| value.len() as u64)
            })
            .sum::<u64>()
            .max(1);
        let first_key = live_entries
            .first()
            .expect("live entries already checked")
            .key
            .clone();
        let last_key = live_entries
            .last()
            .expect("live entries already checked")
            .key
            .clone();
        let max_version = live_entries
            .iter()
            .map(|entry| entry.version)
            .max()
            .expect("live entries already checked");
        let manifest = SstableManifest {
            range: plan.range.clone(),
            level: plan.output_level,
            first_key,
            last_key,
            version: max_version,
            bytes: output_bytes,
            checksum: checksum(&live_entries),
        };

        Ok(CompactionOutput {
            manifest,
            live_entries,
            input_bytes,
            output_bytes,
            reclaimed_bytes: input_bytes.saturating_sub(output_bytes),
            tombstones,
        })
    }
}

fn validate_run(manifest: &SstableManifest, run: &CompactionRun) -> RsfResult<()> {
    if run.entries.is_empty() {
        return Err(invalid("compaction run must include at least one entry"));
    }
    for entry in &run.entries {
        if entry.key < manifest.first_key || entry.key > manifest.last_key {
            return Err(invalid(format!(
                "entry key {} outside SSTable manifest bounds",
                String::from_utf8_lossy(&entry.key)
            )));
        }
    }
    Ok(())
}

fn checksum(entries: &[CompactionEntry]) -> Digest {
    let mut encoded = Vec::new();
    for entry in entries {
        encoded.extend_from_slice(&(entry.key.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&entry.key);
        encoded.extend_from_slice(&entry.version.0.to_le_bytes());
        if let Some(value) = &entry.value {
            encoded.push(1);
            encoded.extend_from_slice(&(value.len() as u64).to_le_bytes());
            encoded.extend_from_slice(value);
        } else {
            encoded.push(0);
        }
    }
    Digest::blake3_bytes(&encoded)
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use rsf_core::PlacementGroupId;
    use rsf_range::RangeId;

    use super::*;

    fn manifest(id: u8, first: &[u8], last: &[u8], version: u64, bytes: u64) -> SstableManifest {
        SstableManifest {
            range: RangeId(7),
            level: 0,
            first_key: first.to_vec(),
            last_key: last.to_vec(),
            version: CommitVersion(version),
            bytes,
            checksum: Digest::blake3_bytes(
                format!(
                    "compaction-input:{id}:{}:{}:{version}:{bytes}",
                    String::from_utf8_lossy(first),
                    String::from_utf8_lossy(last)
                )
                .as_bytes(),
            ),
        }
    }

    fn entry(key: &[u8], value: Option<&[u8]>, version: u64) -> CompactionEntry {
        CompactionEntry {
            key: key.to_vec(),
            value: value.map(|value| value.to_vec()),
            version: CommitVersion(version),
        }
    }

    fn plan(inputs: Vec<SstableManifest>) -> CompactionPlan {
        CompactionPlan {
            range: RangeId(7),
            inputs,
            output_level: 1,
            estimated_bytes: 384,
        }
    }

    #[test]
    fn compaction_keeps_newest_values_and_drops_tombstoned_keys() {
        let old = manifest(1, b"app/users/1", b"app/users/9", 21, 160);
        let new = manifest(2, b"app/users/1", b"app/users/9", 22, 224);
        let output = CompactionExecutor {
            gc_before: CommitVersion(21),
        }
        .compact(
            &plan(vec![old.clone(), new.clone()]),
            &[
                CompactionRun {
                    manifest: old,
                    entries: vec![
                        entry(b"app/users/1", Some(b"ada"), 21),
                        entry(b"app/users/2", Some(b"grace"), 21),
                    ],
                },
                CompactionRun {
                    manifest: new,
                    entries: vec![
                        entry(b"app/users/1", Some(b"ada-lovelace"), 22),
                        entry(b"app/users/2", None, 22),
                    ],
                },
            ],
        )
        .unwrap();

        assert_eq!(output.live_entries.len(), 1);
        assert_eq!(output.live_entries[0].key, b"app/users/1".to_vec());
        assert_eq!(output.live_entries[0].value, Some(b"ada-lovelace".to_vec()));
        assert_eq!(output.tombstones, 1);
        assert!(output.reclaimed_bytes > 0);
        assert_eq!(output.manifest.level, 1);
        assert_ne!(output.manifest.checksum.as_bytes(), &[0; 32]);
    }

    #[test]
    fn missing_planned_run_is_rejected() {
        let old = manifest(1, b"app/users/1", b"app/users/9", 21, 160);
        let new = manifest(2, b"app/users/1", b"app/users/9", 22, 224);
        let err = CompactionExecutor {
            gc_before: CommitVersion(21),
        }
        .compact(
            &plan(vec![old.clone(), new]),
            &[CompactionRun {
                manifest: old,
                entries: vec![entry(b"app/users/1", Some(b"ada"), 21)],
            }],
        )
        .expect_err("missing run should fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn entry_outside_manifest_bounds_is_rejected() {
        let old = manifest(1, b"app/users/1", b"app/users/3", 21, 160);
        let new = manifest(2, b"app/users/4", b"app/users/9", 22, 224);
        let err = CompactionExecutor {
            gc_before: CommitVersion(21),
        }
        .compact(
            &CompactionPlan {
                range: RangeId(7),
                inputs: vec![old.clone(), new.clone()],
                output_level: 1,
                estimated_bytes: 384,
            },
            &[
                CompactionRun {
                    manifest: old,
                    entries: vec![entry(b"app/users/0", Some(b"bad"), 21)],
                },
                CompactionRun {
                    manifest: new,
                    entries: vec![entry(b"app/users/4", Some(b"ok"), 22)],
                },
            ],
        )
        .expect_err("out-of-bounds entry should fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn one_input_plan_is_rejected() {
        let only = manifest(1, b"app/users/1", b"app/users/9", 21, 160);
        let err = CompactionExecutor {
            gc_before: CommitVersion(21),
        }
        .compact(
            &CompactionPlan {
                range: RangeId(7),
                inputs: vec![only.clone()],
                output_level: 1,
                estimated_bytes: 160,
            },
            &[CompactionRun {
                manifest: only,
                entries: vec![entry(b"app/users/1", Some(b"ada"), 21)],
            }],
        )
        .expect_err("single input compaction should fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn manifest_type_keeps_range_dependency_visible() {
        let manifest = SstableManifest {
            range: RangeId(PlacementGroupId(7).0),
            level: 1,
            first_key: b"a".to_vec(),
            last_key: b"b".to_vec(),
            version: CommitVersion(1),
            bytes: 1,
            checksum: Digest::blake3_bytes(b"manifest-type-keeps-range-dependency-visible"),
        };

        assert_eq!(manifest.range, RangeId(7));
    }
}
