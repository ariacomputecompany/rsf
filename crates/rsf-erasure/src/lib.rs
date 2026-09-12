#![forbid(unsafe_code)]

use std::collections::BTreeSet;

use rsf_core::{Bytes, Digest, ErrorCode, NodeId, RsfError, RsfResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErasurePolicy {
    pub data: u8,
    pub parity: u8,
}

impl ErasurePolicy {
    pub fn new(data: u8, parity: u8) -> RsfResult<Self> {
        if data == 0 {
            return Err(invalid("erasure policy must include data shards"));
        }
        if parity == 0 {
            return Err(invalid("erasure policy must include parity shards"));
        }
        let total = u16::from(data) + u16::from(parity);
        if total > 255 {
            return Err(invalid("erasure policy supports at most 255 total shards"));
        }
        Ok(Self { data, parity })
    }

    pub fn total_shards(self) -> usize {
        (self.data + self.parity) as usize
    }

    pub fn read_threshold(self) -> usize {
        self.data as usize
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardManifest {
    pub ordinal: u8,
    pub kind: ShardKind,
    pub node: NodeId,
    pub bytes: Bytes,
    pub checksum: Digest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardKind {
    Data,
    Parity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectStripe {
    pub policy: ErasurePolicy,
    pub original_len: usize,
    pub shards: Vec<ShardManifest>,
}

impl ObjectStripe {
    pub fn missing_tolerance(&self) -> usize {
        self.policy.parity as usize
    }

    pub fn data_shards(&self) -> impl Iterator<Item = &ShardManifest> {
        self.shards
            .iter()
            .filter(|shard| shard.kind == ShardKind::Data)
    }

    pub fn parity_shards(&self) -> impl Iterator<Item = &ShardManifest> {
        self.shards
            .iter()
            .filter(|shard| shard.kind == ShardKind::Parity)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErasurePlanner {
    policy: ErasurePolicy,
}

impl ErasurePlanner {
    pub fn new(policy: ErasurePolicy) -> Self {
        Self { policy }
    }

    pub fn stripe(&self, payload: &[u8], nodes: &[NodeId]) -> RsfResult<ObjectStripe> {
        if nodes.len() < self.policy.total_shards() {
            return Err(invalid(format!(
                "erasure policy requires {} nodes but only {} were provided",
                self.policy.total_shards(),
                nodes.len()
            )));
        }
        if payload.is_empty() {
            return Err(invalid("erasure payload must not be empty"));
        }

        let matrix = coding_matrix(self.policy)?;
        let shard_len = payload.len().div_ceil(self.policy.data as usize);
        let data = (0..self.policy.data)
            .map(|ordinal| {
                let start = ordinal as usize * shard_len;
                let end = payload.len().min(start + shard_len);
                let mut bytes = payload.get(start..end).unwrap_or_default().to_vec();
                bytes.resize(shard_len, 0);
                bytes
            })
            .collect::<Vec<_>>();
        let mut shards = Vec::with_capacity(self.policy.total_shards());
        for ordinal in 0..self.policy.data {
            shards.push(Self::manifest(
                ordinal,
                ShardKind::Data,
                nodes[ordinal as usize],
                data[ordinal as usize].clone(),
            ));
        }

        for index in 0..self.policy.parity {
            let ordinal = self.policy.data + index;
            shards.push(Self::manifest(
                ordinal,
                ShardKind::Parity,
                nodes[ordinal as usize],
                encode_row(&matrix[ordinal as usize], &data, shard_len),
            ));
        }

        Ok(ObjectStripe {
            policy: self.policy,
            original_len: payload.len(),
            shards,
        })
    }

    pub fn reconstruct(&self, stripe: &ObjectStripe, missing: &[u8]) -> RsfResult<Bytes> {
        if stripe.policy != self.policy {
            return Err(invalid("stripe policy does not match planner policy"));
        }
        validate_missing(self.policy, missing)?;
        validate_shard_set(self.policy, &stripe.shards, missing)?;
        for shard in &stripe.shards {
            if !missing.contains(&shard.ordinal) {
                shard.verify()?;
            }
        }
        if missing.len() > stripe.missing_tolerance() {
            return Err(invalid("too many missing shards for erasure policy"));
        }
        if stripe.shards.len() < self.policy.read_threshold() {
            return Err(invalid("not enough data shards to reconstruct payload"));
        }
        let Some(shard_len) = stripe.shards.first().map(|shard| shard.bytes.len()) else {
            return Err(invalid("stripe has no shards"));
        };
        for shard in &stripe.shards {
            if shard.bytes.len() != shard_len {
                return Err(invalid("erasure shards must have uniform length"));
            }
        }

        let matrix = coding_matrix(self.policy)?;
        let selected = stripe
            .shards
            .iter()
            .take(self.policy.read_threshold())
            .collect::<Vec<_>>();
        let decode_matrix = selected
            .iter()
            .map(|shard| matrix[shard.ordinal as usize].clone())
            .collect::<Vec<_>>();
        let inverse = invert_matrix(decode_matrix)?;
        let encoded = selected
            .iter()
            .map(|shard| shard.bytes.as_slice())
            .collect::<Vec<_>>();

        let mut output = Vec::new();
        for row in inverse.iter().take(self.policy.data as usize) {
            output.extend(encode_row(row, &encoded, shard_len));
        }
        output.truncate(stripe.original_len);
        Ok(output)
    }

    fn manifest(ordinal: u8, kind: ShardKind, node: NodeId, bytes: Bytes) -> ShardManifest {
        let checksum = Digest::blake3_bytes(&bytes);
        ShardManifest {
            ordinal,
            kind,
            node,
            bytes,
            checksum,
        }
    }
}

impl ShardManifest {
    pub fn verify(&self) -> RsfResult<()> {
        if Digest::blake3_bytes(&self.bytes) == self.checksum {
            return Ok(());
        }
        Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "erasure shard checksum mismatch",
        ))
    }
}

fn validate_missing(policy: ErasurePolicy, missing: &[u8]) -> RsfResult<()> {
    let mut seen = BTreeSet::new();
    for ordinal in missing {
        if usize::from(*ordinal) >= policy.total_shards() {
            return Err(invalid(format!(
                "missing shard ordinal {ordinal} is outside policy width {}",
                policy.total_shards()
            )));
        }
        if !seen.insert(*ordinal) {
            return Err(invalid(format!(
                "duplicate missing shard ordinal {ordinal}"
            )));
        }
    }
    Ok(())
}

fn validate_shard_set(
    policy: ErasurePolicy,
    shards: &[ShardManifest],
    missing: &[u8],
) -> RsfResult<()> {
    let missing = missing.iter().copied().collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    for shard in shards {
        if usize::from(shard.ordinal) >= policy.total_shards() {
            return Err(invalid(format!(
                "shard ordinal {} is outside policy width {}",
                shard.ordinal,
                policy.total_shards()
            )));
        }
        if missing.contains(&shard.ordinal) {
            return Err(invalid(format!(
                "shard ordinal {} is both present and missing",
                shard.ordinal
            )));
        }
        if !seen.insert(shard.ordinal) {
            return Err(invalid(format!(
                "duplicate present shard ordinal {}",
                shard.ordinal
            )));
        }
    }
    Ok(())
}

fn coding_matrix(policy: ErasurePolicy) -> RsfResult<Vec<Vec<u8>>> {
    let data = policy.data as usize;
    let total = policy.total_shards();
    let vandermonde = (0..total)
        .map(|row| {
            let x = (row + 1) as u8;
            (0..data).map(|col| gf_pow(x, col)).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let top = vandermonde.iter().take(data).cloned().collect::<Vec<_>>();
    let top_inverse = invert_matrix(top)?;
    Ok(vandermonde
        .iter()
        .map(|row| matrix_row_mul(row, &top_inverse))
        .collect())
}

fn encode_row<T>(row: &[u8], shards: &[T], shard_len: usize) -> Bytes
where
    T: AsRef<[u8]>,
{
    let mut out = vec![0u8; shard_len];
    for (coefficient, shard) in row.iter().zip(shards) {
        if *coefficient == 0 {
            continue;
        }
        for (index, byte) in shard.as_ref().iter().take(shard_len).enumerate() {
            out[index] ^= gf_mul(*coefficient, *byte);
        }
    }
    out
}

fn matrix_row_mul(row: &[u8], matrix: &[Vec<u8>]) -> Vec<u8> {
    let width = matrix.first().map_or(0, Vec::len);
    let mut out = vec![0; width];
    for (coefficient, matrix_row) in row.iter().zip(matrix) {
        if *coefficient == 0 {
            continue;
        }
        for (column, value) in matrix_row.iter().enumerate() {
            out[column] ^= gf_mul(*coefficient, *value);
        }
    }
    out
}

fn invert_matrix(mut matrix: Vec<Vec<u8>>) -> RsfResult<Vec<Vec<u8>>> {
    let size = matrix.len();
    if size == 0 || matrix.iter().any(|row| row.len() != size) {
        return Err(invalid(
            "erasure decode matrix must be non-empty and square",
        ));
    }
    let mut inverse = (0..size)
        .map(|row| {
            (0..size)
                .map(|column| u8::from(row == column))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    for column in 0..size {
        let Some(pivot) = (column..size).find(|row| matrix[*row][column] != 0) else {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "erasure decode matrix is singular",
            ));
        };
        matrix.swap(column, pivot);
        inverse.swap(column, pivot);

        let scale = gf_inv(matrix[column][column])?;
        for value in &mut matrix[column] {
            *value = gf_mul(*value, scale);
        }
        for value in &mut inverse[column] {
            *value = gf_mul(*value, scale);
        }

        for row in 0..size {
            if row == column {
                continue;
            }
            let factor = matrix[row][column];
            if factor == 0 {
                continue;
            }
            for inner in 0..size {
                matrix[row][inner] ^= gf_mul(factor, matrix[column][inner]);
                inverse[row][inner] ^= gf_mul(factor, inverse[column][inner]);
            }
        }
    }

    Ok(inverse)
}

fn gf_mul(mut left: u8, mut right: u8) -> u8 {
    let mut out = 0;
    while right != 0 {
        if right & 1 != 0 {
            out ^= left;
        }
        let carry = left & 0x80;
        left <<= 1;
        if carry != 0 {
            left ^= 0x1d;
        }
        right >>= 1;
    }
    out
}

fn gf_pow(value: u8, power: usize) -> u8 {
    let mut out = 1;
    for _ in 0..power {
        out = gf_mul(out, value);
    }
    out
}

fn gf_inv(value: u8) -> RsfResult<u8> {
    if value == 0 {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "zero has no inverse in erasure field",
        ));
    }
    Ok(gf_pow(value, 254))
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stripes_payload_into_data_and_parity_shards() {
        let planner = ErasurePlanner::new(ErasurePolicy::new(3, 2).unwrap());
        let stripe = planner
            .stripe(b"cold-object-payload", &nodes(5))
            .expect("stripe");

        assert_eq!(stripe.shards.len(), 5);
        assert_eq!(stripe.data_shards().count(), 3);
        assert_eq!(stripe.parity_shards().count(), 2);
        assert_eq!(stripe.missing_tolerance(), 2);
        for shard in &stripe.shards {
            shard.verify().unwrap();
        }
    }

    #[test]
    fn reconstructs_with_one_missing_data_shard() {
        let planner = ErasurePlanner::new(ErasurePolicy::new(3, 2).unwrap());
        let stripe = planner
            .stripe(b"cold-object-payload", &nodes(5))
            .expect("stripe");
        let degraded = without_shards(&stripe, &[1]);

        let payload = planner.reconstruct(&degraded, &[1]).expect("reconstruct");

        assert_eq!(payload, b"cold-object-payload");
    }

    #[test]
    fn reconstructs_with_two_missing_data_shards() {
        let planner = ErasurePlanner::new(ErasurePolicy::new(3, 2).unwrap());
        let stripe = planner
            .stripe(b"cold-object-payload", &nodes(5))
            .expect("stripe");
        let degraded = without_shards(&stripe, &[0, 1]);

        let payload = planner
            .reconstruct(&degraded, &[0, 1])
            .expect("reconstruct");

        assert_eq!(payload, b"cold-object-payload");
    }

    #[test]
    fn reconstructs_with_data_and_parity_loss() {
        let planner = ErasurePlanner::new(ErasurePolicy::new(4, 2).unwrap());
        let stripe = planner
            .stripe(b"larger-cold-object-payload-with-layout", &nodes(6))
            .expect("stripe");
        let degraded = without_shards(&stripe, &[2, 5]);

        let payload = planner
            .reconstruct(&degraded, &[2, 5])
            .expect("reconstruct");

        assert_eq!(payload, b"larger-cold-object-payload-with-layout");
    }

    #[test]
    fn shard_verify_rejects_payload_corruption() {
        let planner = ErasurePlanner::new(ErasurePolicy::new(3, 2).unwrap());
        let mut stripe = planner
            .stripe(b"cold-object-payload", &nodes(5))
            .expect("stripe");
        stripe.shards[0].bytes[0] ^= 0xff;

        let err = stripe.shards[0]
            .verify()
            .expect_err("mutated shard payload must fail digest verification");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    #[test]
    fn reconstruction_rejects_corrupt_present_shard_before_using_parity() {
        let planner = ErasurePlanner::new(ErasurePolicy::new(3, 2).unwrap());
        let mut stripe = planner
            .stripe(b"cold-object-payload", &nodes(5))
            .expect("stripe");
        stripe.shards[2].bytes[0] ^= 0xff;
        let degraded = without_shards(&stripe, &[1]);

        let err = planner
            .reconstruct(&degraded, &[1])
            .expect_err("corrupt present shard must fail reconstruction");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("checksum"));
    }

    #[test]
    fn rejects_too_many_missing_shards() {
        let planner = ErasurePlanner::new(ErasurePolicy::new(3, 1).unwrap());
        let stripe = planner.stripe(b"payload", &nodes(4)).expect("stripe");

        let err = planner
            .reconstruct(&stripe, &[0, 1])
            .expect_err("too many missing shards must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn rejects_shard_list_that_still_contains_missing_ordinal() {
        let planner = ErasurePlanner::new(ErasurePolicy::new(3, 2).unwrap());
        let stripe = planner.stripe(b"payload", &nodes(5)).expect("stripe");

        let err = planner
            .reconstruct(&stripe, &[1])
            .expect_err("missing shard must not also be present");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("both present and missing"));
    }

    #[test]
    fn rejects_insufficient_nodes() {
        let planner = ErasurePlanner::new(ErasurePolicy::new(4, 2).unwrap());

        let err = planner
            .stripe(b"payload", &nodes(5))
            .expect_err("insufficient nodes must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    fn nodes(count: u64) -> Vec<NodeId> {
        (1..=count).map(NodeId).collect()
    }

    fn without_shards(stripe: &ObjectStripe, missing: &[u8]) -> ObjectStripe {
        ObjectStripe {
            policy: stripe.policy,
            original_len: stripe.original_len,
            shards: stripe
                .shards
                .iter()
                .filter(|shard| !missing.contains(&shard.ordinal))
                .cloned()
                .collect(),
        }
    }
}
