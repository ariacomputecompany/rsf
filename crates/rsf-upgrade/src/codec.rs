use rsf_core::{NodeId, RsfResult};

use crate::{
    SoftwareVersion, UpgradeGateCheck, UpgradePlan, UpgradeStep, UpgradeStepKind, corrupt, invalid,
    validate_plan, validate_step,
};

const UPGRADE_WAL_MAGIC: &[u8; 7] = b"RSFUPG1";
const UPGRADE_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum UpgradeWalRecord {
    Started { id: String, plan: UpgradePlan },
    StepCompleted { id: String, step: UpgradeStep },
    Failed { id: String, reason: String },
}

pub(crate) fn encode_upgrade_wal_payload(record: &UpgradeWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(UPGRADE_WAL_MAGIC);
    out.push(UPGRADE_WAL_VERSION);
    match record {
        UpgradeWalRecord::Started { id, plan } => {
            out.push(1);
            write_string(id, &mut out)?;
            encode_plan(plan, &mut out)?;
        }
        UpgradeWalRecord::StepCompleted { id, step } => {
            out.push(2);
            write_string(id, &mut out)?;
            encode_step(step, &mut out)?;
        }
        UpgradeWalRecord::Failed { id, reason } => {
            out.push(3);
            write_string(id, &mut out)?;
            write_string(reason, &mut out)?;
        }
    }
    Ok(out)
}

pub(crate) fn decode_upgrade_wal_payload(bytes: &[u8]) -> RsfResult<UpgradeWalRecord> {
    if bytes.len() < UPGRADE_WAL_MAGIC.len() + 2 {
        return Err(corrupt("upgrade WAL payload is too short"));
    }
    if &bytes[..UPGRADE_WAL_MAGIC.len()] != UPGRADE_WAL_MAGIC {
        return Err(corrupt("upgrade WAL payload has invalid magic"));
    }
    let mut offset = UPGRADE_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != UPGRADE_WAL_VERSION {
        return Err(corrupt(format!(
            "upgrade WAL payload version {version} is not supported"
        )));
    }
    let kind = read_u8(bytes, &mut offset)?;
    let record = match kind {
        1 => UpgradeWalRecord::Started {
            id: read_string(bytes, &mut offset)?,
            plan: decode_plan(bytes, &mut offset)?,
        },
        2 => UpgradeWalRecord::StepCompleted {
            id: read_string(bytes, &mut offset)?,
            step: decode_step(bytes, &mut offset)?,
        },
        3 => UpgradeWalRecord::Failed {
            id: read_string(bytes, &mut offset)?,
            reason: read_string(bytes, &mut offset)?,
        },
        other => {
            return Err(corrupt(format!(
                "upgrade WAL payload kind {other} is not supported"
            )));
        }
    };
    if offset != bytes.len() {
        return Err(corrupt("upgrade WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn encode_plan(plan: &UpgradePlan, out: &mut Vec<u8>) -> RsfResult<()> {
    validate_plan(plan)?;
    encode_version(&plan.from, out);
    encode_version(&plan.to, out);
    write_len(plan.steps.len(), "upgrade plan step count", out)?;
    for step in &plan.steps {
        encode_step(step, out)?;
    }
    write_len(
        plan.compatibility_checks.len(),
        "upgrade compatibility check count",
        out,
    )?;
    for check in &plan.compatibility_checks {
        write_string(check, out)?;
    }
    write_len(plan.gate_checks.len(), "upgrade gate check count", out)?;
    for check in &plan.gate_checks {
        write_string(&check.name, out)?;
        out.push(u8::from(check.passed));
        write_string(&check.detail, out)?;
    }
    Ok(())
}

fn decode_plan(bytes: &[u8], offset: &mut usize) -> RsfResult<UpgradePlan> {
    let from = decode_version(bytes, offset)?;
    let to = decode_version(bytes, offset)?;
    let step_len = read_len(bytes, offset, "upgrade plan step count")?;
    let mut steps = Vec::with_capacity(step_len);
    for _ in 0..step_len {
        steps.push(decode_step(bytes, offset)?);
    }
    let compatibility_len = read_len(bytes, offset, "upgrade compatibility check count")?;
    let mut compatibility_checks = Vec::with_capacity(compatibility_len);
    for _ in 0..compatibility_len {
        compatibility_checks.push(read_string(bytes, offset)?);
    }
    let gate_len = read_len(bytes, offset, "upgrade gate check count")?;
    let mut gate_checks = Vec::with_capacity(gate_len);
    for _ in 0..gate_len {
        gate_checks.push(UpgradeGateCheck {
            name: read_string(bytes, offset)?,
            passed: read_bool(bytes, offset)?,
            detail: read_string(bytes, offset)?,
        });
    }
    let plan = UpgradePlan {
        from,
        to,
        steps,
        compatibility_checks,
        gate_checks,
    };
    validate_plan(&plan)?;
    Ok(plan)
}

fn encode_step(step: &UpgradeStep, out: &mut Vec<u8>) -> RsfResult<()> {
    out.push(step_kind_to_u8(step.kind));
    match step.node {
        Some(node) => {
            out.push(1);
            write_u64(node.0, out);
        }
        None => out.push(0),
    }
    write_string(&step.detail, out)?;
    Ok(())
}

fn decode_step(bytes: &[u8], offset: &mut usize) -> RsfResult<UpgradeStep> {
    let kind = step_kind_from_u8(read_u8(bytes, offset)?)?;
    let node = match read_u8(bytes, offset)? {
        0 => None,
        1 => Some(NodeId(read_u64(bytes, offset)?)),
        other => return Err(corrupt(format!("upgrade node marker {other} is invalid"))),
    };
    let step = UpgradeStep {
        kind,
        node,
        detail: read_string(bytes, offset)?,
    };
    validate_step(&step)?;
    Ok(step)
}

fn encode_version(version: &SoftwareVersion, out: &mut Vec<u8>) {
    out.extend_from_slice(&version.major.to_le_bytes());
    out.extend_from_slice(&version.minor.to_le_bytes());
    out.extend_from_slice(&version.patch.to_le_bytes());
}

fn decode_version(bytes: &[u8], offset: &mut usize) -> RsfResult<SoftwareVersion> {
    Ok(SoftwareVersion {
        major: read_u16(bytes, offset)?,
        minor: read_u16(bytes, offset)?,
        patch: read_u16(bytes, offset)?,
    })
}

fn step_kind_to_u8(kind: UpgradeStepKind) -> u8 {
    match kind {
        UpgradeStepKind::Preflight => 1,
        UpgradeStepKind::Drain => 2,
        UpgradeStepKind::Install => 3,
        UpgradeStepKind::Probe => 4,
        UpgradeStepKind::Promote => 5,
    }
}

fn step_kind_from_u8(value: u8) -> RsfResult<UpgradeStepKind> {
    match value {
        1 => Ok(UpgradeStepKind::Preflight),
        2 => Ok(UpgradeStepKind::Drain),
        3 => Ok(UpgradeStepKind::Install),
        4 => Ok(UpgradeStepKind::Probe),
        5 => Ok(UpgradeStepKind::Promote),
        _ => Err(corrupt(format!("unknown upgrade step kind id {value}"))),
    }
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len =
        u32::try_from(value.len()).map_err(|_| invalid("upgrade string is too large for WAL"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_u32(bytes, offset)?;
    let len = usize::try_from(len).map_err(|_| corrupt("upgrade string length is too large"))?;
    let raw = bytes
        .get(*offset..offset.saturating_add(len))
        .ok_or_else(|| corrupt("upgrade WAL payload ended while reading string"))?;
    *offset = offset.saturating_add(len);
    String::from_utf8(raw.to_vec()).map_err(|_| corrupt("upgrade WAL payload has invalid UTF-8"))
}

fn write_len(value: usize, label: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let value = u32::try_from(value).map_err(|_| invalid(format!("{label} is too large")))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_len(bytes: &[u8], offset: &mut usize, label: &str) -> RsfResult<usize> {
    let len = read_u32(bytes, offset)?;
    usize::try_from(len).map_err(|_| corrupt(format!("{label} is too large")))
}

fn write_u64(value: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_bool(bytes: &[u8], offset: &mut usize) -> RsfResult<bool> {
    match read_u8(bytes, offset)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(corrupt(format!("upgrade bool value {other} is invalid"))),
    }
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("upgrade WAL payload ended while reading u8"))?;
    *offset = offset.saturating_add(1);
    Ok(value)
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> RsfResult<u16> {
    let raw = bytes
        .get(*offset..offset.saturating_add(2))
        .ok_or_else(|| corrupt("upgrade WAL payload ended while reading u16"))?;
    *offset = offset.saturating_add(2);
    Ok(u16::from_le_bytes(raw.try_into().expect("slice length")))
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = bytes
        .get(*offset..offset.saturating_add(4))
        .ok_or_else(|| corrupt("upgrade WAL payload ended while reading u32"))?;
    *offset = offset.saturating_add(4);
    Ok(u32::from_le_bytes(raw.try_into().expect("slice length")))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = bytes
        .get(*offset..offset.saturating_add(8))
        .ok_or_else(|| corrupt("upgrade WAL payload ended while reading u64"))?;
    *offset = offset.saturating_add(8);
    Ok(u64::from_le_bytes(raw.try_into().expect("slice length")))
}
