//! Input custody facts distinguish resumable bytes from lost acknowledged input.
use super::*;

/// Complete chosen certificate plus exact old-quorum input descriptor. Payload
/// history travels separately; this package is bounded as one decision field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrMembershipChosenInput {
    pub chosen: VsrMembershipChosen,
    pub input: VsrMembershipInput,
}
impl VsrMembershipChosenInput {
    pub fn new(chosen: VsrMembershipChosen, input: VsrMembershipInput) -> RsfResult<Self> {
        let value = Self { chosen, input };
        value.encode()?;
        Ok(value)
    }
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        if self.chosen.proposal() != self.input.proposal()
            || self.chosen.input_id() != self.input.id()?
        {
            return Err(invalid_input(
                "chosen certificate differs from input descriptor",
            ));
        }
        let chosen = self.chosen.encode()?;
        let input = self.input.encode()?;
        let length = 23usize
            .checked_add(chosen.len())
            .and_then(|n| n.checked_add(input.len()))
            .ok_or_else(|| invalid_input("chosen input size overflow"))?;
        if length > MAX_HISTORY_RECORD_BYTES {
            return Err(invalid_input(
                "chosen input package exceeds bounded decision field",
            ));
        }
        let mut out = b"VSRMCI1".to_vec();
        put_bytes(&mut out, &chosen)?;
        put_bytes(&mut out, &input)?;
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::new(bytes, b"VSRMCI1")?;
        let chosen = VsrMembershipChosen::decode(r.bytes()?)?;
        let input = VsrMembershipInput::decode(r.bytes()?)?;
        r.finish()?;
        let value = Self::new(chosen, input)?;
        if value.encode()? != bytes {
            return Err(corrupt("noncanonical chosen input package"));
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VsrMembershipInputProgress {
    Missing,
    /// Strict unbound factory residue; BEGIN must verify the supplied descriptor
    /// and finish empty storage initialization before reporting a byte cursor.
    Initializing,
    Partial {
        input: VsrMembershipInput,
        records: u64,
    },
    Complete {
        input: VsrMembershipInput,
    },
}
impl VsrMembershipInputProgress {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let mut out = b"VSRMIP1".to_vec();
        match self {
            Self::Missing => out.push(0),
            Self::Initializing => out.push(3),
            Self::Partial { input, records } => {
                if *records >= input.records()? {
                    return Err(invalid_input(
                        "partial membership input is already complete",
                    ));
                }
                out.push(1);
                put_bytes(&mut out, &input.encode()?)?;
                put_u64(&mut out, *records);
            }
            Self::Complete { input } => {
                out.push(2);
                put_bytes(&mut out, &input.encode()?)?;
            }
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::bounded(bytes, b"VSRMIP1", MAX_HISTORY_RECORD_BYTES + 24)?;
        let value = match r.byte()? {
            0 => Self::Missing,
            3 => Self::Initializing,
            1 => Self::Partial {
                input: VsrMembershipInput::decode(r.bytes()?)?,
                records: r.u64()?,
            },
            2 => Self::Complete {
                input: VsrMembershipInput::decode(r.bytes()?)?,
            },
            _ => return Err(corrupt("invalid membership input progress variant")),
        };
        r.finish()?;
        if value.encode()? != bytes {
            return Err(corrupt("noncanonical membership input progress"));
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VsrMembershipImportReply {
    BeginRequired,
    Retained { input: [u8; 32], records: u64 },
    Chosen { input: [u8; 32] },
}
impl VsrMembershipImportReply {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let mut out = b"VSRMIR1".to_vec();
        match self {
            Self::BeginRequired => out.push(0),
            Self::Retained { input, records } => {
                out.push(1);
                out.extend_from_slice(input);
                put_u64(&mut out, *records);
            }
            Self::Chosen { input } => {
                out.push(2);
                out.extend_from_slice(input);
            }
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::bounded(bytes, b"VSRMIR1", 48)?;
        let value = match r.byte()? {
            0 => Self::BeginRequired,
            1 => Self::Retained {
                input: r.id()?,
                records: r.u64()?,
            },
            2 => Self::Chosen { input: r.id()? },
            _ => return Err(corrupt("invalid membership import reply")),
        };
        r.finish()?;
        if value.encode()? != bytes {
            return Err(corrupt("noncanonical membership import reply"));
        }
        Ok(value)
    }
}
