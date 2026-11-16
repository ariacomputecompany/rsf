#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use rsf_core::{CommitVersion, ErrorCode, RsfError, RsfResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedParamType {
    Int64,
    Text,
    Bool,
    Bytes,
}

impl PreparedParamType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Int64 => "int64",
            Self::Text => "text",
            Self::Bool => "bool",
            Self::Bytes => "bytes",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedValue {
    Int64(i64),
    Text(String),
    Bool(bool),
    Bytes(Vec<u8>),
}

impl PreparedValue {
    pub fn ty(&self) -> PreparedParamType {
        match self {
            Self::Int64(_) => PreparedParamType::Int64,
            Self::Text(_) => PreparedParamType::Text,
            Self::Bool(_) => PreparedParamType::Bool,
            Self::Bytes(_) => PreparedParamType::Bytes,
        }
    }

    fn render(&self) -> String {
        match self {
            Self::Int64(value) => value.to_string(),
            Self::Text(value) => format!("'{}'", value.replace('\'', "''")),
            Self::Bool(value) => value.to_string(),
            Self::Bytes(value) => format!("x'{}'", hex(value)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedStatement {
    pub name: String,
    pub sql: String,
    pub schema_version: CommitVersion,
    pub parameter_types: Vec<PreparedParamType>,
    pub uses: u64,
    last_used: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundPreparedSql {
    pub name: String,
    pub sql: String,
    pub schema_version: CommitVersion,
    pub parameters: Vec<PreparedValue>,
    pub use_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedDecision {
    Prepared {
        name: String,
        parameters: usize,
        schema_version: CommitVersion,
    },
    Bound(BoundPreparedSql),
    Invalidated {
        name: String,
        prepared_version: CommitVersion,
        current_version: CommitVersion,
    },
    Deallocated {
        name: String,
    },
    Evicted {
        name: String,
    },
    Rejected {
        name: String,
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedCache {
    capacity: usize,
    clock: u64,
    statements: BTreeMap<String, PreparedStatement>,
}

impl PreparedCache {
    pub fn new(capacity: usize) -> RsfResult<Self> {
        if capacity == 0 {
            return Err(invalid(
                "prepared statement cache capacity must be greater than zero",
            ));
        }
        Ok(Self {
            capacity,
            clock: 0,
            statements: BTreeMap::new(),
        })
    }

    pub fn prepare(
        &mut self,
        name: impl Into<String>,
        sql: impl Into<String>,
        schema_version: CommitVersion,
        parameter_types: Vec<PreparedParamType>,
    ) -> RsfResult<Vec<PreparedDecision>> {
        let name = name.into();
        let sql = normalize_sql(sql.into())?;
        validate_name(&name)?;
        let placeholders = placeholders(&sql)?;
        if placeholders.len() != parameter_types.len() {
            return Ok(vec![PreparedDecision::Rejected {
                name,
                reason: "placeholder count does not match parameter types".to_string(),
            }]);
        }
        for index in 1..=parameter_types.len() {
            if !placeholders.contains(&index) {
                return Ok(vec![PreparedDecision::Rejected {
                    name,
                    reason: format!("missing placeholder ${index}"),
                }]);
            }
        }

        self.clock = self.clock.saturating_add(1);
        let statement = PreparedStatement {
            name: name.clone(),
            sql,
            schema_version,
            parameter_types,
            uses: 0,
            last_used: self.clock,
        };
        self.statements.insert(name.clone(), statement);

        let mut decisions = vec![PreparedDecision::Prepared {
            name,
            parameters: self
                .statements
                .values()
                .max_by_key(|statement| statement.last_used)
                .map(|statement| statement.parameter_types.len())
                .unwrap_or_default(),
            schema_version,
        }];
        if self.statements.len() > self.capacity {
            let evicted = self.evict_lru();
            decisions.push(PreparedDecision::Evicted { name: evicted });
        }
        Ok(decisions)
    }

    pub fn bind(
        &mut self,
        name: &str,
        current_schema_version: CommitVersion,
        parameters: Vec<PreparedValue>,
    ) -> RsfResult<PreparedDecision> {
        validate_name(name)?;
        let Some(statement) = self.statements.get_mut(name) else {
            return Ok(PreparedDecision::Rejected {
                name: name.to_string(),
                reason: "prepared statement is not cached".to_string(),
            });
        };
        if statement.schema_version != current_schema_version {
            let prepared_version = statement.schema_version;
            self.statements.remove(name);
            return Ok(PreparedDecision::Invalidated {
                name: name.to_string(),
                prepared_version,
                current_version: current_schema_version,
            });
        }
        if parameters.len() != statement.parameter_types.len() {
            return Ok(PreparedDecision::Rejected {
                name: name.to_string(),
                reason: format!(
                    "expected {} parameters, got {}",
                    statement.parameter_types.len(),
                    parameters.len()
                ),
            });
        }
        for (idx, (actual, expected)) in parameters
            .iter()
            .map(PreparedValue::ty)
            .zip(statement.parameter_types.iter().copied())
            .enumerate()
        {
            if actual != expected {
                return Ok(PreparedDecision::Rejected {
                    name: name.to_string(),
                    reason: format!(
                        "parameter {} expected {}, got {}",
                        idx + 1,
                        expected.as_str(),
                        actual.as_str()
                    ),
                });
            }
        }

        self.clock = self.clock.saturating_add(1);
        statement.last_used = self.clock;
        statement.uses = statement.uses.saturating_add(1);
        let sql = bind_sql(&statement.sql, &parameters)?;
        Ok(PreparedDecision::Bound(BoundPreparedSql {
            name: name.to_string(),
            sql,
            schema_version: current_schema_version,
            parameters,
            use_count: statement.uses,
        }))
    }

    pub fn deallocate(&mut self, name: &str) -> RsfResult<PreparedDecision> {
        validate_name(name)?;
        if self.statements.remove(name).is_some() {
            Ok(PreparedDecision::Deallocated {
                name: name.to_string(),
            })
        } else {
            Ok(PreparedDecision::Rejected {
                name: name.to_string(),
                reason: "prepared statement is not cached".to_string(),
            })
        }
    }

    pub fn len(&self) -> usize {
        self.statements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    fn evict_lru(&mut self) -> String {
        let victim = self
            .statements
            .values()
            .min_by_key(|statement| statement.last_used)
            .map(|statement| statement.name.clone())
            .expect("cache above capacity must contain an entry");
        self.statements.remove(&victim);
        victim
    }
}

fn bind_sql(sql: &str, parameters: &[PreparedValue]) -> RsfResult<String> {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        match ch {
            '\'' => copy_single_quoted(sql, &mut chars, idx, &mut out),
            '"' => copy_double_quoted(sql, &mut chars, idx, &mut out),
            '-' if matches!(chars.peek(), Some((_, '-'))) => {
                out.push(ch);
                let (_, next) = chars.next().expect("peeked line comment marker");
                out.push(next);
                copy_line_comment(&mut chars, &mut out);
            }
            '/' if matches!(chars.peek(), Some((_, '*'))) => {
                out.push(ch);
                let (_, next) = chars.next().expect("peeked block comment marker");
                out.push(next);
                copy_block_comment(&mut chars, &mut out);
            }
            '$' => {
                let mut number = String::new();
                while let Some((_, digit)) = chars.peek().copied() {
                    if !digit.is_ascii_digit() {
                        break;
                    }
                    number.push(digit);
                    chars.next();
                }
                if number.is_empty() {
                    out.push(ch);
                    continue;
                }
                let index = number
                    .parse::<usize>()
                    .map_err(|_| invalid("placeholder index is too large"))?;
                if index == 0 || index > parameters.len() {
                    return Err(invalid(format!("placeholder ${index} has no parameter")));
                }
                out.push_str(&parameters[index - 1].render());
            }
            _ => out.push(ch),
        }
    }
    Ok(out)
}

fn placeholders(sql: &str) -> RsfResult<BTreeSet<usize>> {
    let mut placeholders = BTreeSet::new();
    let mut chars = sql.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        match ch {
            '\'' => skip_single_quoted(sql, &mut chars, idx),
            '"' => skip_double_quoted(sql, &mut chars, idx),
            '-' if matches!(chars.peek(), Some((_, '-'))) => {
                chars.next();
                skip_line_comment(&mut chars);
            }
            '/' if matches!(chars.peek(), Some((_, '*'))) => {
                chars.next();
                skip_block_comment(&mut chars);
            }
            '$' => {
                let mut number = String::new();
                while let Some((_, digit)) = chars.peek().copied() {
                    if !digit.is_ascii_digit() {
                        break;
                    }
                    number.push(digit);
                    chars.next();
                }
                if number.is_empty() {
                    continue;
                }
                let index = number
                    .parse::<usize>()
                    .map_err(|_| invalid("placeholder index is too large"))?;
                if index == 0 {
                    return Err(invalid("placeholder indexes start at $1"));
                }
                placeholders.insert(index);
            }
            _ => {}
        }
    }
    Ok(placeholders)
}

fn copy_single_quoted(
    sql: &str,
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
    start: usize,
    out: &mut String,
) {
    out.push('\'');
    skip_or_copy_single_quoted(sql, chars, start, Some(out));
}

fn skip_single_quoted(
    sql: &str,
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
    start: usize,
) {
    skip_or_copy_single_quoted(sql, chars, start, None);
}

fn skip_or_copy_single_quoted(
    _sql: &str,
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
    _start: usize,
    mut out: Option<&mut String>,
) {
    while let Some((_, ch)) = chars.next() {
        if let Some(out) = out.as_deref_mut() {
            out.push(ch);
        }
        if ch == '\'' {
            if matches!(chars.peek(), Some((_, '\''))) {
                let (_, escaped) = chars.next().expect("peeked escaped quote");
                if let Some(out) = out.as_deref_mut() {
                    out.push(escaped);
                }
                continue;
            }
            break;
        }
    }
}

fn copy_double_quoted(
    sql: &str,
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
    start: usize,
    out: &mut String,
) {
    out.push('"');
    skip_or_copy_double_quoted(sql, chars, start, Some(out));
}

fn skip_double_quoted(
    sql: &str,
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
    start: usize,
) {
    skip_or_copy_double_quoted(sql, chars, start, None);
}

fn skip_or_copy_double_quoted(
    _sql: &str,
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
    _start: usize,
    mut out: Option<&mut String>,
) {
    while let Some((_, ch)) = chars.next() {
        if let Some(out) = out.as_deref_mut() {
            out.push(ch);
        }
        if ch == '"' {
            if matches!(chars.peek(), Some((_, '"'))) {
                let (_, escaped) = chars.next().expect("peeked escaped quote");
                if let Some(out) = out.as_deref_mut() {
                    out.push(escaped);
                }
                continue;
            }
            break;
        }
    }
}

fn copy_line_comment(chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>, out: &mut String) {
    for (_, ch) in chars.by_ref() {
        out.push(ch);
        if ch == '\n' {
            break;
        }
    }
}

fn skip_line_comment(chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>) {
    for (_, ch) in chars.by_ref() {
        if ch == '\n' {
            break;
        }
    }
}

fn copy_block_comment(
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
    out: &mut String,
) {
    let mut previous = '\0';
    for (_, ch) in chars.by_ref() {
        out.push(ch);
        if previous == '*' && ch == '/' {
            break;
        }
        previous = ch;
    }
}

fn skip_block_comment(chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>) {
    let mut previous = '\0';
    for (_, ch) in chars.by_ref() {
        if previous == '*' && ch == '/' {
            break;
        }
        previous = ch;
    }
}

fn normalize_sql(sql: String) -> RsfResult<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err(invalid("prepared SQL must not be empty"));
    }
    Ok(trimmed.to_string())
}

fn validate_name(name: &str) -> RsfResult<()> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(invalid(
            "prepared statement name must be non-empty ASCII alphanumeric, '_' or '-'",
        ));
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    const CHARS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(CHARS[(byte >> 4) as usize] as char);
        out.push(CHARS[(byte & 0x0f) as usize] as char);
    }
    out
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepares_and_binds_typed_parameters() {
        let mut cache = PreparedCache::new(8).expect("cache");
        cache
            .prepare(
                "get_user",
                "SELECT id, name FROM users WHERE id = $1 AND active = $2",
                CommitVersion(10),
                vec![PreparedParamType::Int64, PreparedParamType::Bool],
            )
            .expect("prepare");

        match cache
            .bind(
                "get_user",
                CommitVersion(10),
                vec![PreparedValue::Int64(7), PreparedValue::Bool(true)],
            )
            .expect("bind")
        {
            PreparedDecision::Bound(bound) => {
                assert_eq!(
                    bound.sql,
                    "SELECT id, name FROM users WHERE id = 7 AND active = true"
                );
                assert_eq!(bound.use_count, 1);
            }
            other => panic!("expected bound statement, got {other:?}"),
        }
    }

    #[test]
    fn bind_ignores_placeholders_inside_quotes_and_comments() {
        let mut cache = PreparedCache::new(8).expect("cache");
        cache
            .prepare(
                "quoted",
                "SELECT '$1' AS literal, \"$2\" FROM users WHERE id = $1 -- $2\n/* $3 */ AND name = $2",
                CommitVersion(10),
                vec![PreparedParamType::Int64, PreparedParamType::Text],
            )
            .expect("prepare");

        match cache
            .bind(
                "quoted",
                CommitVersion(10),
                vec![
                    PreparedValue::Int64(7),
                    PreparedValue::Text("ada".to_string()),
                ],
            )
            .expect("bind")
        {
            PreparedDecision::Bound(bound) => {
                assert_eq!(
                    bound.sql,
                    "SELECT '$1' AS literal, \"$2\" FROM users WHERE id = 7 -- $2\n/* $3 */ AND name = 'ada'"
                );
            }
            other => panic!("expected bound statement, got {other:?}"),
        }
    }

    #[test]
    fn prepare_rejects_zero_placeholder_index() {
        let mut cache = PreparedCache::new(8).expect("cache");
        let err = cache
            .prepare(
                "zero",
                "SELECT id FROM users WHERE id = $0",
                CommitVersion(10),
                vec![PreparedParamType::Int64],
            )
            .expect_err("zero placeholder should be invalid");

        assert!(err.message.contains("placeholder indexes start at $1"));
    }

    #[test]
    fn prepare_rejects_non_contiguous_placeholders() {
        let mut cache = PreparedCache::new(8).expect("cache");
        let decisions = cache
            .prepare(
                "gap",
                "SELECT id FROM users WHERE id = $2",
                CommitVersion(10),
                vec![PreparedParamType::Int64],
            )
            .expect("prepare");

        assert!(matches!(
            decisions.as_slice(),
            [PreparedDecision::Rejected { reason, .. }] if reason.contains("missing placeholder $1")
        ));
    }

    #[test]
    fn bind_distinguishes_placeholder_prefixes() {
        let sql = bind_sql(
            "SELECT $10 AS tenth, $1 AS first",
            &[
                PreparedValue::Int64(1),
                PreparedValue::Int64(2),
                PreparedValue::Int64(3),
                PreparedValue::Int64(4),
                PreparedValue::Int64(5),
                PreparedValue::Int64(6),
                PreparedValue::Int64(7),
                PreparedValue::Int64(8),
                PreparedValue::Int64(9),
                PreparedValue::Text("ten".to_string()),
            ],
        )
        .expect("bind sql");

        assert_eq!(sql, "SELECT 'ten' AS tenth, 1 AS first");
    }

    #[test]
    fn schema_version_change_invalidates_statement() {
        let mut cache = PreparedCache::new(8).expect("cache");
        cache
            .prepare(
                "get_user",
                "SELECT id FROM users WHERE id = $1",
                CommitVersion(10),
                vec![PreparedParamType::Int64],
            )
            .expect("prepare");

        match cache
            .bind("get_user", CommitVersion(11), vec![PreparedValue::Int64(7)])
            .expect("bind")
        {
            PreparedDecision::Invalidated {
                prepared_version,
                current_version,
                ..
            } => {
                assert_eq!(prepared_version, CommitVersion(10));
                assert_eq!(current_version, CommitVersion(11));
            }
            other => panic!("expected invalidation, got {other:?}"),
        }
        assert!(cache.is_empty());
    }

    #[test]
    fn type_mismatch_rejects_bind() {
        let mut cache = PreparedCache::new(8).expect("cache");
        cache
            .prepare(
                "get_user",
                "SELECT id FROM users WHERE id = $1",
                CommitVersion(10),
                vec![PreparedParamType::Int64],
            )
            .expect("prepare");

        match cache
            .bind(
                "get_user",
                CommitVersion(10),
                vec![PreparedValue::Text("7".to_string())],
            )
            .expect("bind")
        {
            PreparedDecision::Rejected { reason, .. } => {
                assert!(reason.contains("expected int64"));
            }
            other => panic!("expected rejection, got {other:?}"),
        }
    }

    #[test]
    fn capacity_evicts_least_recent_statement() {
        let mut cache = PreparedCache::new(1).expect("cache");
        cache
            .prepare(
                "first",
                "SELECT id FROM users WHERE id = $1",
                CommitVersion(10),
                vec![PreparedParamType::Int64],
            )
            .expect("prepare first");
        let decisions = cache
            .prepare(
                "second",
                "SELECT name FROM users WHERE id = $1",
                CommitVersion(10),
                vec![PreparedParamType::Int64],
            )
            .expect("prepare second");

        assert!(matches!(
            decisions.as_slice(),
            [PreparedDecision::Prepared { .. }, PreparedDecision::Evicted { name }] if name == "first"
        ));
        assert_eq!(cache.len(), 1);
    }
}
