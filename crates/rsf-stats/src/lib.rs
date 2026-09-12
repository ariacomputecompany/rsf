#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult,
    catalog::{TableDescriptor, TableName},
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const STATS_PG: PlacementGroupId = PlacementGroupId(51);
const STATS_WAL_MAGIC: &[u8; 7] = b"RSFSTS1";
const STATS_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableEstimate {
    pub table: TableName,
    pub rows: u64,
    pub total_bytes: u64,
    pub version: CommitVersion,
    pub columns: Vec<ColumnEstimate>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnEstimate {
    pub column: String,
    pub distinct_values: u64,
    pub nulls: u64,
    pub histogram: Vec<HistogramBucket>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistogramBucket {
    pub value: String,
    pub rows: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableSample {
    pub table: TableName,
    pub rows: u64,
    pub total_bytes: u64,
    pub version: CommitVersion,
    pub columns: Vec<ColumnSample>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnSample {
    pub column: String,
    pub values: Vec<ValueSample>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValueSample {
    pub value: Option<String>,
    pub rows: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Freshness {
    Fresh,
    Stale { lag: u64 },
}

#[derive(Clone, Debug, Default)]
pub struct StatisticsCatalog {
    estimates: BTreeMap<String, TableEstimate>,
}

impl StatisticsCatalog {
    pub fn publish(
        &mut self,
        table: &TableDescriptor,
        sample: TableSample,
    ) -> RsfResult<TableEstimate> {
        validate_sample(table, &sample)?;
        let estimate = TableEstimate {
            table: sample.table.clone(),
            rows: sample.rows,
            total_bytes: sample.total_bytes,
            version: sample.version,
            columns: sample
                .columns
                .into_iter()
                .map(column_estimate)
                .collect::<RsfResult<Vec<_>>>()?,
        };
        self.estimates.insert(sample.table.0, estimate.clone());
        Ok(estimate)
    }

    pub fn estimate(&self, table: &TableName) -> Option<&TableEstimate> {
        self.estimates.get(&table.0)
    }

    pub fn freshness(
        &self,
        table: &TableName,
        catalog_version: CommitVersion,
        max_lag: u64,
    ) -> RsfResult<Freshness> {
        let estimate = self
            .estimate(table)
            .ok_or_else(|| invalid(format!("missing statistics for table {}", table.0)))?;
        let lag = catalog_version.0.saturating_sub(estimate.version.0);
        if lag > max_lag {
            Ok(Freshness::Stale { lag })
        } else {
            Ok(Freshness::Fresh)
        }
    }

    pub fn table_count(&self) -> usize {
        self.estimates.len()
    }

    fn apply_estimate(&mut self, estimate: TableEstimate) -> RsfResult<()> {
        validate_estimate(&estimate)?;
        self.estimates.insert(estimate.table.0.clone(), estimate);
        Ok(())
    }
}

#[derive(Debug)]
pub struct DurableStatisticsCatalog {
    catalog: StatisticsCatalog,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableStatisticsCatalog {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter { pg: Some(STATS_PG) })?;
        let mut catalog = StatisticsCatalog::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Stats)
        {
            apply_stats_record(&mut catalog, decode_stats_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            catalog,
            store,
            epoch,
            next_index,
        })
    }

    pub fn publish(
        &mut self,
        table: &TableDescriptor,
        sample: TableSample,
    ) -> RsfResult<TableEstimate> {
        let mut catalog = self.catalog.clone();
        let estimate = catalog.publish(table, sample)?;
        self.append_record(&StatsWalRecord::Estimate(estimate.clone()))?;
        self.catalog = catalog;
        Ok(estimate)
    }

    pub fn estimate(&self, table: &TableName) -> Option<&TableEstimate> {
        self.catalog.estimate(table)
    }

    pub fn freshness(
        &self,
        table: &TableName,
        catalog_version: CommitVersion,
        max_lag: u64,
    ) -> RsfResult<Freshness> {
        self.catalog.freshness(table, catalog_version, max_lag)
    }

    pub fn table_count(&self) -> usize {
        self.catalog.table_count()
    }

    pub fn catalog(&self) -> &StatisticsCatalog {
        &self.catalog
    }

    fn append_record(&mut self, record: &StatsWalRecord) -> RsfResult<()> {
        let payload = encode_stats_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(STATS_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: STATS_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Stats,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StatsWalRecord {
    Estimate(TableEstimate),
}

fn apply_stats_record(catalog: &mut StatisticsCatalog, record: StatsWalRecord) -> RsfResult<()> {
    match record {
        StatsWalRecord::Estimate(estimate) => catalog.apply_estimate(estimate),
    }
}

impl TableEstimate {
    pub fn average_row_bytes(&self) -> u64 {
        if self.rows == 0 {
            0
        } else {
            self.total_bytes / self.rows
        }
    }

    pub fn histogram_bucket_count(&self) -> usize {
        self.columns
            .iter()
            .map(|column| column.histogram.len())
            .sum()
    }
}

fn validate_sample(table: &TableDescriptor, sample: &TableSample) -> RsfResult<()> {
    if sample.table != table.name {
        return Err(invalid(format!(
            "statistics sample targets {}, not table {}",
            sample.table.0, table.name.0
        )));
    }
    if sample.rows == 0 {
        return Err(invalid("statistics sample must include at least one row"));
    }
    if sample.total_bytes < sample.rows {
        return Err(invalid("statistics bytes must be at least row count"));
    }
    for column in &sample.columns {
        if column.column.trim().is_empty() {
            return Err(invalid("statistics column name must not be empty"));
        }
        if !table
            .columns
            .iter()
            .any(|descriptor| descriptor.name == column.column)
        {
            return Err(invalid(format!(
                "statistics column {} is not in table {}",
                column.column, table.name.0
            )));
        }
    }
    Ok(())
}

fn validate_estimate(estimate: &TableEstimate) -> RsfResult<()> {
    if estimate.table.0.trim().is_empty() {
        return Err(invalid("statistics estimate table name must not be empty"));
    }
    if estimate.rows == 0 {
        return Err(invalid("statistics estimate must include at least one row"));
    }
    if estimate.total_bytes < estimate.rows {
        return Err(invalid(
            "statistics estimate bytes must be at least row count",
        ));
    }
    for column in &estimate.columns {
        if column.column.trim().is_empty() {
            return Err(invalid("statistics estimate column name must not be empty"));
        }
        let histogram_rows =
            column
                .histogram
                .iter()
                .try_fold(0_u64, |acc, bucket| -> RsfResult<u64> {
                    if bucket.value.trim().is_empty() {
                        return Err(invalid(
                            "statistics histogram bucket value must not be empty",
                        ));
                    }
                    if bucket.rows == 0 {
                        return Err(invalid(
                            "statistics histogram bucket must include at least one row",
                        ));
                    }
                    acc.checked_add(bucket.rows)
                        .ok_or_else(|| invalid("statistics histogram rows overflowed"))
                })?;
        let sampled_rows = histogram_rows
            .checked_add(column.nulls)
            .ok_or_else(|| invalid("statistics column rows overflowed"))?;
        if sampled_rows > estimate.rows {
            return Err(invalid(
                "statistics column sample rows cannot exceed table rows",
            ));
        }
        if column.distinct_values != column.histogram.len() as u64 {
            return Err(invalid(
                "statistics distinct value count must match histogram buckets",
            ));
        }
    }
    Ok(())
}

fn column_estimate(sample: ColumnSample) -> RsfResult<ColumnEstimate> {
    if sample.values.is_empty() {
        return Err(invalid(format!(
            "statistics column {} must include samples",
            sample.column
        )));
    }
    let mut buckets = BTreeMap::<String, u64>::new();
    let mut nulls = 0;
    for sample_value in sample.values {
        if sample_value.rows == 0 {
            return Err(invalid(format!(
                "statistics column {} has empty value sample",
                sample.column
            )));
        }
        if let Some(value) = sample_value.value {
            *buckets.entry(value).or_default() += sample_value.rows;
        } else {
            nulls += sample_value.rows;
        }
    }
    Ok(ColumnEstimate {
        column: sample.column,
        distinct_values: buckets.len() as u64,
        nulls,
        histogram: buckets
            .into_iter()
            .map(|(value, rows)| HistogramBucket { value, rows })
            .collect(),
    })
}

fn encode_stats_wal_payload(record: &StatsWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(STATS_WAL_MAGIC);
    out.push(STATS_WAL_VERSION);
    match record {
        StatsWalRecord::Estimate(estimate) => {
            validate_estimate(estimate)?;
            out.push(1);
            write_string(&mut out, &estimate.table.0)?;
            write_u64(&mut out, estimate.rows);
            write_u64(&mut out, estimate.total_bytes);
            write_u64(&mut out, estimate.version.0);
            write_len(&mut out, estimate.columns.len())?;
            for column in &estimate.columns {
                write_string(&mut out, &column.column)?;
                write_u64(&mut out, column.distinct_values);
                write_u64(&mut out, column.nulls);
                write_len(&mut out, column.histogram.len())?;
                for bucket in &column.histogram {
                    write_string(&mut out, &bucket.value)?;
                    write_u64(&mut out, bucket.rows);
                }
            }
        }
    }
    Ok(out)
}

fn decode_stats_wal_payload(bytes: &[u8]) -> RsfResult<StatsWalRecord> {
    if bytes.len() < STATS_WAL_MAGIC.len() + 2 {
        return Err(corrupt("stats WAL payload is too short"));
    }
    if &bytes[..STATS_WAL_MAGIC.len()] != STATS_WAL_MAGIC {
        return Err(corrupt("stats WAL magic mismatch"));
    }
    let mut offset = STATS_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != STATS_WAL_VERSION {
        return Err(corrupt("unsupported stats WAL version"));
    }
    let tag = read_u8(bytes, &mut offset)?;
    let record = match tag {
        1 => {
            let table = TableName(read_string(bytes, &mut offset)?);
            let rows = read_u64(bytes, &mut offset)?;
            let total_bytes = read_u64(bytes, &mut offset)?;
            let version = CommitVersion(read_u64(bytes, &mut offset)?);
            let column_count = read_len(bytes, &mut offset)?;
            let mut columns = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                let column = read_string(bytes, &mut offset)?;
                let distinct_values = read_u64(bytes, &mut offset)?;
                let nulls = read_u64(bytes, &mut offset)?;
                let bucket_count = read_len(bytes, &mut offset)?;
                let mut histogram = Vec::with_capacity(bucket_count);
                for _ in 0..bucket_count {
                    histogram.push(HistogramBucket {
                        value: read_string(bytes, &mut offset)?,
                        rows: read_u64(bytes, &mut offset)?,
                    });
                }
                columns.push(ColumnEstimate {
                    column,
                    distinct_values,
                    nulls,
                    histogram,
                });
            }
            StatsWalRecord::Estimate(TableEstimate {
                table,
                rows,
                total_bytes,
                version,
                columns,
            })
        }
        _ => return Err(corrupt("unknown stats WAL record tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("trailing bytes in stats WAL payload"));
    }
    Ok(record)
}

fn write_string(out: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    let bytes = value.as_bytes();
    write_len(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_len(out: &mut Vec<u8>, value: usize) -> RsfResult<()> {
    let value = u32::try_from(value).map_err(|_| invalid("stats WAL length exceeds u32"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    if *offset >= bytes.len() {
        return Err(corrupt("unexpected end of stats WAL payload"));
    }
    let value = bytes[*offset];
    *offset += 1;
    Ok(value)
}

fn read_len(bytes: &[u8], offset: &mut usize) -> RsfResult<usize> {
    if bytes.len().saturating_sub(*offset) < 4 {
        return Err(corrupt("unexpected end of stats WAL payload"));
    }
    let value = u32::from_le_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
    ]);
    *offset += 4;
    Ok(value as usize)
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    if bytes.len().saturating_sub(*offset) < 8 {
        return Err(corrupt("unexpected end of stats WAL payload"));
    }
    let value = u64::from_le_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
        bytes[*offset + 4],
        bytes[*offset + 5],
        bytes[*offset + 6],
        bytes[*offset + 7],
    ]);
    *offset += 8;
    Ok(value)
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_len(bytes, offset)?;
    if bytes.len().saturating_sub(*offset) < len {
        return Err(corrupt("unexpected end of stats WAL string"));
    }
    let value = std::str::from_utf8(&bytes[*offset..*offset + len])
        .map_err(|_| corrupt("stats WAL string is not valid UTF-8"))?
        .to_string();
    *offset += len;
    Ok(value)
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests {
    use rsf_core::catalog::{ColumnDescriptor, ColumnType, NamespaceName};

    use super::*;

    fn table() -> TableDescriptor {
        TableDescriptor {
            namespace: NamespaceName("app".to_string()),
            name: TableName("users".to_string()),
            columns: vec![
                ColumnDescriptor {
                    name: "id".to_string(),
                    ty: ColumnType::Int64,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "name".to_string(),
                    ty: ColumnType::Text,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "active".to_string(),
                    ty: ColumnType::Bool,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".to_string()],
        }
    }

    fn sample() -> TableSample {
        TableSample {
            table: TableName("users".to_string()),
            rows: 1_024,
            total_bytes: 524_288,
            version: CommitVersion(22),
            columns: vec![
                ColumnSample {
                    column: "name".to_string(),
                    values: vec![
                        ValueSample {
                            value: Some("ada".to_string()),
                            rows: 5,
                        },
                        ValueSample {
                            value: Some("grace".to_string()),
                            rows: 7,
                        },
                    ],
                },
                ColumnSample {
                    column: "active".to_string(),
                    values: vec![
                        ValueSample {
                            value: Some("true".to_string()),
                            rows: 768,
                        },
                        ValueSample {
                            value: Some("false".to_string()),
                            rows: 256,
                        },
                    ],
                },
            ],
        }
    }

    #[test]
    fn publishes_table_estimate_with_histograms() {
        let mut catalog = StatisticsCatalog::default();
        let estimate = catalog.publish(&table(), sample()).unwrap();

        assert_eq!(estimate.rows, 1_024);
        assert_eq!(estimate.average_row_bytes(), 512);
        assert_eq!(estimate.histogram_bucket_count(), 4);
        assert_eq!(catalog.table_count(), 1);
    }

    #[test]
    fn freshness_reports_stale_statistics() {
        let mut catalog = StatisticsCatalog::default();
        catalog.publish(&table(), sample()).unwrap();

        assert_eq!(
            catalog
                .freshness(&TableName("users".to_string()), CommitVersion(25), 2)
                .unwrap(),
            Freshness::Stale { lag: 3 }
        );
    }

    #[test]
    fn freshness_accepts_bounded_lag() {
        let mut catalog = StatisticsCatalog::default();
        catalog.publish(&table(), sample()).unwrap();

        assert_eq!(
            catalog
                .freshness(&TableName("users".to_string()), CommitVersion(24), 2)
                .unwrap(),
            Freshness::Fresh
        );
    }

    #[test]
    fn unknown_sample_column_is_rejected() {
        let mut bad = sample();
        bad.columns[0].column = "missing".to_string();
        let err = StatisticsCatalog::default()
            .publish(&table(), bad)
            .expect_err("unknown column must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn zero_row_sample_is_rejected() {
        let mut bad = sample();
        bad.rows = 0;
        let err = StatisticsCatalog::default()
            .publish(&table(), bad)
            .expect_err("zero rows must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn durable_stats_recovers_estimate_and_freshness() {
        let path = test_path("durable_stats_recovers_estimate_and_freshness");

        {
            let mut catalog =
                DurableStatisticsCatalog::open(&path, Epoch(4)).expect("open stats catalog");
            let estimate = catalog
                .publish(&table(), sample())
                .expect("publish durable stats");
            assert_eq!(estimate.rows, 1_024);
            assert_eq!(catalog.table_count(), 1);
        }

        let recovered =
            DurableStatisticsCatalog::open(&path, Epoch(4)).expect("recover stats catalog");
        let estimate = recovered
            .estimate(&TableName("users".to_string()))
            .expect("recovered estimate");
        assert_eq!(estimate.average_row_bytes(), 512);
        assert_eq!(estimate.histogram_bucket_count(), 4);
        assert_eq!(
            recovered
                .freshness(&TableName("users".to_string()), CommitVersion(25), 2)
                .expect("freshness"),
            Freshness::Stale { lag: 3 }
        );
    }

    #[test]
    fn durable_stats_rejects_invalid_recovered_estimate() {
        let path = test_path("durable_stats_rejects_invalid_recovered_estimate");
        let estimate = StatisticsCatalog::default()
            .publish(&table(), sample())
            .expect("estimate");
        let mut payload =
            encode_stats_wal_payload(&StatsWalRecord::Estimate(estimate)).expect("encode");
        let rows_offset = STATS_WAL_MAGIC.len() + 1 + 1 + 4 + "users".len();
        payload[rows_offset..rows_offset + 8].copy_from_slice(&0_u64.to_le_bytes());

        let mut store = SegmentStore::open(&path).expect("segment store");
        let recovery = store.recover_current().expect("recover").recovery;
        let tail = recovery.pg_tail(STATS_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        store
            .append_wal(WalAppend {
                pg: STATS_PG,
                epoch: Epoch(5),
                index: LogIndex(1),
                kind: WalRecordKind::Stats,
                previous_hash,
                payload: &payload,
            })
            .expect("append invalid estimate");

        let err =
            DurableStatisticsCatalog::open(&path, Epoch(5)).expect_err("replay rejects estimate");
        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn stats_wal_decode_rejects_trailing_bytes() {
        let estimate = StatisticsCatalog::default()
            .publish(&table(), sample())
            .expect("estimate");
        let mut payload =
            encode_stats_wal_payload(&StatsWalRecord::Estimate(estimate)).expect("encode");
        payload.push(1);

        let err = decode_stats_wal_payload(&payload).expect_err("trailing bytes reject");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rsf-stats-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        path
    }
}
