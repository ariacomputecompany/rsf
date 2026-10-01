//! Explicit local format migrations, separate from cluster membership changes.
use rsf_core::{Epoch, NodeId, PlacementGroupId};
use std::collections::BTreeMap;

pub(crate) fn run_cli(args: &[String]) -> Result<(), String> {
    const USAGE: &str = "usage: rsfd vsr migrate-bootstrap --store <cell-dir> --pg <id> --original-view <epoch> --original-primary <node> --original-voters <node,node,...>";
    if args.first().map(String::as_str) != Some("migrate-bootstrap") || args.len() != 11 {
        return Err(USAGE.into());
    }
    let mut flags = BTreeMap::new();
    for pair in args[1..].chunks_exact(2) {
        if ![
            "--store",
            "--pg",
            "--original-view",
            "--original-primary",
            "--original-voters",
        ]
        .contains(&pair[0].as_str())
            || flags.insert(pair[0].as_str(), pair[1].as_str()).is_some()
        {
            return Err(USAGE.into());
        }
    }
    let number = |flag: &str| {
        flags[flag]
            .parse::<u64>()
            .map_err(|_| format!("{flag} must be an unsigned integer"))
    };
    let pg = PlacementGroupId(number("--pg")?);
    let view = Epoch(number("--original-view")?);
    let primary = NodeId(number("--original-primary")?);
    let voters = flags["--original-voters"]
        .split(',')
        .map(|value| {
            value
                .parse::<u64>()
                .map(NodeId)
                .map_err(|_| "--original-voters must contain comma-separated node IDs".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let changed = rsf_vsr::DurableVsrCell::migrate_legacy_bootstrap(
        flags["--store"],
        pg,
        view,
        primary,
        voters,
    )
    .map_err(|err| err.message)?;
    println!(
        "{}",
        serde_json::json!({"event":"vsr.bootstrap-migration", "pg":pg.0, "changed":changed, "original_view":view.0, "original_primary":primary.0})
    );
    Ok(())
}
