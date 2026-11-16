//! Bounded, real-process load qualification. The regular suite exercises the
//! failure edges; this workload checks them again with a growing durable log,
//! concurrent TLS clients and repeated abrupt shutdowns of every process.
use super::*;
use std::time::Instant;

#[derive(Debug)]
struct Acknowledged {
    key: String,
    request: String,
    value: Vec<u8>,
    response: String,
    micros: u128,
}

#[test]
#[ignore = "real-process stress workload; run through tests/stress-host.pass.fozzy.json"]
fn native_kv_stress_preserves_acknowledged_writes_across_restarts() {
    const CLIENTS: usize = 4;
    const ROUNDS: usize = 3;
    let writes = std::env::var("RSF_STRESS_WRITES_PER_CLIENT")
        .map(|value| value.parse::<usize>().expect("positive stress write count"))
        .unwrap_or(16);
    assert!((1..=4096).contains(&writes));
    let root = root();
    let mut peers = provision(&root);
    let state = initialize_network_state(&root, &peers);
    let mut acknowledged = Vec::<Acknowledged>::new();
    let started = Instant::now();
    let mut write_micros = 0;

    for round in 0..ROUNDS {
        let first = Process::start(&root.join("peer-1.json"));
        let second = Process::start(&root.join("peer-2.json"));
        peers.get_mut(&NodeId(1)).unwrap().address = first.address;
        peers.get_mut(&NodeId(2)).unwrap().address = second.address;
        configure_primary(&root, &state, &peers);
        let primary = Process::primary(&state);

        // Recovered receipts preserve the original result and explicitly report
        // replay. That diagnostic is expected to differ from the first reply.
        for prior in &acknowledged {
            let response = kv_request(
                primary.address,
                &format!("SET {}", prior.key),
                &prior.request,
                &prior.value,
            );
            assert!(response.contains("replayed=true"), "{response}");
            assert_eq!(
                response.replace("replayed=true", "replayed=false"),
                prior.response.replace("replayed=true", "replayed=false"),
                "retry diverged for {}",
                prior.key
            );
        }

        let write_started = Instant::now();
        let address = primary.address;
        let completed = std::thread::scope(|scope| {
            let workers = (0..CLIENTS)
                .map(|client| {
                    scope.spawn(move || {
                        (0..writes)
                            .map(|index| {
                                let key = format!("stress/{round}/{client}/{index}");
                                let request = format!("stress-{round}-{client}-{index}");
                                let value = (0..512 + index % 8 * 512)
                                    .map(|offset| (offset ^ index ^ client ^ round) as u8)
                                    .collect::<Vec<_>>();
                                let start = Instant::now();
                                let response =
                                    kv_request(address, &format!("SET {key}"), &request, &value);
                                let micros = start.elapsed().as_micros();
                                assert!(response.contains("RSF/1 executed"), "{key}: {response}");
                                Acknowledged {
                                    key,
                                    request,
                                    value,
                                    response,
                                    micros,
                                }
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();
            workers
                .into_iter()
                .flat_map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        write_micros += write_started.elapsed().as_micros();
        acknowledged.extend(completed);

        // Minority loss must leave writes available. Losing the remaining peer
        // must reject both a fresh write and a previously successful retry.
        drop(second);
        let key = format!("stress/{round}/minority");
        let request = format!("minority-{round}");
        let value = b"survives minority process loss".to_vec();
        let start = Instant::now();
        let response = kv_request(primary.address, &format!("SET {key}"), &request, &value);
        assert!(response.contains("RSF/1 executed"), "{response}");
        let minority = Acknowledged {
            key,
            request,
            value,
            response,
            micros: start.elapsed().as_micros(),
        };
        drop(first);
        let uncertain_key = format!("stress/{round}/no-quorum");
        let uncertain_request = format!("no-quorum-{round}");
        let uncertain_value = b"resolve using the original request identity";
        let rejected = kv_request(
            primary.address,
            &format!("SET {uncertain_key}"),
            &uncertain_request,
            uncertain_value,
        );
        assert!(rejected.contains("RSF/1 rejected"), "{rejected}");
        assert!(rejected.contains("mutation=unknown"), "{rejected}");
        let retry = kv_request(
            primary.address,
            &format!("SET {}", minority.key),
            &minority.request,
            &minority.value,
        );
        assert!(retry.contains("RSF/1 rejected"), "{retry}");
        drop(primary);

        // Inspect independent durable stores while no process owns them. The
        // stopped minority may lag exactly the last acknowledged write.
        for node in 1..3 {
            let follower = follower_kv(&root, node);
            for prior in &acknowledged {
                assert_eq!(
                    follower.get(prior.key.as_bytes()).unwrap().value,
                    prior.value,
                    "node {node} lost acknowledged key {}",
                    prior.key
                );
            }
            assert_eq!(
                follower.changes_since(rsf_core::CommitVersion(0)).len(),
                acknowledged.len() + usize::from(node == 1)
            );
            assert!(
                follower
                    .get(format!("stress/{round}/no-quorum").as_bytes())
                    .is_none()
            );
            if node == 1 {
                assert_eq!(
                    follower.get(minority.key.as_bytes()).unwrap().value,
                    minority.value
                );
            }
        }
        acknowledged.push(minority);

        // A failed acknowledgement is not an abort. Restore quorum and retry
        // the identical request, then prove every accepted value exists exactly
        // once on both independent stores, including the formerly uncertain one.
        let first = Process::start(&root.join("peer-1.json"));
        let second = Process::start(&root.join("peer-2.json"));
        peers.get_mut(&NodeId(1)).unwrap().address = first.address;
        peers.get_mut(&NodeId(2)).unwrap().address = second.address;
        configure_primary(&root, &state, &peers);
        let primary = Process::primary(&state);
        let start = Instant::now();
        let resolved = kv_request(
            primary.address,
            &format!("SET {uncertain_key}"),
            &uncertain_request,
            uncertain_value,
        );
        assert!(resolved.contains("RSF/1 executed"), "{resolved}");
        let micros = start.elapsed().as_micros();
        let repeated = kv_request(
            primary.address,
            &format!("SET {uncertain_key}"),
            &uncertain_request,
            uncertain_value,
        );
        assert!(repeated.contains("replayed=true"), "{repeated}");
        assert_eq!(
            repeated.replace("replayed=true", "replayed=false"),
            resolved.replace("replayed=true", "replayed=false")
        );
        acknowledged.push(Acknowledged {
            key: uncertain_key,
            request: uncertain_request,
            value: uncertain_value.to_vec(),
            response: resolved,
            micros,
        });
        drop(primary);
        drop(first);
        drop(second);
        for node in 1..3 {
            let follower = follower_kv(&root, node);
            for prior in &acknowledged {
                assert_eq!(
                    follower.get(prior.key.as_bytes()).unwrap().value,
                    prior.value
                );
            }
            assert_eq!(
                follower.changes_since(rsf_core::CommitVersion(0)).len(),
                acknowledged.len()
            );
        }
    }

    let mut samples = acknowledged
        .iter()
        .map(|entry| entry.micros)
        .collect::<Vec<_>>();
    samples.sort_unstable();
    let percentile =
        |percent: usize| samples[(samples.len() * percent).div_ceil(100).saturating_sub(1)];
    println!(
        "{}",
        serde_json::json!({
            "workload":"native-kv-process-stress", "clients":CLIENTS, "rounds":ROUNDS,
        "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "acknowledged":acknowledged.len(), "payload_bytes":acknowledged.iter().map(|entry|entry.value.len()).sum::<usize>(),
            "concurrent_write_ops_per_second": (CLIENTS * writes * ROUNDS) as f64 * 1_000_000.0 / write_micros.max(1) as f64,
            "latency_us":{"p50":percentile(50),"p95":percentile(95),"p99":percentile(99),"max":samples.last()},
            "elapsed_ms":started.elapsed().as_millis(), "abrupt_process_shutdowns":ROUNDS * 6,
            "uncertain_writes_resolved":ROUNDS,
            "latency_scope":"acknowledged writes and uncertain-write retry resolutions",
            "scope":"single host loopback TLS with fsync; hardware/WAN and capacity qualification remain separate"
        })
    );
    std::fs::remove_dir_all(root).unwrap();
}
