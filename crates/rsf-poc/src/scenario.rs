use crate::model::{DatabaseSurface, Implementation, ImplementationId, Topology};

pub fn implementations() -> Vec<Implementation> {
    vec![
        Implementation {
            id: ImplementationId::UniversalVsrRados,
            name: "Universal VSR-RADOS Fabric",
            confidence: 91,
            topology: Topology {
                nodes: 5,
                regions: &["iad", "ord"],
                placement: "RADOS-style placement groups with VSR metadata cells",
                consensus: "Viewstamped Replication",
            },
            database: DatabaseSurface {
                relational: "ordered KV-backed tables and indexes",
                object: "strict metadata plus multipart manifests",
                blob: "content-addressed chunks",
                kv: "MVCC ordered keys",
            },
        },
        Implementation {
            id: ImplementationId::FoundationDbExtent,
            name: "FoundationDB Over Extent Fabric",
            confidence: 90,
            topology: Topology {
                nodes: 6,
                regions: &["iad", "ord"],
                placement: "extent groups below sequencer/proxy/resolver/storage roles",
                consensus: "strict transaction services over replicated logs",
            },
            database: DatabaseSurface {
                relational: "SQL layer encoded into transactional tuples",
                object: "object metadata in ordered keys",
                blob: "immutable extents for bodies and snapshots",
                kv: "native ordered transactional KV",
            },
        },
        Implementation {
            id: ImplementationId::AuroraSocrates,
            name: "Aurora/Socrates Relational-First Fabric",
            confidence: 88,
            topology: Topology {
                nodes: 6,
                regions: &["iad"],
                placement: "protection groups for page/log storage",
                consensus: "quorum log durability with page-server replay",
            },
            database: DatabaseSurface {
                relational: "page/WAL-native relational engine",
                object: "manifest layer over protection groups",
                blob: "long-term chunk store",
                kv: "catalog and metadata KV",
            },
        },
        Implementation {
            id: ImplementationId::GeoLocalWpaxos,
            name: "Geo-Local WPaxos Fabric",
            confidence: 86,
            topology: Topology {
                nodes: 7,
                regions: &["iad", "sfo", "fra"],
                placement: "ownership moves toward active writers",
                consensus: "locality-adaptive Paxos ownership",
            },
            database: DatabaseSurface {
                relational: "partitioned relational ranges with movable ownership",
                object: "metadata heads owned near writers",
                blob: "global immutable chunk cache",
                kv: "geo-local ordered ranges",
            },
        },
        Implementation {
            id: ImplementationId::DualModeCpAp,
            name: "Dual-Mode CP/AP Fabric",
            confidence: 87,
            topology: Topology {
                nodes: 5,
                regions: &["iad", "sfo"],
                placement: "CP placement groups plus AP edge rings",
                consensus: "strict quorum for truth, Dynamo-style AP for edge state",
            },
            database: DatabaseSurface {
                relational: "CP-only transactions and catalog",
                object: "strict manifests with AP cache invalidations",
                blob: "immutable replicated chunks",
                kv: "CP namespaces and AP CRDT namespaces",
            },
        },
    ]
}
