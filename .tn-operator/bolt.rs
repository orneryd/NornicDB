//! NornicDB over Bolt 5.0 with the `neo4j` driver (gate G2; `ledger.py`,
//! `api/dependencies.py`). One driver, with its own connection pool, per login: the
//! service's, or each HTTP caller's own Basic login, which NornicDB itself accepts or
//! refuses. Connections use the configured URI, connection timeout and pool size, and with
//! a private CA bundle encrypt and verify the server's certificate and host name. Sessions
//! name their database (the library or `system`); every result is read to its end before
//! the connection is reused. The driver is synchronous, so callers run it on blocking
//! threads; no retry helper repeats a PD block.

mod errors;
mod values;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use neo4j::driver::auth::AuthToken;
use neo4j::driver::{ConnectionConfig, Driver, DriverConfig, Record as DriverRecord};
use neo4j::session::{Session, SessionConfig};
use neo4j::summary::Summary;
use neo4j::transaction::Transaction;
use pd_contracts::adapters::infra::DependencyFailure;
use pd_contracts::adapters::infra::graph::{
    Counters, Graph, GraphConnector, GraphError, GraphRunner, GraphSession, Params, QueryResult,
    Record, TransactionEnd,
};
use pd_contracts::settings::Settings;

use errors::{driver_error, graph_error, is_refused_login};

const SYSTEM: &str = "system";

/// Opens NornicDB drivers with the deployment's connection settings.
#[derive(Debug, Clone)]
pub struct BoltConnector {
    uri: String,
    database: Arc<String>,
    ca_bundle: Option<PathBuf>,
    connection_timeout: Duration,
    pool_size: usize,
}

impl BoltConnector {
    pub fn new(settings: &Settings) -> Result<Self, GraphError> {
        let timeout = u64::try_from(settings.nornic_timeout)
            .map_err(|_| driver_error("PD_NORNIC_TIMEOUT must not be negative".into()))?;
        let pool_size = usize::try_from(settings.nornic_pool_size)
            .map_err(|_| driver_error("PD_NORNIC_POOL_SIZE must not be negative".into()))?;
        Ok(Self {
            uri: settings.nornic_uri.clone(),
            database: Arc::new(settings.nornic_database.clone()),
            ca_bundle: settings.tls_ca_bundle.clone(),
            connection_timeout: Duration::from_secs(timeout),
            pool_size,
        })
    }

    /// A driver for this login. It connects on first use.
    pub fn open(&self, username: &str, password: &str) -> Result<BoltGraph, GraphError> {
        Ok(BoltGraph {
            driver: self.driver(username, password)?,
            database: Arc::clone(&self.database),
            uri: self.uri.clone(),
        })
    }

    fn driver(&self, username: &str, password: &str) -> Result<Driver, GraphError> {
        let mut connection = ConnectionConfig::try_from(self.uri.as_str())
            .map_err(|error| driver_error(error.to_string()))?;
        if let Some(bundle) = &self.ca_bundle {
            connection = connection
                .with_encryption_trust_custom_cas(&[bundle])
                .map_err(|error| driver_error(error.to_string()))?;
        }
        let config = DriverConfig::new()
            .with_auth(Arc::new(AuthToken::new_basic_auth(username, password)))
            .with_connection_timeout(self.connection_timeout)
            .with_max_connection_pool_size(self.pool_size);
        Ok(Driver::new(connection, config))
    }
}

impl GraphConnector for BoltConnector {
    fn connect(&self, username: &str, password: &str) -> Result<Box<dyn Graph>, GraphError> {
        Ok(Box::new(self.open(username, password)?))
    }

    /// Bolt 5.0 cannot re-authenticate a connection, so the check opens one with the login
    /// on a driver of its own (as the Python driver does below Bolt 5.1).
    fn verify_authentication(&self, username: &str, password: &str) -> Result<bool, GraphError> {
        match self.driver(username, password)?.verify_connectivity() {
            Ok(()) => Ok(true),
            Err(error) if is_refused_login(&error) => Ok(false),
            Err(error) => Err(graph_error(error, &self.uri)),
        }
    }
}

/// One login's driver and connection pool; closing or dropping it closes the connections.
pub struct BoltGraph {
    driver: Driver,
    database: Arc<String>,
    uri: String,
}

impl BoltGraph {
    /// The Bolt protocol version negotiated with the server.
    pub fn protocol_version(&self) -> Result<(u8, u8), GraphError> {
        self.driver
            .get_server_info()
            .map(|info| info.protocol_version)
            .map_err(|error| graph_error(error, &self.uri))
    }

    fn session_on(&self, database: Arc<String>) -> Box<dyn GraphSession + '_> {
        Box::new(BoltSession {
            session: self
                .driver
                .session(SessionConfig::new().with_database(database)),
            uri: &self.uri,
        })
    }
}

impl Graph for BoltGraph {
    fn database(&self) -> &str {
        &self.database
    }

    fn session(&self) -> Result<Box<dyn GraphSession + '_>, GraphError> {
        Ok(self.session_on(Arc::clone(&self.database)))
    }

    fn system_session(&self) -> Result<Box<dyn GraphSession + '_>, GraphError> {
        Ok(self.session_on(Arc::new(SYSTEM.to_owned())))
    }

    fn verify_connectivity(&self) -> Result<(), DependencyFailure> {
        self.driver
            .verify_connectivity()
            .map_err(|error| DependencyFailure {
                name: graph_error(error, &self.uri).kind.name().into(),
            })
    }

    /// Dropping the driver closes its pool's connections.
    fn close(self: Box<Self>) {}
}

struct BoltSession<'driver> {
    session: Session<'driver>,
    uri: &'driver str,
}

impl GraphRunner for BoltSession<'_> {
    // The driver's receiver signature fixes its closure's error type.
    #[allow(clippy::result_large_err)]
    fn run(&mut self, query: &str, params: &Params) -> Result<QueryResult, GraphError> {
        r17_record_query(query, || {
        let parameters = values::parameters(params)?;
        let (records, summary) = self
            .session
            .auto_commit(query)
            .with_parameters(parameters)
            .with_receiver(|stream| {
                let records = stream.by_ref().collect::<neo4j::Result<Vec<_>>>()?;
                Ok((records, stream.consume()?))
            })
            .run()
            .map_err(|error| graph_error(error, self.uri))?;
        query_result(records, summary)
        })
    }
}

impl GraphSession for BoltSession<'_> {
    // The driver's receiver signature fixes its closure's error type.
    #[allow(clippy::result_large_err)]
    fn transaction(
        &mut self,
        work: &mut dyn FnMut(&mut dyn GraphRunner) -> TransactionEnd,
    ) -> Result<(), GraphError> {
        self.session
            .transaction()
            .run(|transaction| {
                let end = work(&mut TransactionRunner {
                    transaction: &transaction,
                    uri: self.uri,
                });
                match end {
                    TransactionEnd::Commit => transaction.commit(),
                    TransactionEnd::Rollback => transaction.rollback(),
                }
            })
            .map_err(|error| graph_error(error, self.uri))
    }
}

struct TransactionRunner<'tx, 'driver> {
    transaction: &'tx Transaction<'driver, 'tx>,
    uri: &'tx str,
}

impl GraphRunner for TransactionRunner<'_, '_> {
    fn run(&mut self, query: &str, params: &Params) -> Result<QueryResult, GraphError> {
        r17_record_query(query, || {
        let parameters = values::parameters(params)?;
        let mut stream = self
            .transaction
            .query(query)
            .with_parameters(parameters)
            .run()
            .map_err(|error| graph_error(error, self.uri))?;
        let records = stream
            .by_ref()
            .collect::<neo4j::Result<Vec<_>>>()
            .map_err(|error| graph_error(error, self.uri))?;
        let summary = stream
            .consume()
            .map_err(|error| graph_error(error, self.uri))?;
        query_result(records, summary)
        })
    }
}

fn query_result(
    records: Vec<DriverRecord>,
    summary: Option<Summary>,
) -> Result<QueryResult, GraphError> {
    let records = records
        .into_iter()
        .map(|record| {
            let (keys, values): (Vec<_>, Vec<_>) = record.into_entries().unzip();
            Ok(Record {
                keys: keys.iter().map(|key| key.as_str().to_owned()).collect(),
                values: values
                    .into_iter()
                    .map(values::receive)
                    .collect::<Result<_, _>>()?,
            })
        })
        .collect::<Result<_, GraphError>>()?;
    Ok(QueryResult {
        records,
        counters: summary
            .map(|summary| counters(&summary))
            .unwrap_or_default(),
    })
}

fn counters(summary: &Summary) -> Counters {
    let counters = &summary.counters;
    Counters {
        nodes_created: counters.nodes_created,
        nodes_deleted: counters.nodes_deleted,
        relationships_created: counters.relationships_created,
        relationships_deleted: counters.relationships_deleted,
        properties_set: counters.properties_set,
        labels_added: counters.labels_added,
        labels_removed: counters.labels_removed,
        indexes_added: counters.indexes_added,
        indexes_removed: counters.indexes_removed,
        constraints_added: counters.constraints_added,
        constraints_removed: counters.constraints_removed,
        system_updates: counters.system_updates,
    }
}

// Private R17 diagnostic instrumentation; query text/parameters and driver control flow
// are unchanged. Duration covers parameter conversion through complete result consumption.
// Trace serialization and I/O happen afterward and are not included in duration_ns.
fn r17_record_query(query: &str, run: impl FnOnce() -> Result<QueryResult, GraphError>) -> Result<QueryResult, GraphError> {
    use sha2::{Digest, Sha256};
    use std::io::Write;
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let started_at_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock").as_nanos();
    let started = std::time::Instant::now();
    let result = run();
    let elapsed = started.elapsed().as_nanos();
    let row_count = result.as_ref().ok().map(|r| r.records.len());
    let value = serde_json::json!({"pid":std::process::id(), "sequence":SEQ.fetch_add(1,std::sync::atomic::Ordering::Relaxed),
        "query_sha256":format!("{:x}",Sha256::digest(query.as_bytes())),"query":query,
        "started_at_unix_ns":started_at_ns,"duration_ns":elapsed,"rows":row_count,"ok":result.is_ok(),
        "scope":"client parameter conversion + Bolt result consumption; not engine-only time"});
    let folder = std::env::var_os("PD_QUERY_TRACE_DIR").expect("R17 query trace directory required");
    let _guard = LOCK.lock().expect("query log lock");
    let path = std::path::Path::new(&folder).join(format!("queries-{}.jsonl",std::process::id()));
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path).expect("query trace");
    writeln!(file,"{}",value).expect("persist query trace");
    result
}
