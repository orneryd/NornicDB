//! A fresh disposable stock NornicDB server for one test, started from `PD_NATIVE_RUNNER`
//! with the deployment's native settings (`deploy/truenas/nornic.json`), as the Python
//! fixture `tests/foundation/test_native_bolt.py::native` does: ambient `NORNICDB_*`
//! settings are discarded, endpoints become loopback, Bolt and HTTP are plain, the store is
//! disposable and embedding scans are faster. `PD_NATIVE_PARSER` (`default` or `antlr`)
//! selects the parser.
//!
//! Provider requests go to `provider_url`. `start()` points them at a closed loopback port, so a
//! test that writes searchable nodes starts [`crate::provider::Provider`] and uses
//! `start_with(runner, &provider.url)`.
//! `restart()` closes this fixture's HTTP pool, gracefully stops the engine (10 s),
//! then starts the same command, environment, ports and store and waits ready (30 s).
//! Callers close their own Bolt graphs before restarting and reconnect afterward.
//! Each job must name its binary and expected commit/SHA-256. Identity is verified
//! before the server starts; missing or mismatched settings fail the test.
//! `PD_NATIVE_STORE_IN` restores a saved `database/` TAR before starting the server.

use std::collections::BTreeMap;
use std::fs::File;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use base64::Engine as _;
use rustix::process::{Pid, Signal, kill_process};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const DATABASE: &str = "library";
const USERNAME: &str = "fixture";
const PASSWORD: &str = "fixture-password";

/// The fixture's changes to the deployment settings (Python `FIXTURE_ENVIRONMENT`).
const FIXTURE_ENVIRONMENT: [(&str, &str); 11] = [
    ("NORNICDB_TELEMETRY_LISTEN", "127.0.0.1:0"),
    ("NORNICDB_DEFAULT_DATABASE", DATABASE),
    ("NORNICDB_HTTP_ADDRESS", "127.0.0.1"),
    ("NORNICDB_HTTP_ENABLED", "true"),
    ("NORNICDB_HTTPS_ENABLED", "false"),
    ("NORNICDB_BOLT_ADDRESS", "127.0.0.1"),
    ("NORNICDB_BOLT_TLS_ENABLED", "false"),
    ("NORNICDB_BOLT_TLS_REQUIRE", "false"),
    ("NORNICDB_HEADLESS", "true"),
    ("NORNICDB_EMBED_SCAN_INTERVAL", "50ms"),
    ("NORNICDB_EMBED_BATCH_DELAY", "1ms"),
];

pub struct Native {
    pub bolt_uri: String,
    pub http_base: String,
    pub username: &'static str,
    pub password: &'static str,
    pub log_path: PathBuf,
    process: Child,
    command: Command,
    agent: ureq::Agent,
    // Dropped last: the store and log live here.
    dir: tempfile::TempDir,
}

/// The repository root as the test sees it (`CARGO_MANIFEST_DIR` at run time, so a nextest
/// archive run with `--workspace-remap` finds the extracted source).
pub fn repository_root() -> PathBuf {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set");
    Path::new(&manifest).join("../../..")
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a loopback port");
    listener.local_addr().expect("loopback address").port()
}

fn deployment_environment() -> BTreeMap<String, String> {
    let path = repository_root().join("deploy/truenas/nornic.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
    let deployment: Value = serde_json::from_str(&text).expect("nornic.json is JSON");
    deployment["environment"]
        .as_object()
        .expect("nornic.json environment")
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().expect("string setting").to_owned()))
        .collect()
}

/// The native YAML secrets the deployment mounts (JSON is YAML).
fn write_secrets(dir: &Path) -> PathBuf {
    let path = dir.join("nornicdb.yaml");
    let secrets = json!({
        "auth": {"enabled": true, "username": USERNAME, "password": PASSWORD,
                 "jwt_secret": "fixture-jwt-signing-secret-long-enough"},
        "embedding": {"api_key": "offline-fixture-key"},
        "search_rerank": {"api_key": "offline-fixture-key"},
    });
    std::fs::write(&path, secrets.to_string()).expect("write native secrets");
    path
}

fn verify_runner(runner: &Path) {
    let selected = std::env::var_os("PD_NATIVE_RUNNER").expect("job must set PD_NATIVE_RUNNER");
    assert_eq!(runner, Path::new(&selected), "job-selected native binary");
    let commit =
        std::env::var("PD_EXPECTED_ENGINE_COMMIT").expect("job must set PD_EXPECTED_ENGINE_COMMIT");
    let expected =
        std::env::var("PD_EXPECTED_BINARY_SHA256").expect("job must set PD_EXPECTED_BINARY_SHA256");
    let bytes = std::fs::read(runner)
        .unwrap_or_else(|error| panic!("read native binary {runner:?}: {error}"));
    let actual = format!("{:x}", Sha256::digest(bytes));
    eprintln!(
        "PD_NATIVE_ENGINE {}",
        json!({"source":commit,"runner":runner,
        "binary_sha256":actual,"expected_binary_sha256":expected,"binary_matches":actual==expected})
    );
    assert_eq!(
        actual, expected,
        "native binary SHA-256 differs from job identity"
    );
}

fn extract_store(store: &Path, dir: &Path) {
    let listing = Command::new("tar")
        .arg("-tf")
        .arg(store)
        .output()
        .unwrap_or_else(|e| panic!("list {store:?}: {e}"));
    assert!(
        listing.status.success(),
        "list {store:?}: {}",
        listing.status
    );
    for entry in String::from_utf8_lossy(&listing.stdout).lines() {
        assert!(
            !entry.starts_with('/') && !entry.split('/').any(|part| part == ".."),
            "{store:?}: entry outside the fixture directory: {entry}"
        );
    }
    let status = Command::new("tar")
        .arg("-xf")
        .arg(store)
        .arg("-C")
        .arg(dir)
        .status()
        .unwrap_or_else(|e| panic!("extract {store:?}: {e}"));
    assert!(status.success(), "extract {store:?}: {status}");
}

impl Native {
    /// Starts the job-selected server; missing required engine settings fail visibly.
    pub fn start() -> Option<Self> {
        let runner = std::env::var_os("PD_NATIVE_RUNNER").expect("job must set PD_NATIVE_RUNNER");
        let provider = format!("http://127.0.0.1:{}", free_port());
        Some(Self::start_with(Path::new(&runner), &provider))
    }

    /// Starts a server whose Bolt listener requires TLS with this certificate and key (the
    /// Python fixture's TLS mode); `bolt_uri` names `localhost`, the certificate's host.
    pub fn start_tls(certificate: &Path, key: &Path) -> Option<Self> {
        let runner = std::env::var_os("PD_NATIVE_RUNNER").expect("job must set PD_NATIVE_RUNNER");
        let provider = format!("http://127.0.0.1:{}", free_port());
        Some(Self::launch(
            Path::new(&runner),
            &provider,
            Some((certificate, key)),
        ))
    }

    pub fn start_with(runner: &Path, provider_url: &str) -> Self {
        Self::launch(runner, provider_url, None)
    }

    /// As `start_with`, but the first request waits for the engine's `server.embedding.ready`
    /// event, so a measurement sees an initialised embedder (the K15 quality fixture).
    pub fn start_with_ready_embedder(runner: &Path, provider_url: &str) -> Self {
        Self::launch_ready(runner, provider_url, None, true)
    }

    fn launch(runner: &Path, provider_url: &str, tls: Option<(&Path, &Path)>) -> Self {
        Self::launch_ready(runner, provider_url, tls, false)
    }

    fn launch_ready(
        runner: &Path,
        provider_url: &str,
        tls: Option<(&Path, &Path)>,
        embedder: bool,
    ) -> Self {
        verify_runner(runner);
        let dir = tempfile::tempdir().expect("native fixture directory");
        if let Some(store) = std::env::var_os("PD_NATIVE_STORE_IN").filter(|s| !s.is_empty()) {
            extract_store(Path::new(&store), dir.path());
        }
        let reservations: [TcpListener; 2] = std::array::from_fn(|_| {
            TcpListener::bind("127.0.0.1:0").expect("reserve a native loopback port")
        });
        let [bolt_port, http_port] = reservations.each_ref().map(|listener| {
            listener
                .local_addr()
                .expect("reserved loopback address")
                .port()
        });
        let mut env = deployment_environment();
        env.extend(FIXTURE_ENVIRONMENT.map(|(k, v)| (k.to_owned(), v.to_owned())));
        let parser = std::env::var("PD_NATIVE_PARSER").unwrap_or_else(|_| "default".into());
        assert!(
            parser == "default" || parser == "antlr",
            "PD_NATIVE_PARSER must be default or antlr"
        );
        if parser == "antlr" {
            env.insert("NORNICDB_PARSER".into(), parser);
        } else {
            env.remove("NORNICDB_PARSER");
        }
        let secrets = write_secrets(dir.path());
        for (key, value) in [
            ("NORNICDB_CONFIG", secrets.display().to_string()),
            (
                "NORNICDB_DATA_DIR",
                dir.path().join("database").display().to_string(),
            ),
            ("NORNICDB_BOLT_PORT", bolt_port.to_string()),
            ("NORNICDB_HTTP_PORT", http_port.to_string()),
            ("NORNICDB_EMBEDDING_API_URL", provider_url.to_owned()),
            ("NORNICDB_SEARCH_RERANK_API_URL", provider_url.to_owned()),
        ] {
            env.insert(key.into(), value);
        }
        let mut bolt_host = "127.0.0.1";
        if let Some((certificate, key)) = tls {
            for (name, value) in [
                ("NORNICDB_BOLT_TLS_ENABLED", "true".into()),
                ("NORNICDB_BOLT_TLS_REQUIRE", "true".into()),
                ("NORNICDB_BOLT_TLS_CERT", certificate.display().to_string()),
                ("NORNICDB_BOLT_TLS_KEY", key.display().to_string()),
            ] {
                env.insert(name.into(), value);
            }
            bolt_host = "localhost";
        }
        // Private R17 diagnostic fixture: preserve logs even when TempDir drops.
        env.insert("NORNICDB_QUERY_LOG_ENABLED".into(), "true".into());
        env.insert("NORNICDB_SLOW_QUERY_THRESHOLD".into(), "1ns".into());
        env.insert("NORNICDB_LOG_LEVEL".into(), "DEBUG".into());
        let retained = std::env::var_os("PD_NATIVE_LOG_DIR").expect("R17 log directory required");
        let retained = PathBuf::from(retained);
        std::fs::create_dir_all(&retained).expect("retained engine log directory");
        let log_path = retained.join(format!("{}.log", dir.path().file_name().expect("fixture name").to_string_lossy()));
        let log = File::create(&log_path).expect("native log");
        let mut command = Command::new(runner);
        command.arg("serve").current_dir(dir.path());
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("NORNICDB_") {
                command.env_remove(key);
            }
        }
        command
            .envs(&env)
            .stdout(log.try_clone().expect("native log handle"))
            .stderr(Stdio::from(log));
        drop(reservations);
        let process = command
            .spawn()
            .unwrap_or_else(|e| panic!("start {runner:?}: {e}"));
        let mut native = Self {
            bolt_uri: format!("bolt://{bolt_host}:{bolt_port}"),
            http_base: format!("http://127.0.0.1:{http_port}"),
            username: USERNAME,
            password: PASSWORD,
            log_path,
            process,
            command,
            agent: ureq::Agent::new_with_defaults(),
            dir,
        };
        if embedder {
            native.wait_embedder();
        }
        native.wait_ready();
        native
    }

    fn wait_embedder(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !String::from_utf8_lossy(&std::fs::read(&self.log_path).unwrap_or_default())
            .contains("\"event_id\":\"server.embedding.ready\"")
        {
            let exited = self
                .process
                .try_wait()
                .expect("native process state")
                .is_some();
            assert!(
                !exited && Instant::now() < deadline,
                "native embedder did not become ready; inspect its retained local log"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Restart this exact engine/store; the caller first closes its Bolt graphs.
    pub fn restart(&mut self) {
        self.agent = ureq::Agent::new_with_defaults();
        // Python's `process.terminate()`: SIGTERM to the engine itself (the runner image has no
        // `kill` program, and `Child::kill` would send SIGKILL).
        let pid = i32::try_from(self.process.id())
            .ok()
            .and_then(Pid::from_raw)
            .expect("native engine pid");
        kill_process(pid, Signal::TERM).expect("terminate native engine");
        let deadline = Instant::now() + Duration::from_secs(10);
        while self
            .process
            .try_wait()
            .expect("native process state")
            .is_none()
        {
            assert!(
                Instant::now() < deadline,
                "native engine did not stop in 10 s"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        self.process = self.command.spawn().expect("restart native engine");
        self.wait_ready();
    }

    fn wait_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(rows) = self.query("RETURN 1 AS ready") {
                assert_eq!(rows, vec![vec![json!(1)]]);
                return;
            }
            let exited = self
                .process
                .try_wait()
                .expect("native process state")
                .is_some();
            if exited || Instant::now() > deadline {
                panic!(
                    "{}",
                    std::fs::read_to_string(&self.log_path).unwrap_or_default()
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Runs one statement in an implicit transaction over NornicDB's HTTP transaction API
    /// and returns its rows; a Cypher error is returned as the server's error objects.
    pub fn query(&self, statement: &str) -> Result<Vec<Vec<Value>>, String> {
        let credentials = base64::engine::general_purpose::STANDARD
            .encode(format!("{}:{}", self.username, self.password));
        let body = json!({"statements": [{"statement": statement}]});
        let mut response = self
            .agent
            .post(format!("{}/db/{DATABASE}/tx/commit", self.http_base))
            .header("Authorization", format!("Basic {credentials}"))
            .send_json(&body)
            .map_err(|e| e.to_string())?;
        let answer: Value = response.body_mut().read_json().map_err(|e| e.to_string())?;
        if answer["errors"]
            .as_array()
            .is_some_and(|errors| !errors.is_empty())
        {
            return Err(answer["errors"].to_string());
        }
        let rows = answer["results"][0]["data"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        Ok(rows
            .iter()
            .map(|row| row["row"].as_array().cloned().unwrap_or_default())
            .collect())
    }

    pub fn directory(&self) -> &Path {
        self.dir.path()
    }
}

impl Drop for Native {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_rejects_missing_or_mismatched_engine_identity() {
        const CHILD: &str = "PD_ENGINE_GUARD_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let _ = Native::start();
            panic!("invalid native engine identity was accepted");
        }
        let executable = std::env::current_exe().expect("native test executable");
        for (setting, value, expected) in [
            ("PD_NATIVE_RUNNER", None, "job must set PD_NATIVE_RUNNER"),
            (
                "PD_EXPECTED_ENGINE_COMMIT",
                None,
                "job must set PD_EXPECTED_ENGINE_COMMIT",
            ),
            (
                "PD_EXPECTED_BINARY_SHA256",
                None,
                "job must set PD_EXPECTED_BINARY_SHA256",
            ),
            (
                "PD_EXPECTED_BINARY_SHA256",
                Some("incorrect"),
                "native binary SHA-256 differs from job identity",
            ),
        ] {
            let mut command = Command::new(&executable);
            command.args([
                "--exact",
                "native::tests::native_rejects_missing_or_mismatched_engine_identity",
                "--nocapture",
            ]);
            command.env(CHILD, "1");
            if let Some(value) = value {
                command.env(setting, value);
            } else {
                command.env_remove(setting);
            }
            let result = command.output().expect("engine identity rejection child");
            let stderr = String::from_utf8(result.stderr).expect("rejection stderr");
            assert!(!result.status.success(), "invalid {setting} passed");
            assert!(
                stderr.contains(expected),
                "unexpected {setting} failure: {stderr}"
            );
            eprintln!("PD_NATIVE_IDENTITY_REJECTED {setting}={value:?}: {expected}");
        }
    }

    #[test]
    fn launched_native_keeps_deployment_durability_settings() {
        let native = Native::start().expect("job-selected native engine");
        let environment: BTreeMap<_, _> = native
            .command
            .get_envs()
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect();
        for (key, expected) in [
            ("NORNICDB_ASYNC_WRITES_ENABLED", "false"),
            ("NORNICDB_STRICT_DURABILITY", "true"),
            ("NORNICDB_WAL_SYNC_MODE", "immediate"),
        ] {
            assert_eq!(
                environment.get(std::ffi::OsStr::new(key)).copied(),
                Some(std::ffi::OsStr::new(expected)),
                "launched native setting {key}"
            );
        }
    }
}
