//! MQTT to Samyama: each message on a mapped topic becomes one parameterized
//! Cypher write (#2217, part 3: the edge ingestion bridge).
//!
//! ```text
//! cargo run --example edge_iot_bridge -- examples/edge_iot_bridge.yaml
//! ```
//!
//! ```yaml
//! broker: mqtt://127.0.0.1:1883
//! samyama: http://127.0.0.1:8080
//! routes:
//!   - topic: factory/+/temperature
//!     cypher: |
//!       MERGE (s:Sensor {id: $sensor})
//!       CREATE (s)-[:EMITTED]->(:Reading {celsius: $value, at: $at})
//!     params:
//!       sensor: topic.1        # the second level of the topic
//!       value: payload.value   # a field of the JSON payload, dotted for nesting
//!       at: received_at        # when the bridge received it, epoch ms
//! ```
//!
//! A parameter is `topic` (the whole topic), `topic.<n>` (level `n`, from 0),
//! `payload` (the whole JSON payload), `payload.<a.b.c>` (one field of it), or
//! `received_at`. A message is checked against every route and written by
//! each one it matches.
//!
//! Delivery is **at least once**. Messages are subscribed at QoS 1 and
//! acknowledged only after Samyama has committed the write, so a write that
//! cannot land (Samyama down, a 5xx) is retried and the broker keeps the
//! message until it does. A crash between the commit and the acknowledgement
//! delivers the message again; key on a message id with `MERGE` where a
//! duplicate matters. A message no route can bind (not JSON, a field missing)
//! or one Samyama refuses as a client error (4xx) is logged and acknowledged:
//! sending it again would fail the same way and hold back every message
//! behind it.
//!
//! What is not here: Sparkplug B (protobuf payloads), OPC-UA, batching several
//! messages into one write, and TLS to the broker. Pair it with
//! `db.retention.set` so the readings it writes stay bounded.
//!
//! Environment: `SAMYAMA_TOKEN` (a bearer token for Samyama), `MQTT_USERNAME`
//! and `MQTT_PASSWORD`.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// `mqtt://host:port`; the port defaults to 1883.
    pub broker: String,
    /// Samyama's HTTP address.
    pub samyama: String,
    #[serde(default = "default_client_id")]
    pub client_id: String,
    pub routes: Vec<Route>,
}

fn default_client_id() -> String {
    "samyama-edge-bridge".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    /// An MQTT topic filter; `+` matches one level, `#` the rest.
    pub topic: String,
    pub cypher: String,
    /// Cypher parameter name to where its value comes from.
    #[serde(default)]
    pub params: BTreeMap<String, String>,
}

impl Config {
    pub fn parse(yaml: &str) -> Result<Self, String> {
        let config: Config = serde_yaml::from_str(yaml).map_err(|e| e.to_string())?;
        if config.routes.is_empty() {
            return Err("no routes: nothing would be written".into());
        }
        for route in &config.routes {
            if !rumqttc::valid_filter(&route.topic) {
                return Err(format!("`{}` is not an MQTT topic filter", route.topic));
            }
            for (name, source) in &route.params {
                check_source(source)
                    .map_err(|e| format!("{}: parameter `{name}`: {e}", route.topic))?;
            }
        }
        broker_address(&config.broker)?;
        Ok(config)
    }
}

fn check_source(source: &str) -> Result<(), String> {
    match source.split_once('.') {
        None if matches!(source, "topic" | "payload" | "received_at") => Ok(()),
        Some(("topic", n)) if n.parse::<usize>().is_ok() => Ok(()),
        Some(("payload", path)) if path.split('.').all(|k| !k.is_empty()) => Ok(()),
        _ => Err(format!(
            "`{source}` is not topic, topic.<n>, payload, payload.<field> or received_at"
        )),
    }
}

/// `mqtt://host:port` (or `host:port`) as (host, port).
pub fn broker_address(url: &str) -> Result<(String, u16), String> {
    let rest = url.strip_prefix("mqtt://").unwrap_or(url);
    let (host, port) = match rest.rsplit_once(':') {
        Some((h, p)) => (
            h,
            p.parse()
                .map_err(|_| format!("`{url}`: `{p}` is not a port"))?,
        ),
        None => (rest, 1883),
    };
    if host.is_empty() {
        return Err(format!("`{url}` names no host"));
    }
    Ok((host.to_string(), port))
}

/// The parameters `route` binds for this message, or why it cannot.
pub fn bind(
    route: &Route,
    topic: &str,
    payload: &[u8],
    received_at_ms: i64,
) -> Result<Map<String, Value>, String> {
    let levels: Vec<&str> = topic.split('/').collect();
    let mut json: Option<Value> = None;
    let mut params = Map::new();
    for (name, source) in &route.params {
        let value = match source.split_once('.') {
            None if source == "topic" => Value::String(topic.to_string()),
            None if source == "received_at" => Value::from(received_at_ms),
            Some(("topic", n)) => {
                let n: usize = n.parse().map_err(|_| format!("bad level in `{source}`"))?;
                let level = levels
                    .get(n)
                    .ok_or_else(|| format!("`{source}`: `{topic}` has {} levels", levels.len()))?;
                Value::String(level.to_string())
            }
            _ => {
                if json.is_none() {
                    json = Some(
                        serde_json::from_slice(payload)
                            .map_err(|e| format!("the payload is not JSON: {e}"))?,
                    );
                }
                let mut value = json.as_ref().unwrap();
                if let Some(("payload", path)) = source.split_once('.') {
                    for key in path.split('.') {
                        value = value.get(key).ok_or_else(|| {
                            format!("the payload has no `{path}` (parameter `{name}`)")
                        })?;
                    }
                }
                value.clone()
            }
        };
        params.insert(name.clone(), value);
    }
    Ok(params)
}

/// What happened to one write.
#[derive(Debug, PartialEq)]
pub enum Outcome {
    /// Committed.
    Written,
    /// Refused as a client error: sending it again cannot succeed.
    Refused(String),
    /// Samyama unreachable or a server error: worth retrying.
    Retry(String),
}

pub struct Sink {
    http: reqwest::Client,
    url: String,
    token: Option<String>,
}

impl Sink {
    pub fn new(samyama: &str, token: Option<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("an HTTP client");
        Sink {
            http,
            url: format!("{}/api/query", samyama.trim_end_matches('/')),
            token,
        }
    }

    pub async fn write(&self, cypher: &str, params: Map<String, Value>) -> Outcome {
        let mut request = self
            .http
            .post(&self.url)
            .json(&serde_json::json!({ "query": cypher, "params": params }));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        match request.send().await {
            Ok(r) if r.status().is_success() => Outcome::Written,
            Ok(r) if r.status().is_client_error() && r.status().as_u16() != 429 => {
                let status = r.status();
                Outcome::Refused(format!("{status}: {}", r.text().await.unwrap_or_default()))
            }
            Ok(r) => Outcome::Retry(format!("{}", r.status())),
            Err(e) => Outcome::Retry(e.to_string()),
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Writes one message through every route it matches; returns once each
/// write has been committed or refused, retrying the rest.
pub async fn deliver(config: &Config, sink: &Sink, topic: &str, payload: &[u8]) {
    let received_at = now_ms();
    for route in config
        .routes
        .iter()
        .filter(|r| rumqttc::matches(topic, &r.topic))
    {
        let params = match bind(route, topic, payload, received_at) {
            Ok(p) => p,
            Err(e) => {
                eprintln!(
                    "edge_iot_bridge: skipped a message on {topic} ({}): {e}",
                    route.topic
                );
                continue;
            }
        };
        let mut wait = Duration::from_millis(200);
        loop {
            match sink.write(&route.cypher, params.clone()).await {
                Outcome::Written => break,
                Outcome::Refused(why) => {
                    eprintln!("edge_iot_bridge: Samyama refused a message on {topic}: {why}");
                    break;
                }
                Outcome::Retry(why) => {
                    eprintln!("edge_iot_bridge: {topic}: {why}; retrying in {wait:?}");
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(Duration::from_secs(10));
                }
            }
        }
    }
}

/// Subscribes to every route's topic and runs until the process is stopped.
pub async fn run(
    config: Config,
    token: Option<String>,
    credentials: Option<(String, String)>,
) -> Result<(), String> {
    let (host, port) = broker_address(&config.broker)?;
    let mut options = MqttOptions::new(&config.client_id, host, port);
    options.set_keep_alive(Duration::from_secs(30));
    options.set_manual_acks(true);
    // A persistent session: the broker keeps what arrives while the bridge is away.
    options.set_clean_session(false);
    if let Some((user, password)) = credentials {
        options.set_credentials(user, password);
    }
    let (client, mut events) = AsyncClient::new(options, 64);
    let sink = Sink::new(&config.samyama, token);
    loop {
        match events.poll().await {
            // On every (re)connect: a broker that lost the session lost the subscriptions too.
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                for route in &config.routes {
                    client
                        .subscribe(&route.topic, QoS::AtLeastOnce)
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
            // The PUBACK goes out on a later `poll`, and nothing polls until
            // `deliver` returns, so a message is acknowledged only after its writes.
            Ok(Event::Incoming(Packet::Publish(message))) => {
                deliver(&config, &sink, &message.topic, &message.payload).await;
                client.ack(&message).await.map_err(|e| e.to_string())?;
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!(
                    "edge_iot_bridge: broker {}: {e}; reconnecting",
                    config.broker
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: edge_iot_bridge <config.yaml>");
        std::process::exit(64);
    };
    let config = std::fs::read_to_string(&path)
        .map_err(|e| format!("{path}: {e}"))
        .and_then(|text| Config::parse(&text).map_err(|e| format!("{path}: {e}")))
        .unwrap_or_else(|e| {
            eprintln!("edge_iot_bridge: {e}");
            std::process::exit(64);
        });
    let token = std::env::var("SAMYAMA_TOKEN").ok();
    let credentials = std::env::var("MQTT_USERNAME")
        .ok()
        .zip(std::env::var("MQTT_PASSWORD").ok());
    if let Err(e) = run(config, token, credentials).await {
        eprintln!("edge_iot_bridge: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use rumqttc::{ConnAck, ConnectReturnCode, PubAck, Publish, SubAck, SubscribeReasonCode};
    use samyama::graph::GraphStore;
    use samyama::http::server::HttpServer;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, RwLock};

    const CONFIG: &str = r#"
broker: mqtt://127.0.0.1:1
samyama: http://127.0.0.1:1
routes:
  - topic: factory/+/temperature
    cypher: "MERGE (s:Sensor {id: $sensor}) CREATE (s)-[:EMITTED]->(:Reading {celsius: $value, at: $at})"
    params: { sensor: topic.1, value: payload.reading.celsius, at: received_at }
"#;

    fn config(broker: u16, samyama: u16) -> Config {
        let text = CONFIG
            .replace("mqtt://127.0.0.1:1", &format!("mqtt://127.0.0.1:{broker}"))
            .replace("http://127.0.0.1:1", &format!("http://127.0.0.1:{samyama}"));
        Config::parse(&text).unwrap()
    }

    #[test]
    fn the_shipped_sample_config_parses() {
        let config = Config::parse(include_str!("edge_iot_bridge.yaml")).unwrap();
        assert_eq!(config.routes.len(), 1);
    }

    #[test]
    fn a_message_binds_topic_levels_payload_fields_and_the_arrival_time() {
        let route = &config(1, 1).routes[0];
        let p = bind(
            route,
            "factory/oven-3/temperature",
            br#"{"reading": {"celsius": 212.5}}"#,
            7,
        )
        .unwrap();
        assert_eq!(p["sensor"], "oven-3");
        assert_eq!(p["value"], 212.5);
        assert_eq!(p["at"], 7);
    }

    #[test]
    fn a_message_it_cannot_bind_says_why() {
        let route = &config(1, 1).routes[0];
        let missing = bind(route, "factory/a/temperature", br#"{"reading": {}}"#, 0).unwrap_err();
        assert!(missing.contains("reading.celsius"), "{missing}");
        let not_json = bind(route, "factory/a/temperature", b"212.5C", 0).unwrap_err();
        assert!(not_json.contains("not JSON"), "{not_json}");
    }

    #[test]
    fn a_config_it_cannot_use_is_refused_before_it_connects() {
        let good = CONFIG.to_string();
        for (bad, why) in [
            (good.replace("topic.1", "topic.one"), "not topic"),
            (
                good.replace("payload.reading.celsius", "body.celsius"),
                "not topic",
            ),
            (
                good.replace("factory/+/temperature", "factory/#/temperature"),
                "not an MQTT topic filter",
            ),
            (
                good.replace("mqtt://127.0.0.1:1", "mqtt://127.0.0.1:x"),
                "not a port",
            ),
            (
                "broker: b\nsamyama: s\nroutes: []\n".to_string(),
                "no routes",
            ),
        ] {
            let err = Config::parse(&bad).unwrap_err();
            assert!(err.contains(why), "{bad}\n=> {err}");
        }
    }

    /// Samyama's own HTTP router on `port` (0 for any), over a store the test can read.
    async fn samyama(port: u16) -> (u16, Arc<RwLock<GraphStore>>) {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let app = HttpServer::new(store.clone(), 0).router();
        let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (port, store)
    }

    /// Just enough of an MQTT 3.1.1 broker for one client: it accepts the
    /// connection and the subscriptions, sends what arrives on the returned
    /// sender as QoS 1 publishes, and reports each PUBACK's packet id.
    async fn broker() -> (u16, mpsc::Sender<(String, Vec<u8>)>, mpsc::Receiver<u16>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (send, mut to_publish) = mpsc::channel::<(String, Vec<u8>)>(16);
        let (acked, acks) = mpsc::channel(16);
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut input = BytesMut::new();
            let mut next_pkid = 1u16;
            // Publishes wait for a subscription, as from a real broker.
            let mut subscribed = false;
            loop {
                let mut out = BytesMut::new();
                loop {
                    match Packet::read(&mut input, 1 << 20) {
                        Ok(Packet::Connect(_)) => {
                            ConnAck::new(ConnectReturnCode::Success, false)
                                .write(&mut out)
                                .unwrap();
                        }
                        Ok(Packet::Subscribe(s)) => {
                            let codes = s
                                .filters
                                .iter()
                                .map(|_| SubscribeReasonCode::Success(QoS::AtLeastOnce))
                                .collect();
                            SubAck::new(s.pkid, codes).write(&mut out).unwrap();
                            subscribed = true;
                        }
                        Ok(Packet::PingReq) => out.extend_from_slice(&[0xD0, 0x00]),
                        Ok(Packet::PubAck(PubAck { pkid, .. })) => acked.send(pkid).await.unwrap(),
                        Ok(_) => {}
                        Err(rumqttc::Error::InsufficientBytes(_)) => break,
                        Err(e) => panic!("broker: {e:?}"),
                    }
                }
                socket.write_all(&out).await.unwrap();
                let mut buf = [0u8; 4096];
                tokio::select! {
                    n = socket.read(&mut buf) => match n.unwrap() {
                        0 => return,
                        n => input.extend_from_slice(&buf[..n]),
                    },
                    Some((topic, payload)) = to_publish.recv(), if subscribed => {
                        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
                        publish.pkid = next_pkid;
                        next_pkid += 1;
                        let mut out = BytesMut::new();
                        publish.write(&mut out).unwrap();
                        socket.write_all(&out).await.unwrap();
                    }
                }
            }
        });
        (port, send, acks)
    }

    async fn next_ack(acks: &mut mpsc::Receiver<u16>) -> u16 {
        tokio::time::timeout(Duration::from_secs(30), acks.recv())
            .await
            .expect("no PUBACK within 30 s")
            .unwrap()
    }

    async fn readings(store: &RwLock<GraphStore>) -> (usize, usize) {
        let s = store.read().await;
        (
            s.get_nodes_by_label(&"Sensor".into()).len(),
            s.get_nodes_by_label(&"Reading".into()).len(),
        )
    }

    #[tokio::test]
    async fn readings_reach_the_graph_and_are_acknowledged_after_the_write() {
        let (samyama_port, store) = samyama(0).await;
        let (broker_port, publish, mut acks) = broker().await;
        tokio::spawn(run(config(broker_port, samyama_port), None, None));

        publish
            .send((
                "factory/oven-1/temperature".into(),
                br#"{"reading": {"celsius": 20.5}}"#.to_vec(),
            ))
            .await
            .unwrap();
        publish
            .send(("factory/oven-1/temperature".into(), b"not json".to_vec()))
            .await
            .unwrap();
        publish
            .send((
                "factory/oven-1/temperature".into(),
                br#"{"reading": {"celsius": 21}}"#.to_vec(),
            ))
            .await
            .unwrap();
        publish
            .send((
                "factory/oven-1/pressure".into(),
                br#"{"reading": {"celsius": 1}}"#.to_vec(),
            ))
            .await
            .unwrap();

        // Every message is acknowledged, the unusable and the unmatched ones too.
        let mut got = vec![];
        for _ in 0..4 {
            got.push(next_ack(&mut acks).await);
        }
        assert_eq!(got, [1, 2, 3, 4]);
        assert_eq!(
            readings(&store).await,
            (1, 2),
            "one sensor, merged; two readings"
        );
        let s = store.read().await;
        let rows = samyama::query::QueryEngine::new()
            .execute("MATCH (:Sensor {id: 'oven-1'})-[:EMITTED]->(r:Reading) RETURN r.celsius AS c ORDER BY c", &s)
            .unwrap();
        let celsius: Vec<_> = rows
            .records
            .iter()
            .map(|r| format!("{:?}", r.get("c").and_then(|v| v.as_property()).unwrap()))
            .collect();
        assert_eq!(
            celsius,
            ["Float(20.5)", "Integer(21)"],
            "JSON numbers keep their type"
        );
    }

    #[tokio::test]
    async fn a_reading_samyama_cannot_take_yet_is_held_until_it_can() {
        // A port nothing listens on yet.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let (broker_port, publish, mut acks) = broker().await;
        tokio::spawn(run(config(broker_port, port), None, None));
        publish
            .send((
                "factory/oven-9/temperature".into(),
                br#"{"reading": {"celsius": 30}}"#.to_vec(),
            ))
            .await
            .unwrap();

        let early = tokio::time::timeout(Duration::from_secs(2), acks.recv()).await;
        assert!(
            early.is_err(),
            "acknowledged a reading that was never written: {early:?}"
        );

        let (_, store) = samyama(port).await;
        assert_eq!(next_ack(&mut acks).await, 1);
        assert_eq!(readings(&store).await, (1, 1));
    }
}
