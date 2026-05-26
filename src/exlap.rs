/// ExLAP client over the Android Auto vendor channel.
///
/// aa-proxy-rs opens its own channel to the HU's ExLAP vendor service
/// (`com.vwag.infotainment.gal.exlap`) and runs the full state machine
/// from ExlapReader.java to read car sensor data independently of any
/// phone app.  EV-relevant fields are injected into the AA energy model.
use crate::ev::{send_ev_data, BatteryData};
use crate::mitm::{Packet, PacketAction, ENCRYPTED, FRAME_TYPE_FIRST, FRAME_TYPE_LAST};
use crate::web::ServerEvent;
use openssl::base64 as ossl_b64;
use openssl::rand::rand_bytes;
use quick_xml::events::Event;
use quick_xml::Reader;
use serde::Serialize;
use sha2::{Digest, Sha256};
use simplelog::*;
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc::Sender, RwLock};

pub const EXLAP_VENDOR_CHANNEL_NAME: &str = "com.vwag.infotainment.gal.exlap";

// ── Shared state exposed to the web server ────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct ExlapUrlEntry {
    pub url: String,
    pub url_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExlapValueEntry {
    pub val: String,
    pub val_type: String,
    pub state: String,
    pub timestamp: Option<String>,
}

/// Live session info needed by the web layer to send subscribe/unsubscribe packets.
/// Web-side req IDs start at 10000 to avoid colliding with the state-machine IDs.
#[derive(Debug)]
pub struct ExlapSessionInfo {
    pub channel: u8,
    pub session_id: String,
    pub next_req_id: Arc<AtomicU32>,
}

impl Clone for ExlapSessionInfo {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel,
            session_id: self.session_id.clone(),
            next_req_id: self.next_req_id.clone(),
        }
    }
}

#[derive(Debug, Default)]
pub struct ExlapSharedState {
    pub connection_state: String,
    pub urls: Vec<ExlapUrlEntry>,
    pub values: HashMap<String, ExlapValueEntry>,
    pub subscription_limit_reached: bool,
    /// Live session info — Some once ExLAP is authenticated and active.
    pub session: Option<ExlapSessionInfo>,
}

pub type SharedExlapData = Arc<RwLock<ExlapSharedState>>;

// All four credential sets from ExlapReader.java, in index order.
// Which one works depends on the HU firmware variant.
const CREDENTIALS: &[(&str, &str)] = &[
    ("Test_TB-105000", "s4T2K6BAv0a7LQvrv3vdaUl17xEl2WJOpTmAThpRZe0=="),
    ("RSE_L-CA2000",   "T53Facvq51jO8vQJrBNx3MqLWmPcHf/hkow7yLu7SuA=="),
    ("RSE_3-DE1400",   "KozPo8iE0j72pkbWXKcP0QihpxgML3Opp8fNJZ0wN24=="),
    ("ML_74-125000",   "Fo7arEpPhAgMMznzxRlV8B7eeZgNDIYQcy0Gr7Ad1Fg=="),
];

// CONTROL flag bit — mirrors mitm.rs private const, defined here to avoid coupling.
const CONTROL_FLAG: u8 = 1 << 2;

const NAME: &str = "<i><bright-black> exlap: </>";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// State machine states mirroring ExlapReader.java.
#[derive(Debug, Clone, PartialEq)]
pub enum ExlapState {
    WaitChanOpen,
    WaitConnReturn,
    WaitInit,
    WaitCapabilities,
    WaitAuthChallenge,
    WaitAuthResponse,
    WaitUrlList,
    Active,
    /// All credentials exhausted; session is dead.
    Failed,
}

/// Per-session ExLAP client state stored in ModifyContext.
pub struct ExlapClient {
    pub channel: u8,
    pub state: ExlapState,
    pub session_id: String,
    pub req_id: u32,
    pub tank_level: Option<f32>,
    pub outside_temp: Option<f32>,
    /// Accumulates bytes when an XML message arrives in multiple AA frames.
    pub assemble_buf: Vec<u8>,
    /// Index into CREDENTIALS currently being tried.
    pub cred_idx: usize,
    /// Set to Some(cred_idx) when auth succeeds; caller should persist this.
    pub working_cred: Option<usize>,
}

impl ExlapClient {
    /// `start_cred_idx` is the hint from the vehicle profile (0 if unknown).
    pub fn new(channel: u8, start_cred_idx: usize) -> Self {
        let cred_idx = start_cred_idx.min(CREDENTIALS.len() - 1);
        Self {
            channel,
            state: ExlapState::WaitChanOpen,
            session_id: uuid::Uuid::new_v4().to_string(),
            req_id: 42,
            tank_level: None,
            outside_temp: None,
            assemble_buf: Vec::new(),
            cred_idx,
            working_cred: None,
        }
    }

    fn next_id(&mut self) -> u32 {
        let id = self.req_id;
        self.req_id += 1;
        id
    }

    fn make_req(&mut self, body: &str) -> String {
        let id = self.next_id();
        format!(
            r#"<ExlapStatement session_id="{sid}"><Req id="{id}">{body}</Req></ExlapStatement>"#,
            sid = self.session_id,
            id = id,
            body = body,
        )
    }

    fn make_pkt(&self, xml: &str) -> Packet {
        Packet {
            channel: self.channel,
            flags: ENCRYPTED | FRAME_TYPE_FIRST | FRAME_TYPE_LAST,
            final_length: None,
            payload: xml.as_bytes().to_vec(),
        }
    }

    fn user(&self) -> &'static str {
        CREDENTIALS[self.cred_idx].0
    }

    fn password(&self) -> &'static str {
        CREDENTIALS[self.cred_idx].1
    }
}

/// Called from `pkt_modify_hook` for every packet arriving on our ExLAP channel.
pub async fn handle_exlap_packet(
    pkt: &Packet,
    client: &mut ExlapClient,
    hu_tx: &Option<Sender<Packet>>,
    sensor_channel: Option<u8>,
    last_battery: Arc<RwLock<Option<BatteryData>>>,
    shared_exlap: SharedExlapData,
    ws_event_tx: &broadcast::Sender<ServerEvent>,
) -> Result<PacketAction> {
    if client.state == ExlapState::Failed {
        return Ok(PacketAction::Drop);
    }

    // Keep shared connection_state in sync
    {
        let state_str = match client.state {
            ExlapState::Active => "active",
            ExlapState::Failed => "failed",
            ExlapState::WaitChanOpen => "connecting",
            _ => "connecting",
        };
        let mut shared = shared_exlap.write().await;
        shared.connection_state = state_str.to_string();
    }

    // ── Control message (CHANNEL_OPEN_RESPONSE) ───────────────────────────────
    if pkt.flags & CONTROL_FLAG != 0 {
        if pkt.payload.len() < 2 {
            return Ok(PacketAction::Drop);
        }
        let msg_id = u16::from_be_bytes([pkt.payload[0], pkt.payload[1]]);
        const CHAN_OPEN_RSP: u16 = 0x000a;
        if msg_id == CHAN_OPEN_RSP && client.state == ExlapState::WaitChanOpen {
            info!(
                "{} channel {:#04x} open confirmed; sending ExlapConnectionRequest (cred={})",
                NAME, client.channel, client.cred_idx
            );
            client.state = ExlapState::WaitConnReturn;
            let xml = format!(r#"<ExlapConnectionRequest session_id="{}"/>"#, client.session_id);
            send_to_hu(hu_tx, client.make_pkt(&xml)).await;
        }
        return Ok(PacketAction::Drop);
    }

    // ── Data message: raw XML bytes, possibly fragmented ─────────────────────
    let is_first = pkt.flags & FRAME_TYPE_FIRST != 0;
    let is_last = pkt.flags & FRAME_TYPE_LAST != 0;

    if is_first {
        client.assemble_buf.clear();
    }
    client.assemble_buf.extend_from_slice(&pkt.payload);

    if !is_last {
        return Ok(PacketAction::Drop);
    }

    let xml = match std::str::from_utf8(&client.assemble_buf) {
        Ok(s) => s.to_owned(),
        Err(_) => {
            warn!("{} invalid UTF-8 on ExLAP channel, dropping", NAME);
            client.assemble_buf.clear();
            return Ok(PacketAction::Drop);
        }
    };
    client.assemble_buf.clear();

    debug!("{} <- {}", NAME, xml.trim());

    let root_tag = xml_root_tag(&xml).unwrap_or_default();

    match root_tag.as_str() {
        "ExlapBeacon" => return Ok(PacketAction::Drop),
        "ExlapConnectionClosed" => {
            warn!("{} server closed ExLAP connection", NAME);
            client.state = ExlapState::WaitInit;
            return Ok(PacketAction::Drop);
        }
        "ExlapConnectionReturn" => {
            if client.state != ExlapState::WaitConnReturn {
                return Ok(PacketAction::Drop);
            }
            let connected = xml_attr_in_tag(&xml, "ExlapConnectionReturn", "connected")
                .map(|v| v == "true")
                .unwrap_or(false);
            if !connected {
                warn!("{} ExlapConnectionReturn: connected=false", NAME);
                return Ok(PacketAction::Drop);
            }
            info!("{} ExLAP connection established; waiting for Init", NAME);
            client.state = ExlapState::WaitInit;
            return Ok(PacketAction::Drop);
        }
        "ExlapStatement" => {
            let sid = xml_attr_in_tag(&xml, "ExlapStatement", "session_id").unwrap_or_default();
            if sid != client.session_id {
                debug!("{} ignoring ExlapStatement for other session ({})", NAME, sid);
                return Ok(PacketAction::Drop);
            }
            advance_statement(client, &xml, hu_tx, sensor_channel, last_battery, shared_exlap, ws_event_tx).await?;
            return Ok(PacketAction::Drop);
        }
        other => {
            debug!("{} ignoring unknown root element: {}", NAME, other);
            return Ok(PacketAction::Drop);
        }
    }
}

async fn advance_statement(
    client: &mut ExlapClient,
    xml: &str,
    hu_tx: &Option<Sender<Packet>>,
    sensor_channel: Option<u8>,
    last_battery: Arc<RwLock<Option<BatteryData>>>,
    shared_exlap: SharedExlapData,
    ws_event_tx: &broadcast::Sender<ServerEvent>,
) -> Result<()> {
    match client.state.clone() {
        ExlapState::WaitInit => {
            if xml.contains("<Init") {
                info!("{} got Init; sending Protocol request", NAME);
                let req = client.make_req(r#"<Protocol version="1" returnCapabilities="true"/>"#);
                send_to_hu(hu_tx, client.make_pkt(&req)).await;
                client.state = ExlapState::WaitCapabilities;
            }
        }
        ExlapState::WaitCapabilities => {
            if xml.contains("<Capabilities") {
                info!(
                    "{} got Capabilities; sending auth challenge (trying cred {} \"{}\")",
                    NAME, client.cred_idx, client.user()
                );
                let req = client.make_req(r#"<Authenticate phase="challenge"/>"#);
                send_to_hu(hu_tx, client.make_pkt(&req)).await;
                client.state = ExlapState::WaitAuthChallenge;
            }
        }
        ExlapState::WaitAuthChallenge => {
            if let Some(nonce_b64) = xml_attr_in_tag(xml, "Challenge", "nonce") {
                match compute_auth_response(&nonce_b64, client.user(), client.password()) {
                    Ok((cnonce_b64, digest_b64)) => {
                        let body = format!(
                            r#"<Authenticate phase="response" user="{}" cnonce="{}" digest="{}"/>"#,
                            client.user(), cnonce_b64, digest_b64
                        );
                        let req = client.make_req(&body);
                        send_to_hu(hu_tx, client.make_pkt(&req)).await;
                        debug!("{} sent auth response for cred {}", NAME, client.cred_idx);
                        client.state = ExlapState::WaitAuthResponse;
                    }
                    Err(e) => {
                        error!("{} failed to compute auth digest: {}", NAME, e);
                    }
                }
            }
        }
        ExlapState::WaitAuthResponse => {
            // Empty <Rsp/> means authenticated; any child elements mean failure.
            if xml.contains("<Rsp") {
                let empty = match extract_rsp_inner(xml) {
                    None => true, // self-closing <Rsp ... />
                    Some(inner) => !inner.trim().contains('<'),
                };
                if empty {
                    info!(
                        "{} authenticated with cred {} (\"{}\")",
                        NAME, client.cred_idx, client.user()
                    );
                    client.working_cred = Some(client.cred_idx);
                    let req = client.make_req("<Dir/>");
                    send_to_hu(hu_tx, client.make_pkt(&req)).await;
                    client.state = ExlapState::WaitUrlList;
                } else {
                    // Try the next credential in the table.
                    let next = client.cred_idx + 1;
                    if next < CREDENTIALS.len() {
                        warn!(
                            "{} cred {} (\"{}\"): auth failed; trying cred {} (\"{}\")",
                            NAME, client.cred_idx, client.user(), next, CREDENTIALS[next].0
                        );
                        client.cred_idx = next;
                        // Re-issue the challenge so the server gives us a fresh nonce.
                        let req = client.make_req(r#"<Authenticate phase="challenge"/>"#);
                        send_to_hu(hu_tx, client.make_pkt(&req)).await;
                        client.state = ExlapState::WaitAuthChallenge;
                    } else {
                        error!(
                            "{} all {} credentials exhausted; ExLAP auth permanently failed",
                            NAME,
                            CREDENTIALS.len()
                        );
                        client.state = ExlapState::Failed;
                    }
                }
            }
        }
        ExlapState::WaitUrlList => {
            if xml.contains("<UrlList") {
                let url_entries = parse_url_list(xml);
                info!("{} HU exposes {} URLs", NAME, url_entries.len());

                // Publish the URL catalog to the web layer.
                {
                    let mut shared = shared_exlap.write().await;
                    shared.urls = url_entries.clone();
                    shared.connection_state = "active".to_string();
                    shared.session = Some(ExlapSessionInfo {
                        channel: client.channel,
                        session_id: client.session_id.clone(),
                        next_req_id: Arc::new(AtomicU32::new(10000)),
                    });
                }

                // Subscribe to EV fields only; user selects additional URLs via the web UI.
                let ev_urls = ["tankLevelSecondary", "outsideTemperature"];
                let mut subscribed = 0u32;
                for url in &ev_urls {
                    if url_entries.is_empty() || url_entries.iter().any(|e| e.url == *url) {
                        let body = format!(r#"<Subscribe url="{}" timeStamp="true"/>"#, url);
                        let req = client.make_req(&body);
                        send_to_hu(hu_tx, client.make_pkt(&req)).await;
                        subscribed += 1;
                        debug!("{} subscribed to {}", NAME, url);
                    }
                }
                if subscribed > 0 {
                    info!("{} subscribed to {} EV URL(s); additional URLs selectable via web UI", NAME, subscribed);
                } else {
                    warn!("{} no EV URLs found in HU URL list", NAME);
                }
                client.state = ExlapState::Active;
            }
        }
        ExlapState::Active => {
            // Check for subscription limit / other Rsp status codes before processing data.
            if xml.contains("<Rsp") {
                if let Some(status) = xml_attr_in_tag(xml, "Rsp", "status") {
                    match status.as_str() {
                        "subscriptionLimitReached" => {
                            warn!("{} subscription limit reached from HU", NAME);
                            shared_exlap.write().await.subscription_limit_reached = true;
                        }
                        "noMatchingUrl" => {
                            debug!("{} HU returned noMatchingUrl for a subscribe/get request", NAME);
                        }
                        "ok" => {}
                        other => {
                            debug!("{} Rsp status: {}", NAME, other);
                        }
                    }
                }
            }
            process_dat_messages(xml, client, hu_tx, sensor_channel, last_battery, shared_exlap, ws_event_tx).await;
        }
        _ => {}
    }
    Ok(())
}

async fn process_dat_messages(
    xml: &str,
    client: &mut ExlapClient,
    hu_tx: &Option<Sender<Packet>>,
    sensor_channel: Option<u8>,
    last_battery: Arc<RwLock<Option<BatteryData>>>,
    shared_exlap: SharedExlapData,
    ws_event_tx: &broadcast::Sender<ServerEvent>,
) {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut current_url: Option<String> = None;
    let mut current_timestamp: Option<String> = None;
    let mut current_val = String::new();
    let mut current_val_type = String::new();
    let mut current_state = String::new();
    let mut in_obj = false;
    let mut obj_fields: Vec<String> = Vec::new();
    let mut dat_depth: u32 = 0;
    let mut ev_updated = false;

    let mut changes: Vec<(String, ExlapValueEntry)> = Vec::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                let local = e.name().local_name();
                let tag = std::str::from_utf8(local.as_ref()).unwrap_or("").to_string();
                match tag.as_str() {
                    "Dat" => {
                        current_url = attr_value(e, b"url");
                        current_timestamp = attr_value(e, b"timestamp");
                        current_val.clear();
                        current_val_type.clear();
                        current_state = "ok".to_string();
                        in_obj = false;
                        obj_fields.clear();
                        dat_depth = 1;
                    }
                    "Obj" | "List" if dat_depth == 1 => {
                        current_val_type = tag.clone();
                        in_obj = true;
                        dat_depth += 1;
                    }
                    "Rel" | "Abs" | "Act" | "Enm" | "Txt" | "Tim" | "Bin" if dat_depth == 1 => {
                        let val = attr_value(e, b"val").unwrap_or_default();
                        let state = attr_value(e, b"state").unwrap_or_else(|| "ok".to_string());
                        current_val_type = tag.clone();
                        current_val = val.clone();
                        current_state = state.clone();
                        // EV telemetry special handling
                        if state != "nodata" && state != "error" {
                            if let (Some(url), Ok(v)) = (current_url.as_deref(), val.parse::<f32>()) {
                                match url {
                                    "tankLevelSecondary" => {
                                        info!("{} tankLevelSecondary = {}%", NAME, v);
                                        client.tank_level = Some(v);
                                        ev_updated = true;
                                    }
                                    "outsideTemperature" => {
                                        info!("{} outsideTemperature = {}°C", NAME, v);
                                        client.outside_temp = Some(v);
                                    }
                                    _ => {}
                                }
                            }
                        }
                        dat_depth += 1;
                    }
                    // Sub-fields of Obj/List: collect name=val pairs for display
                    "Rel" | "Abs" | "Act" | "Enm" | "Txt" | "Tim" | "Bin" if in_obj && dat_depth == 2 => {
                        if let (Some(name), Some(val), Some(state)) = (
                            attr_value(e, b"name"),
                            attr_value(e, b"val"),
                            Some(attr_value(e, b"state").unwrap_or_else(|| "ok".to_string())),
                        ) {
                            if state != "nodata" && state != "error" {
                                obj_fields.push(format!("{}: {}", name, val));
                            }
                        }
                        dat_depth += 1;
                    }
                    _ if dat_depth > 0 => {
                        dat_depth += 1;
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                let local = e.name().local_name();
                let tag = std::str::from_utf8(local.as_ref()).unwrap_or("");
                match tag {
                    "Dat" => {
                        if let Some(url) = current_url.take() {
                            let display_val = if current_val_type == "Obj" || current_val_type == "List" {
                                if obj_fields.is_empty() {
                                    format!("({})", current_val_type.to_lowercase())
                                } else {
                                    obj_fields.join(", ")
                                }
                            } else {
                                current_val.clone()
                            };
                            changes.push((url, ExlapValueEntry {
                                val: display_val,
                                val_type: current_val_type.clone(),
                                state: current_state.clone(),
                                timestamp: current_timestamp.clone(),
                            }));
                        }
                        dat_depth = 0;
                        in_obj = false;
                        obj_fields.clear();
                    }
                    "Obj" | "List" => {
                        in_obj = false;
                        if dat_depth > 0 { dat_depth -= 1; }
                    }
                    _ => {
                        if dat_depth > 0 { dat_depth -= 1; }
                    }
                }
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
    }

    if ev_updated {
        if let (Some(sensor_ch), Some(tx)) = (sensor_channel, hu_tx.as_ref()) {
            let batt = BatteryData {
                battery_level_percentage: client.tank_level,
                battery_level_wh: None,
                battery_capacity_wh: None,
                reference_air_density: None,
                external_temp_celsius: client.outside_temp,
            };
            if let Err(e) = send_ev_data(tx.clone(), sensor_ch, batt, last_battery).await {
                error!("{} send_ev_data failed: {}", NAME, e);
            }
        } else {
            debug!(
                "{} got EV data but sensor_channel={:?} or hu_tx unavailable",
                NAME, sensor_channel
            );
        }
    }

    if !changes.is_empty() {
        let payload = serde_json::to_string(&changes.iter().map(|(url, entry)| {
            serde_json::json!({
                "url": url,
                "val": entry.val,
                "type": entry.val_type,
                "state": entry.state,
                "timestamp": entry.timestamp,
            })
        }).collect::<Vec<_>>()).unwrap_or_default();

        let mut shared = shared_exlap.write().await;
        for (url, entry) in changes {
            shared.values.insert(url, entry);
        }
        drop(shared);

        let _ = ws_event_tx.send(ServerEvent {
            topic: "exlap".to_string(),
            payload,
        });
    }
}

async fn send_to_hu(hu_tx: &Option<Sender<Packet>>, pkt: Packet) {
    if let Some(tx) = hu_tx {
        if let Err(e) = tx.send(pkt).await {
            error!("{} send_to_hu failed: {}", NAME, e);
        }
    } else {
        warn!("{} hu_tx not available", NAME);
    }
}

// ── XML helpers ───────────────────────────────────────────────────────────────

fn xml_root_tag(xml: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                let local = e.name().local_name();
                return Some(std::str::from_utf8(local.as_ref()).unwrap_or("").to_owned());
            }
            Ok(Event::Eof) | Err(_) => return None,
            _ => {}
        }
    }
}

fn xml_attr_in_tag(xml: &str, tag_name: &str, attr_name: &str) -> Option<String> {
    let attr_bytes = attr_name.as_bytes();
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                let local = e.name().local_name();
                let name = std::str::from_utf8(local.as_ref()).unwrap_or("");
                if name == tag_name {
                    return attr_value(&e, attr_bytes);
                }
            }
            Ok(Event::Eof) | Err(_) => return None,
            _ => {}
        }
    }
}

fn parse_url_list(xml: &str) -> Vec<ExlapUrlEntry> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut urls = Vec::new();
    loop {
        match reader.read_event() {
            Ok(Event::Empty(e)) => {
                let local = e.name().local_name();
                if std::str::from_utf8(local.as_ref()).unwrap_or("") == "Match" {
                    if let Some(u) = attr_value(&e, b"url") {
                        let url_type = attr_value(&e, b"type").unwrap_or_default();
                        urls.push(ExlapUrlEntry { url: u, url_type });
                    }
                }
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
    }
    urls
}

fn attr_value(e: &quick_xml::events::BytesStart, name: &[u8]) -> Option<String> {
    e.attributes()
        .filter_map(|a| a.ok())
        .find(|a| a.key.local_name().as_ref() == name)
        .and_then(|a| a.unescape_value().ok())
        .map(|v| v.into_owned())
}

/// Return the text between `<Rsp...>` and `</Rsp>`, or None if self-closing.
fn extract_rsp_inner(xml: &str) -> Option<&str> {
    let start = xml.find("<Rsp")?;
    let after_bracket = xml[start..].find('>')?;
    let open_end = start + after_bracket;
    // Self-closing `<Rsp ... />` — char before `>` is `/`
    if xml.as_bytes().get(open_end.saturating_sub(1)) == Some(&b'/') {
        return None;
    }
    let content_start = open_end + 1;
    let close = xml.find("</Rsp>")?;
    Some(&xml[content_start..close])
}

// ── Auth ──────────────────────────────────────────────────────────────────────

/// Compute the ExLAP SHA-256 auth digest for the given credential pair.
///
/// Matches ExlapReader.java `computeDigest`:
///   sha256("{user:.44}:{password:.44}:{b64(nonce_bytes):.44}:{b64(cnonce_bytes):.44}") → base64
fn compute_auth_response(
    nonce_b64: &str,
    user: &str,
    password: &str,
) -> Result<(String, String)> {
    let nonce_bytes = ossl_b64::decode_block(nonce_b64)?;
    let nonce_clean = ossl_b64::encode_block(&nonce_bytes);

    let mut cnonce_bytes = [0u8; 16];
    rand_bytes(&mut cnonce_bytes)?;
    let cnonce_b64 = ossl_b64::encode_block(&cnonce_bytes);

    let input = format!(
        "{:.44}:{:.44}:{:.44}:{:.44}",
        user, password, nonce_clean, cnonce_b64
    );
    let hash = Sha256::digest(input.as_bytes());
    let digest_b64 = ossl_b64::encode_block(hash.as_slice());

    Ok((cnonce_b64, digest_b64))
}
