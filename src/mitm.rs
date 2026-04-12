use anyhow::Context;
use log::log_enabled;
use openssl::ssl::{ErrorCode, Ssl, SslContextBuilder, SslFiletype, SslMethod};
use simplelog::*;
use std::collections::{HashMap, HashSet};
use std::collections::VecDeque;
use std::fmt;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::timeout;
use tokio_uring::buf::BoundedBuf;

// protobuf stuff:
include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
use crate::mitm::protos::navigation_maneuver::NavigationType::*;
use crate::mitm::protos::Config as AudioConfig;
use crate::mitm::protos::*;
use crate::mitm::sensor_source_service::Sensor;
use crate::mitm::AudioStreamType::*;
use crate::mitm::ByeByeReason::USER_SELECTION;
use crate::mitm::Gear::GEAR_PARK;
use crate::mitm::MediaMessageId::*;
use crate::mitm::SensorMessageId::*;
use crate::mitm::SensorType::*;
use protobuf::text_format::print_to_string_pretty;
use protobuf::{Enum, EnumOrUnknown, Message, MessageDyn};
use protos::ControlMessageType::{self, *};

use crate::config::{Action::Stop, AppConfig, SharedConfig};
use crate::config_types::HexdumpLevel;
use crate::ev::EvTaskCommand;
use crate::io_uring::Endpoint;
use crate::io_uring::IoDevice;
use crate::io_uring::BUFFER_LEN;
use crate::media_tap::{reassemble_media_packet, tap_media_message, MediaFrameBuffer};
pub use crate::media_tap::{
    media_tcp_server, AudioStreamConfig, MediaSink, MediaStreamInfo, MediaStreamKind,
};

// module name for logging engine
fn get_name(proxy_type: ProxyType) -> String {
    let proxy = match proxy_type {
        ProxyType::HeadUnit => "HU",
        ProxyType::MobileDevice => "MD",
    };
    format!("<i><bright-black> mitm/{}: </>", proxy)
}

// Just a generic Result type to ease error handling for us. Errors in multithreaded
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum InjectedMediaPhase {
    #[default]
    Idle,
    SetupSeen,
    FocusSent,
    Started,
    Streaming,
}

impl InjectedMediaPhase {
    fn can_stream(self) -> bool {
        matches!(self, Self::Started | Self::Streaming)
    }

    fn awaiting_focus(self) -> bool {
        matches!(self, Self::SetupSeen)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct InjectedMediaState {
    phase: InjectedMediaPhase,
    session_id: i32,
    ack_counter: u32,
    last_flags: u8,
    trace_after_start: u16,
}

#[derive(Clone, Copy, Debug, Default)]
struct InjectedMediaCounters {
    setup_seen: u64,
    start_seen: u64,
    data_seen: u64,
    stop_seen: u64,
    focus_requests_seen: u64,
    focus_replies: u64,
    config_replies: u64,
    ack_replies: u64,
    passthrough_drops: u64,
}
// async contexts needs some extra restrictions
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// message related constants:
pub const HEADER_LENGTH: usize = 4;
pub const FRAME_TYPE_FIRST: u8 = 1 << 0;
pub const FRAME_TYPE_LAST: u8 = 1 << 1;
pub const FRAME_TYPE_MASK: u8 = FRAME_TYPE_FIRST | FRAME_TYPE_LAST;
const _CONTROL: u8 = 1 << 2;
pub const ENCRYPTED: u8 = 1 << 3;
const HANDSHAKE_PACKET_TIMEOUT_MS: u64 = 30_000;
const RUNTIME_TRANSMIT_TIMEOUT_CONTROL_MS: u64 = 5_000;
const RUNTIME_TRANSMIT_TIMEOUT_DATA_MS: u64 = 5_000;
const CONTROL_STATS_LOG_EVERY: u64 = 64;
const CONTROL_STATS_WINDOW_SAMPLES: usize = 256;
const INJECTED_MEDIA_STATS_LOG_EVERY: u64 = 128;
const STARTUP_TRACE_WINDOW: usize = 12;
const STARTUP_OPEN_TRACE_WINDOW: usize = 8;

// location for hu_/md_ private keys and certificates:
const KEYS_PATH: &str = "/etc/aa-proxy-rs";

// DHU string consts for developer mode
pub const DHU_MAKE: &str = "Google";
pub const DHU_MODEL: &str = "Desktop Head Unit";

pub struct ModifyContext {
    sensor_channel: Option<u8>,
    nav_channel: Option<u8>,
    audio_channels: Vec<u8>,
    ev_tx: Sender<EvTaskCommand>,
    /// Offset→sink map (keys 0-6). Used only at SDR time to look up which sink
    /// to assign to each real channel. Never used for tapping.
    media_sinks: HashMap<u8, MediaSink>,
    /// channel_id→sink map. Populated from SDR. Used for tapping data packets.
    media_channels: HashMap<u8, MediaSink>,
    /// Human-readable media channel labels derived from SDR, used for logging.
    media_service_labels: HashMap<u8, String>,
    /// Per-channel reassembly state for tapped media messages that span multiple
    /// AA transport frames.
    media_fragments: HashMap<u8, MediaFrameBuffer>,
    /// Original HU-advertised services from ServiceDiscoveryResponse.
    hu_service_ids: HashSet<i32>,
    /// Services synthesized by aa-proxy-rs and exposed only to the phone side.
    injected_service_ids: HashSet<i32>,
    /// Counter of channel opens targeting injected-only services.
    injected_open_seen: u64,
    /// Counter of injected-only service opens forwarded towards HU.
    injected_open_forwarded_to_hu: u64,
    /// Channels corresponding to injected services that must never be forwarded to HU.
    injected_channels: HashSet<u8>,
    /// Injected media service_id/channel -> display type.
    injected_media_display: HashMap<u8, DisplayType>,
    /// Per-channel media state for injected virtual sinks (hidden from HU).
    injected_media_state: HashMap<u8, InjectedMediaState>,
    /// Last observed tap client connection generation by media channel.
    injected_media_connect_gen: HashMap<u8, u64>,
    /// Last observed tap-consumer presence by media channel.
    injected_media_had_tap_client: HashMap<u8, bool>,
    /// Runtime counters for injected virtual sink packets.
    injected_media_counters: InjectedMediaCounters,
    /// True once MobileDevice proxy has forwarded SDR towards the phone.
    md_sdr_forwarded: bool,
    /// Number of control packets received from phone after SDR was forwarded.
    md_control_after_sdr: u64,
    /// Last control message id received from phone after SDR.
    md_last_control_after_sdr: Option<i32>,
    /// Recent control message ids on MD proxy (phone -> HU path) after SDR.
    md_recent_control_from_md: VecDeque<u16>,
    /// Recent control message ids on MD proxy (HU -> phone path) after SDR.
    md_recent_control_to_md: VecDeque<u16>,
    /// Recent media message ids on MD proxy (phone -> HU path) after SDR.
    md_recent_media_from_md: VecDeque<u16>,
    /// Recent media message ids on MD proxy (HU -> phone path) after SDR.
    md_recent_media_to_md: VecDeque<u16>,
    /// Recent pending channel-open metadata on MD proxy after SDR.
    md_pending_channel_opens: VecDeque<String>,
    /// Seen post-SDR AUDIO_FOCUS_RELEASE from phone.
    md_audio_focus_release_seen: bool,
    /// Seen post-SDR AUDIO_FOCUS_GAIN* request from phone.
    md_audio_focus_gain_seen: bool,
    /// Last AUDIO_FOCUS_REQUEST type observed from phone after SDR.
    md_last_audio_focus_request: Option<protos::AudioFocusRequestType>,
    /// Seen post-SDR AUDIO_FOCUS_STATE_GAIN* from HU.
    hu_audio_focus_gain_seen: bool,
    /// Last AUDIO_FOCUS_NOTIFICATION state observed from HU after SDR.
    hu_last_audio_focus_state: Option<protos::AudioFocusStateType>,
    /// Last unsolicited flag observed on HU AUDIO_FOCUS_NOTIFICATION after SDR.
    hu_last_audio_focus_unsolicited: Option<bool>,
    /// Seen post-SDR media setup/start/config from phone.
    md_media_startup_seen: bool,
    /// Count of HU ping requests forwarded towards phone after SDR.
    hu_ping_requests_forwarded_to_md: u64,
    /// Count of ping responses observed from phone after SDR.
    md_ping_responses_seen: u64,
    /// SERVICE_DISCOVERY_UPDATE counts by flow.
    sdu_from_endpoint: u64,
    sdu_to_endpoint: u64,
    /// CHANNEL_OPEN_REQUEST counts by flow.
    open_req_from_endpoint: u64,
    open_req_to_endpoint: u64,
    /// CHANNEL_OPEN_RESPONSE counts by flow.
    open_rsp_from_endpoint: u64,
    open_rsp_to_endpoint: u64,
}

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum ProxyType {
    HeadUnit,
    MobileDevice,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacketFlow {
    /// Packet observed on endpoint reader path (device -> proxy).
    FromEndpoint,
    /// Packet observed on forwarding path (proxy -> endpoint).
    ToEndpoint,
}

#[derive(Clone, Copy)]
struct DisplayProfile {
    display_type: DisplayType,
    display_id: u32,
    codec_resolution: VideoCodecResolutionType,
    width_margin: u32,
    height_margin: u32,
    density: u32,
    viewing_distance: u32,
    touch_width: i32,
    touch_height: i32,
}

fn display_profiles(cfg: &AppConfig) -> Vec<DisplayProfile> {
    let mut profiles = Vec::new();
    let Some(display_types) = cfg.inject_display_types.0.as_ref() else {
        return profiles;
    };

    for display_type in display_types {
        match display_type {
            DisplayType::DISPLAY_TYPE_CLUSTER => profiles.push(DisplayProfile {
                display_type: DisplayType::DISPLAY_TYPE_CLUSTER,
                display_id: cfg.inject_cluster_display_id.into(),
                codec_resolution: cfg.inject_cluster_codec_resolution.0,
                width_margin: cfg.inject_cluster_width_margin.into(),
                height_margin: cfg.inject_cluster_height_margin.into(),
                density: if cfg.dpi > 0 {
                    cfg.dpi.into()
                } else {
                    cfg.inject_cluster_density.into()
                },
                viewing_distance: cfg.inject_cluster_viewing_distance.into(),
                touch_width: cfg.inject_cluster_touch_width.into(),
                touch_height: cfg.inject_cluster_touch_height.into(),
            }),
            DisplayType::DISPLAY_TYPE_AUXILIARY => profiles.push(DisplayProfile {
                display_type: DisplayType::DISPLAY_TYPE_AUXILIARY,
                display_id: cfg.inject_aux_display_id.into(),
                codec_resolution: VideoCodecResolutionType::VIDEO_1280x720,
                width_margin: cfg.inject_aux_width_margin.into(),
                height_margin: cfg.inject_aux_height_margin.into(),
                density: if cfg.dpi > 0 {
                    cfg.dpi.into()
                } else {
                    cfg.inject_aux_density.into()
                },
                viewing_distance: cfg.inject_aux_viewing_distance.into(),
                touch_width: cfg.inject_aux_touch_width.into(),
                touch_height: cfg.inject_aux_touch_height.into(),
            }),
            DisplayType::DISPLAY_TYPE_MAIN => {
                // Main display is expected from HU and is intentionally not synthesized.
            }
        }
    }

    profiles
}

fn has_video_display(msg: &ServiceDiscoveryResponse, display_type: DisplayType) -> bool {
    msg.services.iter().any(|svc| {
        !svc.media_sink_service.video_configs.is_empty()
            && svc.media_sink_service.display_type() == display_type
    })
}

fn has_input_display(msg: &ServiceDiscoveryResponse, display_id: u32) -> bool {
    msg.services.iter().any(|svc| {
        svc.input_source_service.is_some() && svc.input_source_service.display_id() == display_id
    })
}

fn next_service_id(msg: &ServiceDiscoveryResponse) -> i32 {
    msg.services.iter().map(|s| s.id()).max().unwrap_or(0) + 1
}

fn create_media_sink_service(id: i32, profile: DisplayProfile) -> Service {
    let mut margins = Insets::new();
    margins.set_top(profile.height_margin / 2);
    margins.set_bottom(profile.height_margin / 2);
    margins.set_left(profile.width_margin / 2);
    margins.set_right(profile.width_margin / 2);

    let mut ui_config = UiConfig::new();
    ui_config.margins = Some(margins).into();
    ui_config.content_insets = Some(Insets::new()).into();
    ui_config.stable_content_insets = Some(Insets::new()).into();
    ui_config.set_ui_theme(UiTheme::UI_THEME_AUTOMATIC);

    let mut video_cfg = VideoConfiguration::new();
    video_cfg.set_codec_resolution(profile.codec_resolution);
    video_cfg.set_frame_rate(VideoFrameRateType::VIDEO_FPS_30);
    video_cfg.set_width_margin(profile.width_margin);
    video_cfg.set_height_margin(profile.height_margin);
    video_cfg.set_density(profile.density);
    video_cfg.set_decoder_additional_depth(0);
    video_cfg.set_viewing_distance(profile.viewing_distance);
    video_cfg.set_pixel_aspect_ratio_e4(10000);
    video_cfg.set_real_density(profile.density);
    video_cfg.set_video_codec_type(MediaCodecType::MEDIA_CODEC_VIDEO_H264_BP);
    video_cfg.ui_config = Some(ui_config).into();

    let mut sink = MediaSinkService::new();
    sink.set_available_type(MediaCodecType::MEDIA_CODEC_VIDEO_H264_BP);
    sink.video_configs.push(video_cfg);
    sink.set_display_id(profile.display_id);
    sink.set_display_type(profile.display_type);

    let mut service = Service::new();
    service.set_id(id);
    service.media_sink_service = Some(sink).into();
    service
}

fn create_input_source_service(id: i32, profile: DisplayProfile) -> Service {
    let keycodes = match profile.display_type {
        DisplayType::DISPLAY_TYPE_CLUSTER => vec![19, 20, 21, 22, 23],
        DisplayType::DISPLAY_TYPE_AUXILIARY => {
            vec![3, 4, 5, 6, 84, 85, 87, 88, 126, 127, 65537, 65538, 65540]
        }
        DisplayType::DISPLAY_TYPE_MAIN => unreachable!("main input source must not be synthesized"),
    };

    let mut source = InputSourceService::new();
    source.keycodes_supported = keycodes;
    if profile.display_type == DisplayType::DISPLAY_TYPE_AUXILIARY {
        let mut touchscreen = input_source_service::TouchScreen::new();
        touchscreen.set_width(profile.touch_width);
        touchscreen.set_height(profile.touch_height);
        touchscreen.set_type(TouchScreenType::RESISTIVE);
        touchscreen.set_is_secondary(true);
        source.touchscreen.push(touchscreen);
    }
    source.set_display_id(profile.display_id);

    let mut service = Service::new();
    service.set_id(id);
    service.input_source_service = Some(source).into();
    service
}

fn add_display_services(msg: &mut ServiceDiscoveryResponse, cfg: &AppConfig) -> usize {
    if !cfg.mitm {
        return 0;
    }

    let mut added = 0usize;
    for profile in display_profiles(cfg) {
        if !has_video_display(msg, profile.display_type) {
            let id = next_service_id(msg);
            msg.services
                .push(create_media_sink_service(id, profile));
            added += 1;
        }

        if cfg.inject_add_input_sources && !has_input_display(msg, profile.display_id) {
            let id = next_service_id(msg);
            msg.services
                .push(create_input_source_service(id, profile));
            added += 1;
        }
    }

    added
}

fn service_role(svc: &Service) -> &'static str {
    if !svc.media_sink_service.video_configs.is_empty() {
        "media/video"
    } else if !svc.media_sink_service.audio_configs.is_empty() || svc.media_sink_service.audio_type.is_some() {
        "media/audio"
    } else if svc.input_source_service.is_some() {
        "input"
    } else if !svc.sensor_source_service.sensors.is_empty() {
        "sensor"
    } else if svc.bluetooth_service.is_some() {
        "bluetooth"
    } else if svc.wifi_projection_service.is_some() {
        "wifi"
    } else if svc.navigation_status_service.is_some() {
        "navigation"
    } else {
        "other"
    }
}

fn summarize_services(services: &[Service]) -> String {
    services
        .iter()
        .map(|svc| {
            let mut details = format!("id={} role={}", svc.id(), service_role(svc));
            if !svc.media_sink_service.video_configs.is_empty() {
                details.push_str(&format!(
                    " display={:?}/{}",
                    svc.media_sink_service.display_type(),
                    svc.media_sink_service.display_id()
                ));
            } else if svc.input_source_service.is_some() {
                details.push_str(&format!(" display_id={}", svc.input_source_service.display_id()));
            }
            details
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn bump_flow_counter(flow: PacketFlow, from_endpoint: &mut u64, to_endpoint: &mut u64) {
    match flow {
        PacketFlow::FromEndpoint => *from_endpoint += 1,
        PacketFlow::ToEndpoint => *to_endpoint += 1,
    }
}

fn log_divergence_counters(proxy_type: ProxyType, ctx: &ModifyContext, reason: &str) {
    info!(
        "{} <blue>divergence counters ({}):</> sdu[from={},to={}] open_req[from={},to={}] open_rsp[from={},to={}] injected_open_seen={} injected_forwarded_to_hu={}",
        get_name(proxy_type),
        reason,
        ctx.sdu_from_endpoint,
        ctx.sdu_to_endpoint,
        ctx.open_req_from_endpoint,
        ctx.open_req_to_endpoint,
        ctx.open_rsp_from_endpoint,
        ctx.open_rsp_to_endpoint,
        ctx.injected_open_seen,
        ctx.injected_open_forwarded_to_hu,
    );
}

fn push_trace_id(window: &mut VecDeque<u16>, message_id: u16) {
    if window.len() >= STARTUP_TRACE_WINDOW {
        window.pop_front();
    }
    window.push_back(message_id);
}

fn format_trace_ids(window: &VecDeque<u16>) -> String {
    window
        .iter()
        .map(|id| format!("0x{id:04X}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn push_trace_text(window: &mut VecDeque<String>, text: String, max_len: usize) {
    if window.len() >= max_len {
        window.pop_front();
    }
    window.push_back(text);
}

fn format_trace_text(window: &VecDeque<String>) -> String {
    window.iter().cloned().collect::<Vec<_>>().join(" | ")
}

fn describe_media_service(svc: &Service) -> Option<String> {
    if !svc.media_sink_service.video_configs.is_empty() {
        let video_cfg = &svc.media_sink_service.video_configs[0];
        Some(format!(
            "id={} video codec={:?} display={:?}/{} res={:?} fps={:?} margin={}x{} density={} real_density={} viewing_distance={}",
            svc.id(),
            video_cfg.video_codec_type(),
            svc.media_sink_service.display_type(),
            svc.media_sink_service.display_id(),
            video_cfg.codec_resolution(),
            video_cfg.frame_rate(),
            video_cfg.width_margin(),
            video_cfg.height_margin(),
            video_cfg.density(),
            video_cfg.real_density(),
            video_cfg.viewing_distance(),
        ))
    } else if !svc.media_sink_service.audio_configs.is_empty() || svc.media_sink_service.audio_type.is_some() {
        let audio_cfg = svc.media_sink_service.audio_configs.first();
        Some(format!(
            "id={} audio codec={:?} stream={:?} {}",
            svc.id(),
            svc.media_sink_service.available_type(),
            svc.media_sink_service.audio_type(),
            audio_cfg
                .map(|cfg| format!("{}Hz/{}ch/{}bit", cfg.sampling_rate(), cfg.number_of_channels(), cfg.number_of_bits()))
                .unwrap_or_else(|| "no-config".to_string())
        ))
    } else {
        None
    }
}

fn summarize_media_services(services: &[Service]) -> String {
    services
        .iter()
        .filter_map(describe_media_service)
        .collect::<Vec<_>>()
        .join(" | ")
}

fn media_channel_label(ctx: &ModifyContext, channel: u8) -> String {
    ctx.media_service_labels
        .get(&channel)
        .cloned()
        .unwrap_or_else(|| format!("channel={:#04x}", channel))
}

fn log_native_media_packet(proxy_type: ProxyType, flow: PacketFlow, pkt: &Packet, ctx: &ModifyContext) {
    if pkt.payload.len() < 2 {
        return;
    }

    let message_id: i32 = u16::from_be_bytes([pkt.payload[0], pkt.payload[1]]).into();
    let data = &pkt.payload[2..];
    let channel_label = media_channel_label(ctx, pkt.channel);

    match protos::MediaMessageId::from_i32(message_id).unwrap_or(MEDIA_MESSAGE_DATA) {
        MEDIA_MESSAGE_SETUP => {
            if let Ok(msg) = Setup::parse_from_bytes(data) {
                info!(
                    "{} <blue>native media:</> flow={:?} channel={:#04x} {} SETUP type={:?}",
                    get_name(proxy_type),
                    flow,
                    pkt.channel,
                    channel_label,
                    msg.type_()
                );
            }
        }
        MEDIA_MESSAGE_START => {
            if let Ok(msg) = Start::parse_from_bytes(data) {
                info!(
                    "{} <blue>native media:</> flow={:?} channel={:#04x} {} START session_id={} cfg_index={}",
                    get_name(proxy_type),
                    flow,
                    pkt.channel,
                    channel_label,
                    msg.session_id(),
                    msg.configuration_index()
                );
            }
        }
        MEDIA_MESSAGE_CONFIG => {
            if let Ok(msg) = AudioConfig::parse_from_bytes(data) {
                info!(
                    "{} <blue>native media:</> flow={:?} channel={:#04x} {} CONFIG status={:?} max_unacked={} cfg_indices={:?}",
                    get_name(proxy_type),
                    flow,
                    pkt.channel,
                    channel_label,
                    msg.status(),
                    msg.max_unacked(),
                    msg.configuration_indices
                );
            }
        }
        MEDIA_MESSAGE_ACK => {
            if let Ok(msg) = Ack::parse_from_bytes(data) {
                info!(
                    "{} <blue>native media:</> flow={:?} channel={:#04x} {} ACK session_id={} ack={}",
                    get_name(proxy_type),
                    flow,
                    pkt.channel,
                    channel_label,
                    msg.session_id(),
                    msg.ack()
                );
            }
        }
        MEDIA_MESSAGE_VIDEO_FOCUS_REQUEST => {
            if let Ok(msg) = VideoFocusRequestNotification::parse_from_bytes(data) {
                info!(
                    "{} <blue>native media:</> flow={:?} channel={:#04x} {} VIDEO_FOCUS_REQUEST mode={:?} reason={:?}",
                    get_name(proxy_type),
                    flow,
                    pkt.channel,
                    channel_label,
                    msg.mode(),
                    msg.reason()
                );
            }
        }
        MEDIA_MESSAGE_VIDEO_FOCUS_NOTIFICATION => {
            if let Ok(msg) = VideoFocusNotification::parse_from_bytes(data) {
                info!(
                    "{} <blue>native media:</> flow={:?} channel={:#04x} {} VIDEO_FOCUS_NOTIFICATION focus={:?} unsolicited={}",
                    get_name(proxy_type),
                    flow,
                    pkt.channel,
                    channel_label,
                    msg.focus(),
                    msg.unsolicited()
                );
            }
        }
        MEDIA_MESSAGE_STOP => {
            info!(
                "{} <blue>native media:</> flow={:?} channel={:#04x} {} STOP",
                get_name(proxy_type),
                flow,
                pkt.channel,
                channel_label
            );
        }
        _ => {}
    }
}

fn format_md_startup_snapshot(ctx: &ModifyContext) -> String {
    format!(
        "md_sdr_forwarded={} md_control_after_sdr={} md_last_control_after_sdr={:?} focus_release_seen={} focus_gain_seen={} md_last_focus_req={:?} hu_focus_gain_seen={} hu_last_focus_state={:?} hu_last_focus_unsolicited={:?} media_startup_seen={} hu_ping_req_to_md={} md_ping_rsp_seen={} recent_ctrl_from_md=[{}] recent_ctrl_to_md=[{}] recent_media_from_md=[{}] recent_media_to_md=[{}] pending_channel_opens=[{}]",
        ctx.md_sdr_forwarded,
        ctx.md_control_after_sdr,
        ctx.md_last_control_after_sdr,
        ctx.md_audio_focus_release_seen,
        ctx.md_audio_focus_gain_seen,
        ctx.md_last_audio_focus_request,
        ctx.hu_audio_focus_gain_seen,
        ctx.hu_last_audio_focus_state,
        ctx.hu_last_audio_focus_unsolicited,
        ctx.md_media_startup_seen,
        ctx.hu_ping_requests_forwarded_to_md,
        ctx.md_ping_responses_seen,
        format_trace_ids(&ctx.md_recent_control_from_md),
        format_trace_ids(&ctx.md_recent_control_to_md),
        format_trace_ids(&ctx.md_recent_media_from_md),
        format_trace_ids(&ctx.md_recent_media_to_md),
        format_trace_text(&ctx.md_pending_channel_opens),
    )
}

fn injected_max_unacked(display_type: DisplayType) -> u32 {
    match display_type {
        DisplayType::DISPLAY_TYPE_CLUSTER => 1,
        DisplayType::DISPLAY_TYPE_AUXILIARY => 2,
        _ => 1,
    }
}

fn maybe_log_injected_media_counters(
    proxy_type: ProxyType,
    counters: &InjectedMediaCounters,
    channel: u8,
    display_type: DisplayType,
) {
    let total = counters
        .setup_seen
        .saturating_add(counters.start_seen)
        .saturating_add(counters.data_seen)
        .saturating_add(counters.stop_seen)
        .saturating_add(counters.passthrough_drops);
    if total == 0 || total % INJECTED_MEDIA_STATS_LOG_EVERY != 0 {
        return;
    }

    info!(
        "{} <blue>injected media stats:</> channel={:#04x} display={:?} setup={} start={} data={} stop={} config_replies={} ack_replies={} drops={}",
        get_name(proxy_type),
        channel,
        display_type,
        counters.setup_seen,
        counters.start_seen,
        counters.data_seen,
        counters.stop_seen,
        counters.config_replies,
        counters.ack_replies,
        counters.passthrough_drops,
    );
}

fn injected_phase_label(phase: InjectedMediaPhase) -> &'static str {
    match phase {
        InjectedMediaPhase::Idle => "idle",
        InjectedMediaPhase::SetupSeen => "setup_seen",
        InjectedMediaPhase::FocusSent => "focus_sent",
        InjectedMediaPhase::Started => "started",
        InjectedMediaPhase::Streaming => "streaming",
    }
}

fn trace_injected_phase_transition(
    cfg: &AppConfig,
    proxy_type: ProxyType,
    channel: u8,
    display_type: DisplayType,
    from: InjectedMediaPhase,
    to: InjectedMediaPhase,
    reason: &str,
    session_id: i32,
    ack_counter: u32,
) {
    if !cfg.trace_channel_flow || from == to {
        return;
    }

    info!(
        "{} <cyan>flow trace:</> injected phase {} -> {} on channel <b>{:#04x}</> display={:?} reason={} session_id={} ack={}",
        get_name(proxy_type),
        injected_phase_label(from),
        injected_phase_label(to),
        channel,
        display_type,
        reason,
        session_id,
        ack_counter,
    );
}

fn frame_type_label(flags: u8) -> &'static str {
    match flags & FRAME_TYPE_MASK {
        f if f == (FRAME_TYPE_FIRST | FRAME_TYPE_LAST) => "first+last",
        f if f == FRAME_TYPE_FIRST => "first",
        f if f == FRAME_TYPE_LAST => "last",
        _ => "middle",
    }
}

fn first_fragment_message_id(pkt: &Packet) -> Option<u16> {
    if pkt.payload.len() < 2 {
        return None;
    }

    match pkt.flags & FRAME_TYPE_MASK {
        f if f == FRAME_TYPE_FIRST || f == (FRAME_TYPE_FIRST | FRAME_TYPE_LAST) => {
            Some(u16::from_be_bytes([pkt.payload[0], pkt.payload[1]]))
        }
        _ => None,
    }
}

fn debug_injected_probe(proxy_type: ProxyType, leg: &str, pkt: &Packet, ctx: &ModifyContext) {
    let is_injected = ctx.injected_channels.contains(&pkt.channel)
        || ctx.injected_media_display.contains_key(&pkt.channel)
        || ctx.injected_media_state.contains_key(&pkt.channel);
    if !is_injected {
        return;
    }

    let msg_id = first_fragment_message_id(pkt)
        .map(|id| format!("0x{id:04X}"))
        .unwrap_or_else(|| "n/a".to_string());
    let state = ctx.injected_media_state.get(&pkt.channel);
    let (phase, session_id, ack_counter, trace_after_start) = state
        .map(|s| {
            (
                s.phase,
                s.session_id,
                s.ack_counter,
                s.trace_after_start,
            )
        })
        .unwrap_or((InjectedMediaPhase::Idle, 0, 0, 0));
    let frag = ctx.media_fragments.get(&pkt.channel);
    let (frag_len, frag_expected) = frag
        .map(|f| (f.data.len(), f.expected_len))
        .unwrap_or((0, None));

    debug!(
        "{} <blue>injected probe:</> leg={} channel={:#04x} flags=0x{:02X} frame={} msg_id={} payload_len={} final_len={:?} phase={} session_id={} ack={} trace_after_start={} frag_len={} frag_expected={:?}",
        get_name(proxy_type),
        leg,
        pkt.channel,
        pkt.flags,
        frame_type_label(pkt.flags),
        msg_id,
        pkt.payload.len(),
        pkt.final_length,
        injected_phase_label(phase),
        session_id,
        ack_counter,
        trace_after_start,
        frag_len,
        frag_expected,
    );
}

fn rewrite_media_config_ready(pkt: &mut Packet, max_unacked: u32) -> Result<()> {
    let mut cfg = protos::Config::new();
    cfg.set_status(protos::config::Status::STATUS_READY);
    cfg.set_max_unacked(max_unacked);
    cfg.configuration_indices.push(0);

    let mut payload = cfg.write_to_bytes()?;
    payload.insert(0, ((MEDIA_MESSAGE_CONFIG as u16) >> 8) as u8);
    payload.insert(1, ((MEDIA_MESSAGE_CONFIG as u16) & 0xff) as u8);
    pkt.payload = payload;
    // Payload was rebuilt, so any old fragment metadata must be cleared.
    pkt.final_length = None;
    pkt.flags = (pkt.flags & !FRAME_TYPE_MASK) | FRAME_TYPE_FIRST | FRAME_TYPE_LAST;
    Ok(())
}

fn rewrite_media_ack(pkt: &mut Packet, session_id: i32, ack_counter: u32) -> Result<()> {
    let mut ack = Ack::new();
    ack.set_session_id(session_id);
    ack.set_ack(ack_counter);

    let mut payload = ack.write_to_bytes()?;
    payload.insert(0, ((MEDIA_MESSAGE_ACK as u16) >> 8) as u8);
    payload.insert(1, ((MEDIA_MESSAGE_ACK as u16) & 0xff) as u8);
    pkt.payload = payload;
    // Payload was rebuilt, so any old fragment metadata must be cleared.
    pkt.final_length = None;
    pkt.flags = (pkt.flags & !FRAME_TYPE_MASK) | FRAME_TYPE_FIRST | FRAME_TYPE_LAST;
    Ok(())
}

fn rewrite_video_focus_notification(
    pkt: &mut Packet,
    focus: VideoFocusMode,
    unsolicited: bool,
) -> Result<()> {
    let mut notification = VideoFocusNotification::new();
    notification.set_focus(focus);
    notification.set_unsolicited(unsolicited);

    let mut payload = notification.write_to_bytes()?;
    payload.insert(
        0,
        ((MEDIA_MESSAGE_VIDEO_FOCUS_NOTIFICATION as u16) >> 8) as u8,
    );
    payload.insert(
        1,
        ((MEDIA_MESSAGE_VIDEO_FOCUS_NOTIFICATION as u16) & 0xff) as u8,
    );
    pkt.payload = payload;
    // Payload was rebuilt, so any old fragment metadata must be cleared.
    pkt.final_length = None;
    pkt.flags = (pkt.flags & !FRAME_TYPE_MASK) | FRAME_TYPE_FIRST | FRAME_TYPE_LAST;
    Ok(())
}

fn maybe_emit_pending_injected_focus(
    proxy_type: ProxyType,
    ctx: &mut ModifyContext,
    cfg: &AppConfig,
    tx: &Sender<Packet>,
) -> Result<()> {
    let mut ready_channels: Vec<(u8, u8, bool)> = Vec::new();
    let mut toggle_channels: Vec<(u8, u8)> = Vec::new();
    let mut release_channels: Vec<(u8, u8)> = Vec::new();
    let mut connect_gen_updates: Vec<(u8, u64)> = Vec::new();
    let mut tap_presence_updates: Vec<(u8, bool)> = Vec::new();

    for (&channel, state) in &ctx.injected_media_state {
        let sink = ctx.media_channels.get(&channel);
        let has_tap_client = sink.map(|s| s.has_subscribers()).unwrap_or(false);
        let connect_gen = sink
            .map(|s| s.client_connect_generation())
            .unwrap_or_default();
        let seen_connect_gen = ctx
            .injected_media_connect_gen
            .get(&channel)
            .copied()
            .unwrap_or_default();
        let new_connection = connect_gen > seen_connect_gen;
        let had_tap_client = ctx
            .injected_media_had_tap_client
            .get(&channel)
            .copied()
            .unwrap_or(false);
        let lost_last_consumer = had_tap_client && !has_tap_client;

        connect_gen_updates.push((channel, connect_gen));
        tap_presence_updates.push((channel, has_tap_client));

        let is_cluster = ctx
            .injected_media_display
            .get(&channel)
            .copied()
            .unwrap_or(DisplayType::DISPLAY_TYPE_CLUSTER)
            == DisplayType::DISPLAY_TYPE_CLUSTER;

        if new_connection
            && has_tap_client
            && is_cluster
            && matches!(
                state.phase,
                InjectedMediaPhase::FocusSent
                    | InjectedMediaPhase::Started
                    | InjectedMediaPhase::Streaming
            )
        {
            toggle_channels.push((channel, state.last_flags));
        }

        // Reconnect into Idle: re-acquire projected focus so the phone restarts the stream.
        if new_connection
            && has_tap_client
            && is_cluster
            && state.phase == InjectedMediaPhase::Idle
        {
            info!(
                "{} <blue>injected media:</> new tap client on channel {:#04x} in idle phase; re-acquiring projected focus",
                get_name(proxy_type),
                channel
            );
            ready_channels.push((channel, state.last_flags, has_tap_client));
        }

        if lost_last_consumer
            && is_cluster
            && !cfg.inject_force_focus_without_tap
            && matches!(
                state.phase,
                InjectedMediaPhase::FocusSent
                    | InjectedMediaPhase::Started
                    | InjectedMediaPhase::Streaming
            )
        {
            release_channels.push((channel, state.last_flags));
        }

        if !state.phase.awaiting_focus() {
            continue;
        }

        debug!(
            "{} deferred_focus check: ch={:#04x} phase={} tap_client={} force={} media_channels_has_sink={} connect_gen={} seen_connect_gen={} new_connection={}",
            get_name(proxy_type),
            channel,
            injected_phase_label(state.phase),
            has_tap_client,
            cfg.inject_force_focus_without_tap,
            ctx.media_channels.contains_key(&channel),
            connect_gen,
            seen_connect_gen,
            new_connection
        );

        if has_tap_client || cfg.inject_force_focus_without_tap {
            ready_channels.push((channel, state.last_flags, has_tap_client));
        }
    }

    // Reacquire projected focus on fresh cluster tap connections even if we do not
    // currently have injected media runtime state for that channel.
    for (&channel, &display_type) in &ctx.injected_media_display {
        if display_type != DisplayType::DISPLAY_TYPE_CLUSTER {
            continue;
        }
        if ctx.injected_media_state.contains_key(&channel) {
            continue;
        }

        let Some(sink) = ctx.media_channels.get(&channel) else {
            continue;
        };

        let has_tap_client = sink.has_subscribers();
        let connect_gen = sink.client_connect_generation();
        let seen_connect_gen = ctx
            .injected_media_connect_gen
            .get(&channel)
            .copied()
            .unwrap_or_default();
        let new_connection = connect_gen > seen_connect_gen;

        connect_gen_updates.push((channel, connect_gen));
        tap_presence_updates.push((channel, has_tap_client));

        if new_connection && has_tap_client {
            debug!(
                "{} deferred_focus check: ch={:#04x} phase=absent tap_client=true force={} media_channels_has_sink=true connect_gen={} seen_connect_gen={} new_connection=true",
                get_name(proxy_type),
                channel,
                cfg.inject_force_focus_without_tap,
                connect_gen,
                seen_connect_gen,
            );
            toggle_channels.push((channel, ENCRYPTED | FRAME_TYPE_FIRST | FRAME_TYPE_LAST));
        }
    }

    for (channel, connect_gen) in connect_gen_updates {
        ctx.injected_media_connect_gen.insert(channel, connect_gen);
    }

    for (channel, has_tap_client) in tap_presence_updates {
        ctx.injected_media_had_tap_client
            .insert(channel, has_tap_client);
    }

    for (channel, flags) in release_channels {
        let mut release_focus_pkt = Packet {
            channel,
            flags,
            final_length: None,
            payload: Vec::new(),
        };
        rewrite_video_focus_notification(
            &mut release_focus_pkt,
            VideoFocusMode::VIDEO_FOCUS_NATIVE,
            true,
        )?;

        info!(
            "{} <blue>injected media:</> last tap client disconnected on channel <b>{:#04x}</>; releasing projected focus",
            get_name(proxy_type),
            channel
        );

        match tx.try_send(release_focus_pkt) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    "{} <yellow>cluster focus release backpressure:</> queue full while sending native focus for channel <b>{:#04x}</>; will retry",
                    get_name(proxy_type),
                    channel
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err("injected focus queue closed".into());
            }
        }
    }

    for (channel, flags) in toggle_channels {
        let mut drop_focus_pkt = Packet {
            channel,
            flags,
            final_length: None,
            payload: Vec::new(),
        };
        rewrite_video_focus_notification(
            &mut drop_focus_pkt,
            VideoFocusMode::VIDEO_FOCUS_NATIVE,
            true,
        )?;

        let mut project_focus_pkt = Packet {
            channel,
            flags,
            final_length: None,
            payload: Vec::new(),
        };
        rewrite_video_focus_notification(
            &mut project_focus_pkt,
            VideoFocusMode::VIDEO_FOCUS_PROJECTED,
            true,
        )?;

        info!(
            "{} <blue>injected media:</> cluster tap client connected on channel <b>{:#04x}</>; toggling VIDEO_FOCUS_NOTIFICATION native→projected",
            get_name(proxy_type),
            channel
        );

        let mut drop_sent = false;
        match tx.try_send(drop_focus_pkt) {
            Ok(()) => {
                drop_sent = true;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    "{} <yellow>cluster focus toggle backpressure:</> queue full while sending native focus for channel <b>{:#04x}</>; will retry",
                    get_name(proxy_type),
                    channel
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err("injected focus queue closed".into());
            }
        }

        match tx.try_send(project_focus_pkt) {
            Ok(()) => {
                debug!(
                    "{} cluster focus toggle on channel <b>{:#04x}</>: native_sent={} projected_sent=true",
                    get_name(proxy_type),
                    channel,
                    drop_sent
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    "{} <yellow>cluster focus toggle backpressure:</> queue full while sending projected focus for channel <b>{:#04x}</>; will retry",
                    get_name(proxy_type),
                    channel
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err("injected focus queue closed".into());
            }
        }
    }

    for (channel, flags, has_tap_client) in ready_channels {
        let mut focus_pkt = Packet {
            channel,
            flags,
            final_length: None,
            payload: Vec::new(),
        };
        rewrite_video_focus_notification(
            &mut focus_pkt,
            VideoFocusMode::VIDEO_FOCUS_PROJECTED,
            true,
        )?;
        info!(
            "{} <blue>injected media:</> synthesized VIDEO_FOCUS_NOTIFICATION on channel <b>{:#04x}</> tap_client={} force={}",
            get_name(proxy_type),
            channel,
            has_tap_client,
            cfg.inject_force_focus_without_tap
        );

        match tx.try_send(focus_pkt) {
            Ok(()) => {
                if let Some(state) = ctx.injected_media_state.get_mut(&channel) {
                    let prev_phase = state.phase;
                    state.phase = InjectedMediaPhase::FocusSent;
                    trace_injected_phase_transition(
                        cfg,
                        proxy_type,
                        channel,
                        ctx.injected_media_display
                            .get(&channel)
                            .copied()
                            .unwrap_or(DisplayType::DISPLAY_TYPE_CLUSTER),
                        prev_phase,
                        state.phase,
                        "deferred_focus",
                        state.session_id,
                        state.ack_counter,
                    );
                }
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    "{} <yellow>deferred focus backpressure:</> queue full while emitting focus for channel <b>{:#04x}</>; will retry",
                    get_name(proxy_type),
                    channel
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err("injected focus queue closed".into());
            }
        }
    }

    Ok(())
}

fn emulate_injected_media_packet(
    proxy_type: ProxyType,
    pkt: &mut Packet,
    ctx: &mut ModifyContext,
    reassembled_frame: Option<&[u8]>,
    has_fragment_state: bool,
    cfg: &AppConfig,
) -> Result<bool> {
    let message_id = first_fragment_message_id(pkt)
        .or_else(|| {
            reassembled_frame.and_then(|frame| {
                if frame.len() >= 2 {
                    Some(u16::from_be_bytes([frame[0], frame[1]]))
                } else {
                    None
                }
            })
        })
        .or_else(|| {
            if has_fragment_state {
                Some(MEDIA_MESSAGE_DATA.value() as u16)
            } else {
                None
            }
        });

    let Some(message_id) = message_id else {
        return Ok(false);
    };

    let data = reassembled_frame
        .and_then(|frame| frame.get(2..))
        .or_else(|| pkt.payload.get(2..))
        .unwrap_or_default();
    let state = ctx.injected_media_state.entry(pkt.channel).or_default();
    let display_type = ctx
        .injected_media_display
        .get(&pkt.channel)
        .copied()
        .unwrap_or(DisplayType::DISPLAY_TYPE_CLUSTER);
    let max_unacked = injected_max_unacked(display_type);

    match protos::MediaMessageId::from_i32(message_id.into()).unwrap_or(MEDIA_MESSAGE_DATA) {
        MEDIA_MESSAGE_SETUP => {
            ctx.injected_media_counters.setup_seen =
                ctx.injected_media_counters.setup_seen.saturating_add(1);
            let prev_phase = state.phase;
            state.phase = InjectedMediaPhase::SetupSeen;
            state.session_id = 0;
            state.ack_counter = 0;
            state.last_flags = pkt.flags;
            trace_injected_phase_transition(
                cfg,
                proxy_type,
                pkt.channel,
                display_type,
                prev_phase,
                state.phase,
                "media_setup",
                state.session_id,
                state.ack_counter,
            );
            info!(
                "{} <blue>injected media:</> SETUP on channel <b>{:#04x}</> display={:?}",
                get_name(proxy_type),
                pkt.channel,
                display_type
            );
            // Virtual sink: immediately advertise readiness and keep unacked window tiny.
            rewrite_media_config_ready(pkt, max_unacked)?;
            ctx.injected_media_counters.config_replies =
                ctx.injected_media_counters.config_replies.saturating_add(1);
            maybe_log_injected_media_counters(
                proxy_type,
                &ctx.injected_media_counters,
                pkt.channel,
                display_type,
            );
            Ok(true)
        }
        MEDIA_MESSAGE_START => {
            ctx.injected_media_counters.start_seen =
                ctx.injected_media_counters.start_seen.saturating_add(1);
            if let Ok(msg) = Start::parse_from_bytes(data) {
                let prev_phase = state.phase;
                state.session_id = msg.session_id();
                state.ack_counter = 0;
                state.phase = InjectedMediaPhase::Started;
                state.last_flags = pkt.flags;
                state.trace_after_start = 128;
                trace_injected_phase_transition(
                    cfg,
                    proxy_type,
                    pkt.channel,
                    display_type,
                    prev_phase,
                    state.phase,
                    "media_start",
                    state.session_id,
                    state.ack_counter,
                );
                info!(
                    "{} <blue>injected media:</> START on channel <b>{:#04x}</> display={:?} session_id={} cfg_index={}",
                    get_name(proxy_type),
                    pkt.channel,
                    display_type,
                    msg.session_id(),
                    msg.configuration_index()
                );
            } else {
                warn!(
                    "{} <yellow>injected media:</> START parse failed on channel <b>{:#04x}</> display={:?}",
                    get_name(proxy_type),
                    pkt.channel,
                    display_type
                );
            }
            // Native sinks do not emit a control reply for START. Emitting CONFIG here
            // can stall phone-side control flow right after injected startup.
            maybe_log_injected_media_counters(
                proxy_type,
                &ctx.injected_media_counters,
                pkt.channel,
                display_type,
            );
            Ok(false)
        }
        MEDIA_MESSAGE_DATA => {
            if reassembled_frame.is_none() && (has_fragment_state || pkt.flags & FRAME_TYPE_MASK != (FRAME_TYPE_FIRST | FRAME_TYPE_LAST)) {
                debug!(
                    "{} <blue>injected media:</> fragment_wait on channel <b>{:#04x}</> display={:?} phase={}",
                    get_name(proxy_type),
                    pkt.channel,
                    display_type,
                    injected_phase_label(state.phase)
                );
                return Ok(false);
            }

            ctx.injected_media_counters.data_seen =
                ctx.injected_media_counters.data_seen.saturating_add(1);

            if state.phase.can_stream() {
                let prev_phase = state.phase;
                state.phase = InjectedMediaPhase::Streaming;
                state.ack_counter = state.ack_counter.saturating_add(1);
                trace_injected_phase_transition(
                    cfg,
                    proxy_type,
                    pkt.channel,
                    display_type,
                    prev_phase,
                    state.phase,
                    "media_data",
                    state.session_id,
                    state.ack_counter,
                );
                rewrite_media_ack(pkt, state.session_id, state.ack_counter)?;
                ctx.injected_media_counters.ack_replies =
                    ctx.injected_media_counters.ack_replies.saturating_add(1);
                if cfg.trace_channel_flow && (state.ack_counter == 1 || state.ack_counter % 32 == 0)
                {
                    info!(
                        "{} <cyan>flow trace:</> injected ACK channel=<b>{:#04x}</> display={:?} session_id={} ack={}",
                        get_name(proxy_type),
                        pkt.channel,
                        display_type,
                        state.session_id,
                        state.ack_counter
                    );
                }
                if state.ack_counter == 1 || state.ack_counter % 256 == 0 {
                    info!(
                        "{} <blue>injected media:</> DATA ack on channel <b>{:#04x}</> display={:?} session_id={} ack={}",
                        get_name(proxy_type),
                        pkt.channel,
                        display_type,
                        state.session_id,
                        state.ack_counter
                    );
                }
                maybe_log_injected_media_counters(
                    proxy_type,
                    &ctx.injected_media_counters,
                    pkt.channel,
                    display_type,
                );
                return Ok(true);
            }

            warn!(
                "{} <yellow>injected media:</> state_not_started on channel <b>{:#04x}</> display={:?} phase={}",
                get_name(proxy_type),
                pkt.channel,
                display_type,
                injected_phase_label(state.phase)
            );
            maybe_log_injected_media_counters(
                proxy_type,
                &ctx.injected_media_counters,
                pkt.channel,
                display_type,
            );
            Ok(false)
        }
        MEDIA_MESSAGE_STOP => {
            ctx.injected_media_counters.stop_seen =
                ctx.injected_media_counters.stop_seen.saturating_add(1);
            info!(
                "{} <blue>injected media:</> STOP on channel <b>{:#04x}</> display={:?} session_id={} final_ack={}",
                get_name(proxy_type),
                pkt.channel,
                display_type,
                state.session_id,
                state.ack_counter
            );
            let prev_phase = state.phase;
            state.phase = InjectedMediaPhase::Idle;
            state.ack_counter = 0;
            state.session_id = 0;
            state.last_flags = pkt.flags;
            trace_injected_phase_transition(
                cfg,
                proxy_type,
                pkt.channel,
                display_type,
                prev_phase,
                state.phase,
                "media_stop",
                state.session_id,
                state.ack_counter,
            );
            maybe_log_injected_media_counters(
                proxy_type,
                &ctx.injected_media_counters,
                pkt.channel,
                display_type,
            );
            Ok(false)
        }
        MEDIA_MESSAGE_VIDEO_FOCUS_REQUEST => {
            ctx.injected_media_counters.focus_requests_seen =
                ctx.injected_media_counters.focus_requests_seen.saturating_add(1);

            let mut requested_focus = VideoFocusMode::VIDEO_FOCUS_PROJECTED;
            let mut reason = VideoFocusReason::UNKNOWN;
            if let Ok(msg) = VideoFocusRequestNotification::parse_from_bytes(data) {
                requested_focus = msg.mode();
                reason = msg.reason();
            } else {
                warn!(
                    "{} <yellow>injected media:</> VIDEO_FOCUS_REQUEST parse failed on channel <b>{:#04x}</> display={:?}",
                    get_name(proxy_type),
                    pkt.channel,
                    display_type
                );
            }

            info!(
                "{} <blue>injected media:</> VIDEO_FOCUS_REQUEST on channel <b>{:#04x}</> display={:?} focus={:?} reason={:?}",
                get_name(proxy_type),
                pkt.channel,
                display_type,
                requested_focus,
                reason
            );

            rewrite_video_focus_notification(pkt, requested_focus, false)?;
            let prev_phase = state.phase;
            state.phase = InjectedMediaPhase::FocusSent;
            trace_injected_phase_transition(
                cfg,
                proxy_type,
                pkt.channel,
                display_type,
                prev_phase,
                state.phase,
                "video_focus_request",
                state.session_id,
                state.ack_counter,
            );
            ctx.injected_media_counters.focus_replies =
                ctx.injected_media_counters.focus_replies.saturating_add(1);
            maybe_log_injected_media_counters(
                proxy_type,
                &ctx.injected_media_counters,
                pkt.channel,
                display_type,
            );
            Ok(true)
        }
        _ => {
            info!(
                "{} <blue>injected media:</> passthrough message_id=0x{:04X} on channel <b>{:#04x}</> display={:?}",
                get_name(proxy_type),
                message_id,
                pkt.channel,
                display_type
            );
            maybe_log_injected_media_counters(
                proxy_type,
                &ctx.injected_media_counters,
                pkt.channel,
                display_type,
            );
            Ok(false)
        }
    }
}

async fn recv_packet_with_timeout(
    proxy_type: ProxyType,
    queue_name: &str,
    stage: &str,
    rx: &mut Receiver<Packet>,
) -> Result<Packet> {
    debug!(
        "{} waiting for packet on {} ({})",
        get_name(proxy_type),
        queue_name,
        stage
    );

    let maybe_pkt = timeout(Duration::from_millis(HANDSHAKE_PACKET_TIMEOUT_MS), rx.recv())
        .await
        .with_context(|| {
            format!(
                "{} timeout waiting on {} ({})",
                get_name(proxy_type),
                queue_name,
                stage
            )
        })?;

    let pkt = maybe_pkt.ok_or_else(|| {
        format!(
            "{} channel closed while waiting on {} ({})",
            get_name(proxy_type),
            queue_name,
            stage
        )
    })?;

    debug!(
        "{} received packet on {} ({})",
        get_name(proxy_type),
        queue_name,
        stage
    );

    Ok(pkt)
}

fn packet_message_id(pkt: &Packet) -> Option<u16> {
    if pkt.payload.len() < 2 {
        return None;
    }
    Some(u16::from_be_bytes([pkt.payload[0], pkt.payload[1]]))
}

async fn transmit_packet_runtime<A: Endpoint<A>>(
    proxy_type: ProxyType,
    direction: &str,
    pkt: &Packet,
    logical_msg_id: Option<u16>,
    device: &mut IoDevice<A>,
) -> Result<()> {
    let msg_id = logical_msg_id
        .or_else(|| packet_message_id(pkt))
        .map(|v| format!("0x{v:04X}"))
        .unwrap_or_else(|| "n/a".to_string());

    let timeout_ms = if pkt.channel == 0 {
        RUNTIME_TRANSMIT_TIMEOUT_CONTROL_MS
    } else {
        RUNTIME_TRANSMIT_TIMEOUT_DATA_MS
    };

    let tx_res = timeout(
        Duration::from_millis(timeout_ms),
        pkt.transmit(device),
    )
    .await
    .with_context(|| {
        format!(
            "{} runtime transmit timeout direction={} channel={:#04x} msg_id={} payload_len={} timeout_ms={}",
            get_name(proxy_type),
            direction,
            pkt.channel,
            msg_id,
            pkt.payload.len(),
            timeout_ms
        )
    })?;

    tx_res.with_context(|| {
        format!(
            "{} runtime transmit error direction={} channel={:#04x} msg_id={} payload_len={}",
            get_name(proxy_type),
            direction,
            pkt.channel,
            msg_id,
            pkt.payload.len()
        )
    })?;

    Ok(())
}

#[derive(Default)]
struct DirectionControlStats {
    ingress: u64,
    egress: u64,
    queue_wait_count: u64,
    tx_count: u64,
    queue_wait_ms_total: u64,
    tx_ms_total: u64,
    queue_wait_ms_max: u64,
    tx_ms_max: u64,
    queue_wait_samples_ms: VecDeque<u64>,
    tx_samples_ms: VecDeque<u64>,
}

impl DirectionControlStats {
    fn note_ingress(&mut self) {
        self.ingress = self.ingress.saturating_add(1);
    }

    fn note_egress(&mut self) {
        self.egress = self.egress.saturating_add(1);
    }

    fn push_window(samples: &mut VecDeque<u64>, elapsed_ms: u64) {
        if samples.len() >= CONTROL_STATS_WINDOW_SAMPLES {
            samples.pop_front();
        }
        samples.push_back(elapsed_ms);
    }

    fn percentile(samples: &VecDeque<u64>, numerator: usize, denominator: usize) -> u64 {
        if samples.is_empty() {
            return 0;
        }

        let mut ordered: Vec<u64> = samples.iter().copied().collect();
        ordered.sort_unstable();

        let n = ordered.len();
        let idx = ((n - 1) * numerator) / denominator;
        ordered[idx]
    }

    fn note_queue_wait(&mut self, elapsed_ms: u64) {
        self.queue_wait_count = self.queue_wait_count.saturating_add(1);
        self.queue_wait_ms_total = self.queue_wait_ms_total.saturating_add(elapsed_ms);
        self.queue_wait_ms_max = self.queue_wait_ms_max.max(elapsed_ms);
        Self::push_window(&mut self.queue_wait_samples_ms, elapsed_ms);
    }

    fn note_tx(&mut self, elapsed_ms: u64) {
        self.tx_count = self.tx_count.saturating_add(1);
        self.tx_ms_total = self.tx_ms_total.saturating_add(elapsed_ms);
        self.tx_ms_max = self.tx_ms_max.max(elapsed_ms);
        Self::push_window(&mut self.tx_samples_ms, elapsed_ms);
    }

    fn queue_avg_ms(&self) -> u64 {
        if self.queue_wait_count == 0 {
            0
        } else {
            self.queue_wait_ms_total / self.queue_wait_count
        }
    }

    fn tx_avg_ms(&self) -> u64 {
        if self.tx_count == 0 {
            0
        } else {
            self.tx_ms_total / self.tx_count
        }
    }
}

#[derive(Default)]
struct ControlRuntimeStats {
    rx_to_endpoint: DirectionControlStats,
    endpoint_to_tx: DirectionControlStats,
}

impl ControlRuntimeStats {
    fn maybe_log(&self, proxy_type: ProxyType) {
        let samples = self
            .rx_to_endpoint
            .egress
            .saturating_add(self.endpoint_to_tx.egress);
        if samples == 0 || samples % CONTROL_STATS_LOG_EVERY != 0 {
            return;
        }

        let rx_queue_p50 = DirectionControlStats::percentile(
            &self.rx_to_endpoint.queue_wait_samples_ms,
            50,
            100,
        );
        let rx_queue_p95 = DirectionControlStats::percentile(
            &self.rx_to_endpoint.queue_wait_samples_ms,
            95,
            100,
        );
        let rx_queue_p99 = DirectionControlStats::percentile(
            &self.rx_to_endpoint.queue_wait_samples_ms,
            99,
            100,
        );

        let rx_tx_p50 = DirectionControlStats::percentile(
            &self.rx_to_endpoint.tx_samples_ms,
            50,
            100,
        );
        let rx_tx_p95 = DirectionControlStats::percentile(
            &self.rx_to_endpoint.tx_samples_ms,
            95,
            100,
        );
        let rx_tx_p99 = DirectionControlStats::percentile(
            &self.rx_to_endpoint.tx_samples_ms,
            99,
            100,
        );

        let endpoint_queue_p50 = DirectionControlStats::percentile(
            &self.endpoint_to_tx.queue_wait_samples_ms,
            50,
            100,
        );
        let endpoint_queue_p95 = DirectionControlStats::percentile(
            &self.endpoint_to_tx.queue_wait_samples_ms,
            95,
            100,
        );
        let endpoint_queue_p99 = DirectionControlStats::percentile(
            &self.endpoint_to_tx.queue_wait_samples_ms,
            99,
            100,
        );

        let endpoint_tx_p50 = DirectionControlStats::percentile(
            &self.endpoint_to_tx.tx_samples_ms,
            50,
            100,
        );
        let endpoint_tx_p95 = DirectionControlStats::percentile(
            &self.endpoint_to_tx.tx_samples_ms,
            95,
            100,
        );
        let endpoint_tx_p99 = DirectionControlStats::percentile(
            &self.endpoint_to_tx.tx_samples_ms,
            99,
            100,
        );

        info!(
            "{} <blue>control stats:</> rx_to_endpoint ingress={} egress={} queue_avg_ms={} queue_max_ms={} queue_p50_ms={} queue_p95_ms={} queue_p99_ms={} tx_avg_ms={} tx_max_ms={} tx_p50_ms={} tx_p95_ms={} tx_p99_ms={} | endpoint_to_tx ingress={} egress={} queue_avg_ms={} queue_max_ms={} queue_p50_ms={} queue_p95_ms={} queue_p99_ms={} tx_avg_ms={} tx_max_ms={} tx_p50_ms={} tx_p95_ms={} tx_p99_ms={}",
            get_name(proxy_type),
            self.rx_to_endpoint.ingress,
            self.rx_to_endpoint.egress,
            self.rx_to_endpoint.queue_avg_ms(),
            self.rx_to_endpoint.queue_wait_ms_max,
            rx_queue_p50,
            rx_queue_p95,
            rx_queue_p99,
            self.rx_to_endpoint.tx_avg_ms(),
            self.rx_to_endpoint.tx_ms_max,
            rx_tx_p50,
            rx_tx_p95,
            rx_tx_p99,
            self.endpoint_to_tx.ingress,
            self.endpoint_to_tx.egress,
            self.endpoint_to_tx.queue_avg_ms(),
            self.endpoint_to_tx.queue_wait_ms_max,
            endpoint_queue_p50,
            endpoint_queue_p95,
            endpoint_queue_p99,
            self.endpoint_to_tx.tx_avg_ms(),
            self.endpoint_to_tx.tx_ms_max,
            endpoint_tx_p50,
            endpoint_tx_p95,
            endpoint_tx_p99,
        );
    }
}

/// rust-openssl doesn't support BIO_s_mem
/// This SslMemBuf is about to provide `Read` and `Write` implementations
/// to be used with `openssl::ssl::SslStream`
/// more info:
/// https://github.com/sfackler/rust-openssl/issues/1697
type LocalDataBuffer = Arc<Mutex<VecDeque<u8>>>;
#[derive(Clone)]
pub struct SslMemBuf {
    /// a data buffer that the server writes to and the client reads from
    pub server_stream: LocalDataBuffer,
    /// a data buffer that the client writes to and the server reads from
    pub client_stream: LocalDataBuffer,
}

// Read implementation used internally by OpenSSL
impl Read for SslMemBuf {
    fn read(&mut self, buf: &mut [u8]) -> std::result::Result<usize, std::io::Error> {
        self.client_stream.lock().unwrap().read(buf)
    }
}

// Write implementation used internally by OpenSSL
impl Write for SslMemBuf {
    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        self.server_stream.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        self.server_stream.lock().unwrap().flush()
    }
}

// Own functions for accessing shared data
impl SslMemBuf {
    fn read_to(&mut self, buf: &mut Vec<u8>) -> std::result::Result<usize, std::io::Error> {
        self.server_stream.lock().unwrap().read_to_end(buf)
    }
    fn write_from(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        self.client_stream.lock().unwrap().write(buf)
    }
}

pub struct Packet {
    pub channel: u8,
    pub flags: u8,
    pub final_length: Option<u32>,
    pub payload: Vec<u8>,
}

impl Packet {
    /// payload encryption if needed
    async fn encrypt_payload(
        &mut self,
        mem_buf: &mut SslMemBuf,
        server: &mut openssl::ssl::SslStream<SslMemBuf>,
    ) -> Result<()> {
        if (self.flags & ENCRYPTED) == ENCRYPTED {
            // save plain data for encryption
            server.ssl_write(&self.payload)?;
            // read encrypted data
            let mut res: Vec<u8> = Vec::new();
            mem_buf.read_to(&mut res)?;
            self.payload = res;
        }

        Ok(())
    }

    /// payload decryption if needed
    async fn decrypt_payload(
        &mut self,
        mem_buf: &mut SslMemBuf,
        server: &mut openssl::ssl::SslStream<SslMemBuf>,
    ) -> Result<()> {
        if (self.flags & ENCRYPTED) == ENCRYPTED {
            // save encrypted data
            mem_buf.write_from(&self.payload)?;
            // read plain data
            let mut res: Vec<u8> = Vec::new();
            server.read_to_end(&mut res)?;
            self.payload = res;
        }

        Ok(())
    }

    /// composes a final frame and transmits it to endpoint device (HU/MD)
    async fn transmit<A: Endpoint<A>>(
        &self,
        device: &mut IoDevice<A>,
    ) -> std::result::Result<usize, std::io::Error> {
        let len = self.payload.len() as u16;
        let mut frame: Vec<u8> = vec![];
        frame.push(self.channel);
        frame.push(self.flags);
        frame.push((len >> 8) as u8);
        frame.push((len & 0xff) as u8);
        if let Some(final_len) = self.final_length {
            // adding addional 4-bytes of final_len header
            frame.push((final_len >> 24) as u8);
            frame.push((final_len >> 16) as u8);
            frame.push((final_len >> 8) as u8);
            frame.push((final_len & 0xff) as u8);
        }
        match device {
            IoDevice::UsbWriter(device, _) => {
                frame.append(&mut self.payload.clone());
                let mut dev = device.borrow_mut();
                dev.write(&frame).await
            }
            IoDevice::EndpointIo(device) => {
                frame.append(&mut self.payload.clone());
                device.write(frame).submit().await.0
            }
            IoDevice::TcpStreamIo(device) => {
                frame.append(&mut self.payload.clone());
                device.write(frame).submit().await.0
            }
            _ => todo!(),
        }
    }

    /// decapsulates SSL payload and writes to SslStream
    async fn ssl_decapsulate_write(&self, mem_buf: &mut SslMemBuf) -> Result<()> {
        let message_type = u16::from_be_bytes(self.payload[0..=1].try_into()?);
        if message_type == ControlMessageType::MESSAGE_ENCAPSULATED_SSL as u16 {
            mem_buf.write_from(&self.payload[2..])?;
        }
        Ok(())
    }
}

impl fmt::Display for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "packet dump:\n")?;
        write!(f, " channel: {:02X}\n", self.channel)?;
        write!(f, " flags: {:02X}\n", self.flags)?;
        write!(f, " final length: {:04X?}\n", self.final_length)?;
        write!(f, " payload: {:02X?}\n", self.payload.clone().into_iter())?;

        Ok(())
    }
}

/// shows packet/message contents as pretty string for debug
pub async fn pkt_debug(
    proxy_type: ProxyType,
    hexdump: HexdumpLevel,
    hex_requested: HexdumpLevel,
    pkt: &Packet,
) -> Result<()> {
    // don't run further if we are not in Debug mode
    if !log_enabled!(Level::Debug) {
        return Ok(());
    }

    // if for some reason we have too small packet, bail out
    if pkt.payload.len() < 2 {
        return Ok(());
    }
    // message_id is the first 2 bytes of payload
    let message_id: i32 = u16::from_be_bytes(pkt.payload[0..=1].try_into()?).into();

    // trying to obtain an Enum from message_id
    let control = protos::ControlMessageType::from_i32(message_id);
    debug!("message_id = {:04X}, {:?}", message_id, control);
    if hex_requested >= hexdump {
        debug!("{} {:?} {}", get_name(proxy_type), hexdump, pkt);
    }

    // parsing data
    let data = &pkt.payload[2..]; // start of message data
    let message: &dyn MessageDyn = match control.unwrap_or(MESSAGE_UNEXPECTED_MESSAGE) {
        MESSAGE_BYEBYE_REQUEST => &ByeByeRequest::parse_from_bytes(data)?,
        MESSAGE_BYEBYE_RESPONSE => &ByeByeResponse::parse_from_bytes(data)?,
        MESSAGE_AUTH_COMPLETE => &AuthResponse::parse_from_bytes(data)?,
        MESSAGE_SERVICE_DISCOVERY_REQUEST => &ServiceDiscoveryRequest::parse_from_bytes(data)?,
        MESSAGE_SERVICE_DISCOVERY_RESPONSE => &ServiceDiscoveryResponse::parse_from_bytes(data)?,
        MESSAGE_SERVICE_DISCOVERY_UPDATE => &ServiceDiscoveryUpdate::parse_from_bytes(data)?,
        MESSAGE_PING_REQUEST => &PingRequest::parse_from_bytes(data)?,
        MESSAGE_PING_RESPONSE => &PingResponse::parse_from_bytes(data)?,
        MESSAGE_NAV_FOCUS_REQUEST => &NavFocusRequestNotification::parse_from_bytes(data)?,
        MESSAGE_CHANNEL_OPEN_RESPONSE => &ChannelOpenResponse::parse_from_bytes(data)?,
        MESSAGE_CHANNEL_OPEN_REQUEST => &ChannelOpenRequest::parse_from_bytes(data)?,
        MESSAGE_AUDIO_FOCUS_REQUEST => &AudioFocusRequestNotification::parse_from_bytes(data)?,
        MESSAGE_AUDIO_FOCUS_NOTIFICATION => &AudioFocusNotification::parse_from_bytes(data)?,
        _ => return Ok(()),
    };
    // show pretty string from the message
    debug!("{}", print_to_string_pretty(message));

    Ok(())
}

/// packet modification hook
pub async fn pkt_modify_hook(
    proxy_type: ProxyType,
    flow: PacketFlow,
    pkt: &mut Packet,
    ctx: &mut ModifyContext,
    tap_media: bool,
    sensor_channel: Arc<tokio::sync::Mutex<Option<u8>>>,
    input_channel: Arc<tokio::sync::Mutex<Option<u8>>>,
    cfg: &AppConfig,
    config: &mut SharedConfig,
) -> Result<bool> {
    // if for some reason we have too small packet, bail out
    if pkt.payload.len() < 2 {
        return Ok(false);
    }

    // message_id is the first 2 bytes of payload
    let message_id: i32 = u16::from_be_bytes(pkt.payload[0..=1].try_into()?).into();
    let data = &pkt.payload[2..]; // start of message data

    // handling data on sensor channel
    if let Some(ch) = ctx.sensor_channel {
        if ch == pkt.channel {
            match protos::SensorMessageId::from_i32(message_id).unwrap_or(SENSOR_MESSAGE_ERROR) {
                SENSOR_MESSAGE_REQUEST => {
                    if let Ok(msg) = SensorRequest::parse_from_bytes(data) {
                        if msg.type_() == SensorType::SENSOR_VEHICLE_ENERGY_MODEL_DATA {
                            debug!(
                                "additional SENSOR_MESSAGE_REQUEST for {:?}, making a response with success...",
                                msg.type_()
                            );
                            let mut response = SensorResponse::new();
                            response.set_status(MessageStatus::STATUS_SUCCESS);

                            let mut payload: Vec<u8> = response.write_to_bytes()?;
                            payload.insert(0, ((SENSOR_MESSAGE_RESPONSE as u16) >> 8) as u8);
                            payload.insert(1, ((SENSOR_MESSAGE_RESPONSE as u16) & 0xff) as u8);

                            let reply = Packet {
                                channel: ch,
                                flags: ENCRYPTED | FRAME_TYPE_FIRST | FRAME_TYPE_LAST,
                                final_length: None,
                                payload: payload,
                            };
                            *pkt = reply;

                            // start EV battery logger if neded
                            if let Some(path) = &cfg.ev_battery_logger {
                                ctx.ev_tx
                                    .send(EvTaskCommand::Start(path.to_string()))
                                    .await?;
                            }

                            // return true => send own reply without processing
                            return Ok(true);
                        }
                    }
                }
                SENSOR_MESSAGE_BATCH => {
                    if let Ok(mut msg) = SensorBatch::parse_from_bytes(data) {
                        if cfg.video_in_motion {
                            // === DRIVING STATUS: must be UNRESTRICTED (0) ===
                            // This is the primary flag AA checks. Value is a bitmask:
                            // 0 = unrestricted, 1 = no video, 2 = no keyboard, etc.
                            if !msg.driving_status_data.is_empty() {
                                msg.driving_status_data[0].set_status(0);
                            }

                            // === GEAR: force PARK ===
                            if !msg.gear_data.is_empty() {
                                msg.gear_data[0].set_gear(GEAR_PARK);
                            }

                            // === PARKING BRAKE: engaged ===
                            // Modern AA cross-checks parking brake with gear/speed.
                            if !msg.parking_brake_data.is_empty() {
                                msg.parking_brake_data[0].set_parking_brake(true);
                            }

                            // === VEHICLE SPEED: zero ===
                            // SpeedData.speed_e3 is speed in m/s * 1000. Zero = stopped.
                            if !msg.speed_data.is_empty() {
                                msg.speed_data[0].set_speed_e3(0);
                                // Also ensure cruise control is disengaged
                                msg.speed_data[0].set_cruise_engaged(false);
                            }

                            // === GPS/LOCATION: zero speed, keep position ===
                            // LocationData.speed_e3 is GPS-derived speed.
                            // Modern AA compares this against SpeedData for consistency.
                            if !msg.location_data.is_empty() {
                                msg.location_data[0].set_speed_e3(0);
                                // Zero bearing = not turning
                                msg.location_data[0].set_bearing_e6(0);
                            }

                            // === ACCELEROMETER: gravity only (stationary) ===
                            // A parked car only feels gravity on Z axis (~9810 mm/s²).
                            // Any X/Y acceleration implies movement/turning.
                            if !msg.accelerometer_data.is_empty() {
                                msg.accelerometer_data[0].set_acceleration_x_e3(0);
                                msg.accelerometer_data[0].set_acceleration_y_e3(0);
                                msg.accelerometer_data[0].set_acceleration_z_e3(9810);
                            }

                            // === GYROSCOPE: zero rotation ===
                            // Any rotation speed implies the vehicle is turning.
                            if !msg.gyroscope_data.is_empty() {
                                msg.gyroscope_data[0].set_rotation_speed_x_e3(0);
                                msg.gyroscope_data[0].set_rotation_speed_y_e3(0);
                                msg.gyroscope_data[0].set_rotation_speed_z_e3(0);
                            }

                            // === DEAD RECKONING: zero wheel speed + steering ===
                            // Wheel speed ticks and steering angle are used by Toyota
                            // and other modern HUs as independent motion verification.
                            if !msg.dead_reckoning_data.is_empty() {
                                msg.dead_reckoning_data[0].set_steering_angle_e1(0);
                                msg.dead_reckoning_data[0].wheel_speed_e3.clear();
                                // Push four zero values for the four wheels
                                msg.dead_reckoning_data[0].wheel_speed_e3.push(0);
                                msg.dead_reckoning_data[0].wheel_speed_e3.push(0);
                                msg.dead_reckoning_data[0].wheel_speed_e3.push(0);
                                msg.dead_reckoning_data[0].wheel_speed_e3.push(0);
                            }

                            // === COMPASS: freeze bearing ===
                            // Changing compass bearing implies turning/moving.
                            if !msg.compass_data.is_empty() {
                                msg.compass_data[0].set_pitch_e6(0);
                                msg.compass_data[0].set_roll_e6(0);
                            }

                            // === RPM: idle engine ===
                            // High RPM with zero speed is suspicious on some HUs.
                            // ~700 RPM idle is realistic for a parked car.
                            if !msg.rpm_data.is_empty() {
                                msg.rpm_data[0].set_rpm_e3(700_000);
                            }

                            // Regenerate payload with ALL spoofed fields
                            pkt.payload = msg.write_to_bytes()?;
                            pkt.payload.insert(0, (message_id >> 8) as u8);
                            pkt.payload.insert(1, (message_id & 0xff) as u8);
                        }
                    }
                }
                _ => (),
            }
            // end sensors processing
            return Ok(false);
        }
    }

    // apply waze workaround on navigation data
    if let Some(ch) = ctx.nav_channel {
        // check for channel and a specific packet header only
        if ch == pkt.channel
            && proxy_type == ProxyType::HeadUnit
            && pkt.payload[0] == 0x80
            && pkt.payload[1] == 0x06
            && pkt.payload[2] == 0x0A
        {
            if let Ok(mut msg) = NavigationState::parse_from_bytes(&data) {
                if msg.steps[0].maneuver.type_() == U_TURN_LEFT {
                    msg.steps[0]
                        .maneuver
                        .as_mut()
                        .unwrap()
                        .set_type(U_TURN_RIGHT);
                    info!(
                        "{} swapped U_TURN_LEFT to U_TURN_RIGHT",
                        get_name(proxy_type)
                    );

                    // rewrite payload to new message contents
                    pkt.payload = msg.write_to_bytes()?;
                    // inserting 2 bytes of message_id at the beginning
                    pkt.payload.insert(0, (message_id >> 8) as u8);
                    pkt.payload.insert(1, (message_id & 0xff) as u8);
                    return Ok(false);
                }
            }
            // end navigation service processing
            return Ok(false);
        }
    }

    // if configured, override max_unacked for matching audio channels
    if cfg.audio_max_unacked > 0
        && ctx.audio_channels.contains(&pkt.channel)
        && proxy_type == ProxyType::HeadUnit
    {
        match protos::MediaMessageId::from_i32(message_id).unwrap_or(MEDIA_MESSAGE_DATA) {
            m @ MEDIA_MESSAGE_CONFIG => {
                if let Ok(mut msg) = AudioConfig::parse_from_bytes(&data) {
                    // get previous/original value
                    let prev_val = msg.max_unacked();
                    // set new value
                    msg.set_max_unacked(cfg.audio_max_unacked.into());

                    info!(
                        "{} <yellow>{:?}</>: overriding max audio unacked from <b>{}</> to <b>{}</> for channel: <b>{:#04x}</>",
                        get_name(proxy_type),
                        m,
                        prev_val,
                        cfg.audio_max_unacked,
                        pkt.channel,
                    );

                    // FIXME: this code fragment is used multiple times
                    // rewrite payload to new message contents
                    pkt.payload = msg.write_to_bytes()?;
                    // inserting 2 bytes of message_id at the beginning
                    pkt.payload.insert(0, (message_id >> 8) as u8);
                    pkt.payload.insert(1, (message_id & 0xff) as u8);
                    return Ok(false);
                }
                // end processing
                return Ok(false);
            }
            _ => (),
        }
    }

    // tap media frames for debug streaming (only on MobileDevice path = phone → HU direction)
    if tap_media && proxy_type == ProxyType::MobileDevice {
        if let Some(frame_data) = reassemble_media_packet(&mut ctx.media_fragments, pkt) {
            if frame_data.len() >= 2 {
                if let Some(sink) = ctx.media_channels.get(&pkt.channel).cloned() {
                    tap_media_message(proxy_type, pkt, &sink, &frame_data).await;
                }
            }
        }
    }

    if proxy_type == ProxyType::MobileDevice && ctx.md_sdr_forwarded && pkt.channel != 0 {
        let media_id = message_id as u16;
        match flow {
            PacketFlow::FromEndpoint => push_trace_id(&mut ctx.md_recent_media_from_md, media_id),
            PacketFlow::ToEndpoint => push_trace_id(&mut ctx.md_recent_media_to_md, media_id),
        }

        match protos::MediaMessageId::from_i32(message_id).unwrap_or(MEDIA_MESSAGE_DATA) {
            MEDIA_MESSAGE_SETUP
            | MEDIA_MESSAGE_START
            | MEDIA_MESSAGE_CONFIG
            | MEDIA_MESSAGE_ACK
            | MEDIA_MESSAGE_VIDEO_FOCUS_REQUEST
            | MEDIA_MESSAGE_VIDEO_FOCUS_NOTIFICATION
            | MEDIA_MESSAGE_STOP => {
                log_native_media_packet(proxy_type, flow, pkt, ctx);
            }
            _ => {}
        }

        match protos::MediaMessageId::from_i32(message_id).unwrap_or(MEDIA_MESSAGE_DATA) {
            MEDIA_MESSAGE_SETUP | MEDIA_MESSAGE_START | MEDIA_MESSAGE_CONFIG => {
                if flow == PacketFlow::FromEndpoint && !ctx.md_media_startup_seen {
                    info!(
                        "{} <blue>startup phase:</> first media startup message after SDR: channel={:#04x} id=0x{:04X} {}",
                        get_name(proxy_type),
                        pkt.channel,
                        media_id,
                        media_channel_label(ctx, pkt.channel)
                    );
                }
                if flow == PacketFlow::FromEndpoint {
                    ctx.md_media_startup_seen = true;
                }
            }
            _ => {}
        }
    }

    let control = protos::ControlMessageType::from_i32(message_id);
    let control_allowed_on_service_channel = matches!(
        control,
        Some(MESSAGE_CHANNEL_OPEN_REQUEST | MESSAGE_CHANNEL_OPEN_RESPONSE)
    );

    if pkt.channel != 0 && !control_allowed_on_service_channel {
        return Ok(false);
    }
    // trying to obtain an Enum from message_id
    debug!(
        "message_id = {:04X}, {:?}, proxy_type: {:?}, flow: {:?}",
        message_id, control, proxy_type, flow
    );

    if proxy_type == ProxyType::MobileDevice
        && ctx.md_sdr_forwarded
    {
        let control_id = message_id as u16;
        match flow {
            PacketFlow::FromEndpoint => {
                push_trace_id(&mut ctx.md_recent_control_from_md, control_id);
                ctx.md_control_after_sdr = ctx.md_control_after_sdr.saturating_add(1);
                ctx.md_last_control_after_sdr = Some(message_id);
                if cfg.trace_channel_flow {
                    info!(
                        "{} <cyan>flow trace:</> post-SDR control from phone msg_id=0x{:04X} {:?} count={}",
                        get_name(proxy_type),
                        message_id,
                        control,
                        ctx.md_control_after_sdr
                    );
                } else if ctx.md_control_after_sdr <= 8 {
                    info!(
                        "{} <blue>post-SDR phone control:</> msg_id=0x{:04X} {:?} count={}",
                        get_name(proxy_type),
                        message_id,
                        control,
                        ctx.md_control_after_sdr
                    );
                }
            }
            PacketFlow::ToEndpoint => {
                push_trace_id(&mut ctx.md_recent_control_to_md, control_id);
            }
        }
    }

    // parsing data
    match control.unwrap_or(MESSAGE_UNEXPECTED_MESSAGE) {
        MESSAGE_BYEBYE_REQUEST => {
            log_divergence_counters(proxy_type, ctx, "BYEBYE_REQUEST");
            if cfg.stop_on_disconnect && proxy_type == ProxyType::MobileDevice {
                if let Ok(msg) = ByeByeRequest::parse_from_bytes(data) {
                    if msg.reason.unwrap_or_default() == USER_SELECTION.into() {
                        info!(
                        "{} <bold><blue>Disconnect</> option selected in Android Auto; auto-connect temporarily disabled",
                        get_name(proxy_type),
                    );
                        config.write().await.action_requested = Some(Stop);
                    }
                }
            }
        }
        MESSAGE_SERVICE_DISCOVERY_RESPONSE => {
            let mut msg = match ServiceDiscoveryResponse::parse_from_bytes(data) {
                Err(e) => {
                    error!(
                        "{} error parsing SDR: {}, ignored!",
                        get_name(proxy_type),
                        e
                    );
                    return Ok(false);
                }
                Ok(msg) => msg,
            };

            if log_enabled!(log::Level::Info) {
                info!(
                    "{} <blue>SDR input:</> [{}]",
                    get_name(proxy_type),
                    summarize_services(&msg.services)
                );
                info!(
                    "{} <blue>SDR media detail:</> [{}]",
                    get_name(proxy_type),
                    summarize_media_services(&msg.services)
                );
            }

            if proxy_type == ProxyType::HeadUnit {
                ctx.hu_service_ids = msg.services.iter().map(|s| s.id()).collect();
            }

            // Populate media_channels (channel_id→sink) from the offset→sink map.
            // Both MD and HU need this; for HU, we'll populate again after add_display_services
            // to include injected services (cluster, etc).
            ctx.media_service_labels.clear();
            for svc in msg.services.iter() {
                if let Some(label) = describe_media_service(svc) {
                    ctx.media_service_labels.insert(svc.id() as u8, label);
                }
            }

            if !ctx.media_sinks.is_empty() {
                for svc in msg.services.iter() {
                    let ch = svc.id() as u8;
                    if !svc.media_sink_service.video_configs.is_empty() {
                        let offset = svc.media_sink_service.display_type().value() as u8;
                        if let Some(sink) = ctx.media_sinks.get(&offset).cloned() {
                            ctx.media_channels.insert(ch, sink);
                            debug!(
                                "{} media_channels.insert: ch={:#04x} offset={}",
                                get_name(proxy_type),
                                ch,
                                offset
                            );
                            info!(
                                "{} <blue>media tap:</> video channel <b>{:#04x}</> → port offset <b>{}</> ({:?}, {:?})",
                                get_name(proxy_type),
                                ch,
                                offset,
                                svc.media_sink_service.available_type(),
                                svc.media_sink_service.display_type()
                            );
                        }
                    } else if !svc.media_sink_service.audio_configs.is_empty()
                        || svc.media_sink_service.audio_type.is_some()
                    {
                        // audio offset = audio_type value + 2 (offsets 0-2 reserved for video)
                        let offset = svc.media_sink_service.audio_type().value() as u8 + 2;
                        if let Some(sink) = ctx.media_sinks.get(&offset).cloned() {
                            let audio_config = svc.media_sink_service.audio_configs.first().map(|acfg| {
                                AudioStreamConfig {
                                    sample_rate: acfg.sampling_rate(),
                                    channels: acfg.number_of_channels(),
                                    bits: acfg.number_of_bits(),
                                }
                            });
                            ctx.media_channels.insert(ch, sink);
                            debug!(
                                "{} media_channels.insert: ch={:#04x} offset={}",
                                get_name(proxy_type),
                                ch,
                                offset
                            );
                            if let Some(acfg) = audio_config {
                                info!(
                                    "{} <blue>media tap:</> audio channel <b>{:#04x}</> → port offset <b>{}</> ({:?}, {:?}, {}Hz, {}ch, {}bit)",
                                    get_name(proxy_type),
                                    ch,
                                    offset,
                                    svc.media_sink_service.available_type(),
                                    svc.media_sink_service.audio_type(),
                                    acfg.sample_rate,
                                    acfg.channels,
                                    acfg.bits
                                );
                            } else {
                                info!(
                                    "{} <blue>media tap:</> audio channel <b>{:#04x}</> → port offset <b>{}</> ({:?}, {:?})",
                                    get_name(proxy_type),
                                    ch,
                                    offset,
                                    svc.media_sink_service.available_type(),
                                    svc.media_sink_service.audio_type()
                                );
                            }
                        }
                    }
                }
            }

            // SDR rewriting is HeadUnit-only; MobileDevice sees SDR read-only (for channel map above)
            if proxy_type == ProxyType::MobileDevice {
                ctx.md_sdr_forwarded = true;
                return Ok(false);
            }

            // DPI
            if cfg.dpi > 0 {
                if let Some(svc) = msg
                    .services
                    .iter_mut()
                    .find(|svc| !svc.media_sink_service.video_configs.is_empty())
                {
                    // get previous/original value
                    let prev_val = svc.media_sink_service.video_configs[0].density();
                    // set new value
                    svc.media_sink_service.as_mut().unwrap().video_configs[0]
                        .set_density(cfg.dpi.into());
                    info!(
                        "{} <yellow>{:?}</>: replacing DPI value: from <b>{}</> to <b>{}</>",
                        get_name(proxy_type),
                        control.unwrap(),
                        prev_val,
                        cfg.dpi
                    );
                }
            }

            // disable tts sink
            if cfg.disable_tts_sink {
                while let Some(svc) = msg.services.iter_mut().find(|svc| {
                    !svc.media_sink_service.audio_configs.is_empty()
                        && svc.media_sink_service.audio_type() == AUDIO_STREAM_GUIDANCE
                }) {
                    svc.media_sink_service
                        .as_mut()
                        .unwrap()
                        .set_audio_type(AUDIO_STREAM_SYSTEM_AUDIO);
                }
                info!(
                    "{} <yellow>{:?}</>: TTS sink disabled",
                    get_name(proxy_type),
                    control.unwrap(),
                );
            }

            // disable media sink
            if cfg.disable_media_sink {
                msg.services
                    .retain(|svc| svc.media_sink_service.audio_type() != AUDIO_STREAM_MEDIA);
                info!(
                    "{} <yellow>{:?}</>: media sink disabled",
                    get_name(proxy_type),
                    control.unwrap(),
                );
            }

            // save all audio sink channels in context
            if cfg.audio_max_unacked > 0 {
                for svc in msg
                    .services
                    .iter()
                    .filter(|svc| !svc.media_sink_service.audio_configs.is_empty())
                {
                    ctx.audio_channels.push(svc.id() as u8);
                }
                info!(
                    "{} <blue>media_sink_service:</> channels: <b>{:02x?}</>",
                    get_name(proxy_type),
                    ctx.audio_channels
                );
            }

            // save sensor channel in context
            if cfg.ev || cfg.video_in_motion {
                if let Some(svc) = msg
                    .services
                    .iter()
                    .find(|svc| !svc.sensor_source_service.sensors.is_empty())
                {
                    // set in local context
                    ctx.sensor_channel = Some(svc.id() as u8);
                    // set in REST server context for remote EV requests
                    let mut sc_lock = sensor_channel.lock().await;
                    *sc_lock = Some(svc.id() as u8);

                    info!(
                        "{} <blue>sensor_source_service</> channel is: <b>{:#04x}</>",
                        get_name(proxy_type),
                        svc.id() as u8
                    );
                }
            }

            // save input channel in REST server context for remote key requests
            if let Some(svc) = msg.services.iter().find(|svc| svc.input_source_service.is_some()) {
                let mut ic_lock = input_channel.lock().await;
                *ic_lock = Some(svc.id() as u8);

                info!(
                    "{} <blue>input_source_service</> channel is: <b>{:#04x}</>",
                    get_name(proxy_type),
                    svc.id() as u8
                );
            }

            // save navigation channel in context
            if cfg.waze_lht_workaround {
                if let Some(svc) = msg
                    .services
                    .iter()
                    .find(|svc| svc.navigation_status_service.is_some())
                {
                    // set in local context
                    ctx.nav_channel = Some(svc.id() as u8);

                    info!(
                        "{} <blue>navigation_status_service</> channel is: <b>{:#04x}</>",
                        get_name(proxy_type),
                        svc.id() as u8
                    );
                }
            }

            // remove tap restriction by removing SENSOR_SPEED
            if cfg.remove_tap_restriction {
                if let Some(svc) = msg
                    .services
                    .iter_mut()
                    .find(|svc| !svc.sensor_source_service.sensors.is_empty())
                {
                    svc.sensor_source_service
                        .as_mut()
                        .unwrap()
                        .sensors
                        .retain(|s| s.sensor_type() != SENSOR_SPEED);
                }
            }

            // video_in_motion: strip motion-related sensors from SDR capabilities
            // and downgrade location_characterization so AA cannot cross-validate
            if cfg.video_in_motion {
                if let Some(svc) = msg
                    .services
                    .iter_mut()
                    .find(|svc| !svc.sensor_source_service.sensors.is_empty())
                {
                    // Remove sensor types that reveal vehicle motion.
                    // Keep DRIVING_STATUS, GEAR, PARKING_BRAKE, LOCATION (we spoof those)
                    // but remove the ones that are harder to spoof consistently per-HU.
                    let sensors_to_strip = [
                        SENSOR_ACCELEROMETER_DATA,
                        SENSOR_GYROSCOPE_DATA,
                        SENSOR_DEAD_RECKONING_DATA,
                        SENSOR_SPEED,
                    ];
                    svc.sensor_source_service
                        .as_mut()
                        .unwrap()
                        .sensors
                        .retain(|s| !sensors_to_strip.contains(&s.sensor_type()));

                    // Reset location_characterization to RAW_GPS_ONLY (256).
                    // This tells AA the HU does NOT fuse wheel speed, gyroscope,
                    // accelerometer, or dead reckoning into position fixes, so AA
                    // will not expect those signals for cross-validation.
                    svc.sensor_source_service
                        .as_mut()
                        .unwrap()
                        .set_location_characterization(256); // RAW_GPS_ONLY

                    info!(
                        "{} <yellow>{:?}</> video_in_motion: stripped motion sensors from SDR, location_characterization=RAW_GPS_ONLY",
                        get_name(proxy_type),
                        control.unwrap(),
                    );
                }
            }

            // enabling developer mode
            if cfg.developer_mode {
                msg.set_make(DHU_MAKE.into());
                msg.set_model(DHU_MODEL.into());
                msg.set_head_unit_make(DHU_MAKE.into());
                msg.set_head_unit_model(DHU_MODEL.into());
                if let Some(info) = msg.headunit_info.as_mut() {
                    info.set_make(DHU_MAKE.into());
                    info.set_model(DHU_MODEL.into());
                    info.set_head_unit_make(DHU_MAKE.into());
                    info.set_head_unit_model(DHU_MODEL.into());
                }
                info!(
                    "{} <yellow>{:?}</>: enabling developer mode",
                    get_name(proxy_type),
                    control.unwrap(),
                );
            }

            if cfg.remove_bluetooth {
                msg.services.retain(|svc| svc.bluetooth_service.is_none());
            }

            if cfg.remove_wifi {
                msg.services
                    .retain(|svc| svc.wifi_projection_service.is_none());
            }

            // EV routing features
            if cfg.ev {
                if let Some(svc) = msg
                    .services
                    .iter_mut()
                    .find(|svc| !svc.sensor_source_service.sensors.is_empty())
                {
                    info!(
                        "{} <yellow>{:?}</>: adding <b><green>EV</> features...",
                        get_name(proxy_type),
                        control.unwrap(),
                    );

                    // add VEHICLE_ENERGY_MODEL_DATA sensor
                    let mut sensor = Sensor::new();
                    sensor.set_sensor_type(SENSOR_VEHICLE_ENERGY_MODEL_DATA);
                    svc.sensor_source_service
                        .as_mut()
                        .unwrap()
                        .sensors
                        .push(sensor);

                    // set FUEL_TYPE
                    svc.sensor_source_service
                        .as_mut()
                        .unwrap()
                        .supported_fuel_types = vec![FuelType::FUEL_TYPE_ELECTRIC.into()];

                    // supported connector types
                    let connectors: Vec<EnumOrUnknown<EvConnectorType>> =
                        match &cfg.ev_connector_types.0 {
                            Some(types) => types.iter().map(|&t| t.into()).collect(),
                            None => {
                                vec![EvConnectorType::EV_CONNECTOR_TYPE_MENNEKES.into()]
                            }
                        };
                    info!(
                        "{} <yellow>{:?}</>: EV connectors: {:?}",
                        get_name(proxy_type),
                        control.unwrap(),
                        connectors,
                    );
                    svc.sensor_source_service
                        .as_mut()
                        .unwrap()
                        .supported_ev_connector_types = connectors;
                }
            }

            let added_services = add_display_services(&mut msg, cfg);
            if added_services > 0 {
                let before_ids: HashSet<i32> = ctx.hu_service_ids.clone();
                let after_ids: HashSet<i32> = msg.services.iter().map(|s| s.id()).collect();
                for sid in after_ids.difference(&before_ids) {
                    ctx.injected_service_ids.insert(*sid);
                    if let Some(svc) = msg.services.iter().find(|s| s.id() == *sid) {
                        if !svc.media_sink_service.video_configs.is_empty() {
                            ctx.injected_media_display
                                .insert(*sid as u8, svc.media_sink_service.display_type());
                        }
                    }
                }
                info!(
                    "{} <yellow>{:?}</>: injected <b>{}</> display service(s)",
                    get_name(proxy_type),
                    control.unwrap(),
                    added_services,
                );
                info!(
                    "{} <blue>injected service ids:</> <b>{:?}</>",
                    get_name(proxy_type),
                    ctx.injected_service_ids
                );
            }

            // Populate media_channels (channel_id→sink) from the offset→sink map.
            // Do this after add_display_services so HU includes injected channels (e.g., cluster 0x08).
            // Both proxy contexts use this for injected suppression/deferred focus evaluation.
            debug!(
                "{} SDR handling: media_sinks.len()={} media_channels.len()={}",
                get_name(proxy_type),
                ctx.media_sinks.len(),
                ctx.media_channels.len()
            );
            ctx.media_service_labels.clear();
            for svc in msg.services.iter() {
                if let Some(label) = describe_media_service(svc) {
                    ctx.media_service_labels.insert(svc.id() as u8, label);
                }
            }

            if !ctx.media_sinks.is_empty() {
                for svc in msg.services.iter() {
                    let ch = svc.id() as u8;
                    if !svc.media_sink_service.video_configs.is_empty() {
                        let offset = svc.media_sink_service.display_type().value() as u8;
                        if let Some(sink) = ctx.media_sinks.get(&offset).cloned() {
                            sink.set_video_stream_info(
                                svc.media_sink_service.available_type(),
                                svc.media_sink_service.display_type(),
                            )
                            .await;
                            ctx.media_channels.insert(ch, sink);
                            debug!(
                                "{} media_channels.insert: ch={:#04x} offset={}",
                                get_name(proxy_type),
                                ch,
                                offset
                            );
                            info!(
                                "{} <blue>media tap:</> video channel <b>{:#04x}</> → port offset <b>{}</> ({:?}, {:?})",
                                get_name(proxy_type),
                                ch,
                                offset,
                                svc.media_sink_service.available_type(),
                                svc.media_sink_service.display_type()
                            );
                        }
                    } else if !svc.media_sink_service.audio_configs.is_empty()
                        || svc.media_sink_service.audio_type.is_some()
                    {
                        // audio offset = audio_type value + 2 (offsets 0-2 reserved for video)
                        let offset = svc.media_sink_service.audio_type().value() as u8 + 2;
                        if let Some(sink) = ctx.media_sinks.get(&offset).cloned() {
                            let audio_config = svc.media_sink_service.audio_configs.first().map(|acfg| {
                                AudioStreamConfig {
                                    sample_rate: acfg.sampling_rate(),
                                    channels: acfg.number_of_channels(),
                                    bits: acfg.number_of_bits(),
                                }
                            });
                            sink.set_audio_stream_info(
                                svc.media_sink_service.available_type(),
                                svc.media_sink_service.audio_type(),
                                audio_config,
                            )
                            .await;
                            ctx.media_channels.insert(ch, sink);
                            debug!(
                                "{} media_channels.insert: ch={:#04x} offset={}",
                                get_name(proxy_type),
                                ch,
                                offset
                            );
                            if let Some(acfg) = audio_config {
                                info!(
                                    "{} <blue>media tap:</> audio channel <b>{:#04x}</> → port offset <b>{}</> ({:?}, {:?}, {}Hz, {}ch, {}bit)",
                                    get_name(proxy_type),
                                    ch,
                                    offset,
                                    svc.media_sink_service.available_type(),
                                    svc.media_sink_service.audio_type(),
                                    acfg.sample_rate,
                                    acfg.channels,
                                    acfg.bits
                                );
                            } else {
                                info!(
                                    "{} <blue>media tap:</> audio channel <b>{:#04x}</> → port offset <b>{}</> ({:?}, {:?})",
                                    get_name(proxy_type),
                                    ch,
                                    offset,
                                    svc.media_sink_service.available_type(),
                                    svc.media_sink_service.audio_type()
                                );
                            }
                        }
                    }
                }
            }

            if log_enabled!(log::Level::Info) {
                info!(
                    "{} <blue>SDR output:</> [{}]",
                    get_name(proxy_type),
                    summarize_services(&msg.services)
                );
                info!(
                    "{} <blue>SDR output media detail:</> [{}]",
                    get_name(proxy_type),
                    summarize_media_services(&msg.services)
                );
            }

            debug!(
                "{} SDR after changes: {}",
                get_name(proxy_type),
                protobuf::text_format::print_to_string_pretty(&msg)
            );

            // rewrite payload to new message contents
            pkt.payload = msg.write_to_bytes()?;
            // inserting 2 bytes of message_id at the beginning
            pkt.payload.insert(0, (message_id >> 8) as u8);
            pkt.payload.insert(1, (message_id & 0xff) as u8);
        }
        MESSAGE_SERVICE_DISCOVERY_UPDATE => {
            bump_flow_counter(flow, &mut ctx.sdu_from_endpoint, &mut ctx.sdu_to_endpoint);
            if let Ok(msg) = ServiceDiscoveryUpdate::parse_from_bytes(data) {
                if let Some(service) = msg.service.as_ref() {
                    let sid = service.id();
                    let injected = ctx.injected_service_ids.contains(&sid);
                    let hu_advertised = ctx.hu_service_ids.contains(&sid);

                    info!(
                        "{} <blue>SDU:</> flow={:?}, service_id=<b>{}</>, role={}, injected={}, hu_advertised={}",
                        get_name(proxy_type),
                        flow,
                        sid,
                        service_role(service),
                        injected,
                        hu_advertised
                    );

                    if proxy_type == ProxyType::HeadUnit && flow == PacketFlow::FromEndpoint {
                        ctx.hu_service_ids.insert(sid);
                    }
                } else {
                    info!(
                        "{} <blue>SDU:</> flow={:?}, no service payload",
                        get_name(proxy_type),
                        flow
                    );
                }
            }
        }
        MESSAGE_CHANNEL_OPEN_REQUEST => {
            bump_flow_counter(
                flow,
                &mut ctx.open_req_from_endpoint,
                &mut ctx.open_req_to_endpoint,
            );
            if let Ok(msg) = ChannelOpenRequest::parse_from_bytes(data) {
                let sid = msg.service_id();
                let injected = ctx.injected_service_ids.contains(&sid);
                let hu_known = ctx.hu_service_ids.contains(&sid);
                let service_label = ctx
                    .media_service_labels
                    .get(&(sid as u8))
                    .cloned()
                    .unwrap_or_else(|| format!("sid={} role={}", sid, if hu_known { "known" } else { "unknown" }));

                info!(
                    "{} <blue>ChannelOpenRequest:</> flow={:?}, service_id=<b>{}</>, priority={}, injected={}, hu_known={}, service={}",
                    get_name(proxy_type),
                    flow,
                    sid,
                    msg.priority(),
                    injected,
                    hu_known,
                    service_label
                );

                if proxy_type == ProxyType::MobileDevice && ctx.md_sdr_forwarded {
                    push_trace_text(
                        &mut ctx.md_pending_channel_opens,
                        format!("flow={:?} sid={} {}", flow, sid, service_label),
                        STARTUP_OPEN_TRACE_WINDOW,
                    );
                }

                if injected {
                    ctx.injected_open_seen += 1;
                    if proxy_type == ProxyType::HeadUnit && flow == PacketFlow::ToEndpoint {
                        // Keep injected services hidden from HU: answer locally with success.
                        ctx.injected_channels.insert(sid as u8);

                        let mut response = ChannelOpenResponse::new();
                        response.set_status(MessageStatus::STATUS_SUCCESS);
                        let mut payload = response.write_to_bytes()?;
                        payload.insert(0, ((MESSAGE_CHANNEL_OPEN_RESPONSE as u16) >> 8) as u8);
                        payload.insert(1, ((MESSAGE_CHANNEL_OPEN_RESPONSE as u16) & 0xff) as u8);
                        pkt.payload = payload;

                        info!(
                            "{} <yellow>transparency:</> synthesized CHANNEL_OPEN_RESPONSE for injected service_id <b>{}</>; HU path suppressed",
                            get_name(proxy_type),
                            sid
                        );

                        // handled=true => send this reply packet back to MD side only.
                        return Ok(true);
                    }
                }
            }
        }
        MESSAGE_CHANNEL_OPEN_RESPONSE => {
            bump_flow_counter(
                flow,
                &mut ctx.open_rsp_from_endpoint,
                &mut ctx.open_rsp_to_endpoint,
            );
            if let Ok(msg) = ChannelOpenResponse::parse_from_bytes(data) {
                let pending = if proxy_type == ProxyType::MobileDevice && ctx.md_sdr_forwarded {
                    ctx.md_pending_channel_opens.pop_front()
                } else {
                    None
                };
                info!(
                    "{} <blue>ChannelOpenResponse:</> flow={:?}, status={:?}, pending_match={}",
                    get_name(proxy_type),
                    flow,
                    msg.status(),
                    pending.unwrap_or_else(|| "n/a".to_string())
                );
            }
        }
        MESSAGE_AUDIO_FOCUS_REQUEST => {
            if let Ok(msg) = AudioFocusRequestNotification::parse_from_bytes(data) {
                if cfg.trace_channel_flow {
                    info!(
                        "{} <cyan>flow trace:</> AUDIO_FOCUS_REQUEST flow={:?} request={:?}",
                        get_name(proxy_type),
                        flow,
                        msg.request()
                    );
                } else {
                    debug!(
                        "{} <blue>AUDIO_FOCUS_REQUEST:</> flow={:?}, request={:?}",
                        get_name(proxy_type),
                        flow,
                        msg.request()
                    );
                }

                if proxy_type == ProxyType::MobileDevice
                    && flow == PacketFlow::FromEndpoint
                    && ctx.md_sdr_forwarded
                {
                    ctx.md_last_audio_focus_request = Some(msg.request());
                    match msg.request() {
                        protos::AudioFocusRequestType::AUDIO_FOCUS_RELEASE => {
                            ctx.md_audio_focus_release_seen = true
                        }
                        protos::AudioFocusRequestType::AUDIO_FOCUS_GAIN
                        | protos::AudioFocusRequestType::AUDIO_FOCUS_GAIN_TRANSIENT
                        | protos::AudioFocusRequestType::AUDIO_FOCUS_GAIN_TRANSIENT_MAY_DUCK => {
                            ctx.md_audio_focus_gain_seen = true
                        }
                    }
                }
                log_divergence_counters(proxy_type, ctx, "AUDIO_FOCUS_REQUEST");
            }
        }
        MESSAGE_AUDIO_FOCUS_NOTIFICATION => {
            if let Ok(msg) = AudioFocusNotification::parse_from_bytes(data) {
                if proxy_type == ProxyType::MobileDevice
                    && flow == PacketFlow::ToEndpoint
                    && ctx.md_sdr_forwarded
                {
                    ctx.hu_last_audio_focus_state = Some(msg.focus_state());
                    ctx.hu_last_audio_focus_unsolicited = Some(msg.unsolicited());
                    match msg.focus_state() {
                        protos::AudioFocusStateType::AUDIO_FOCUS_STATE_GAIN
                        | protos::AudioFocusStateType::AUDIO_FOCUS_STATE_GAIN_TRANSIENT
                        | protos::AudioFocusStateType::AUDIO_FOCUS_STATE_GAIN_MEDIA_ONLY
                        | protos::AudioFocusStateType::AUDIO_FOCUS_STATE_GAIN_TRANSIENT_GUIDANCE_ONLY => {
                            ctx.hu_audio_focus_gain_seen = true
                        }
                        _ => {}
                    }
                }
            }
        }
        MESSAGE_PING_REQUEST => {
            if let Ok(msg) = PingRequest::parse_from_bytes(data) {
                if cfg.trace_channel_flow {
                    info!(
                        "{} <cyan>flow trace:</> PING_REQUEST flow={:?} timestamp={} bug_report={}",
                        get_name(proxy_type),
                        flow,
                        msg.timestamp(),
                        msg.bug_report()
                    );
                } else {
                    debug!(
                        "{} <blue>PING_REQUEST:</> flow={:?}, timestamp={}, bug_report={}",
                        get_name(proxy_type),
                        flow,
                        msg.timestamp(),
                        msg.bug_report()
                    );
                }
                if proxy_type == ProxyType::MobileDevice
                    && flow == PacketFlow::ToEndpoint
                    && ctx.md_sdr_forwarded
                {
                    ctx.hu_ping_requests_forwarded_to_md =
                        ctx.hu_ping_requests_forwarded_to_md.saturating_add(1);
                }
            }
        }
        MESSAGE_PING_RESPONSE => {
            if let Ok(msg) = PingResponse::parse_from_bytes(data) {
                if cfg.trace_channel_flow {
                    info!(
                        "{} <cyan>flow trace:</> PING_RESPONSE flow={:?} timestamp={}",
                        get_name(proxy_type),
                        flow,
                        msg.timestamp()
                    );
                } else {
                    debug!(
                        "{} <blue>PING_RESPONSE:</> flow={:?}, timestamp={}",
                        get_name(proxy_type),
                        flow,
                        msg.timestamp()
                    );
                }
                if proxy_type == ProxyType::MobileDevice
                    && flow == PacketFlow::FromEndpoint
                    && ctx.md_sdr_forwarded
                {
                    ctx.md_ping_responses_seen =
                        ctx.md_ping_responses_seen.saturating_add(1);
                }
            }
        }
        _ => return Ok(false),
    };

    Ok(false)
}

/// encapsulates SSL data into Packet
async fn ssl_encapsulate(mut mem_buf: SslMemBuf) -> Result<Packet> {
    // read SSL-generated data
    let mut res: Vec<u8> = Vec::new();
    mem_buf.read_to(&mut res)?;

    // create MESSAGE_ENCAPSULATED_SSL Packet
    let message_type = ControlMessageType::MESSAGE_ENCAPSULATED_SSL as u16;
    res.insert(0, (message_type >> 8) as u8);
    res.insert(1, (message_type & 0xff) as u8);
    Ok(Packet {
        channel: 0x00,
        flags: FRAME_TYPE_FIRST | FRAME_TYPE_LAST,
        final_length: None,
        payload: res,
    })
}

/// creates Ssl for HeadUnit (SSL server) and MobileDevice (SSL client)
async fn ssl_builder(proxy_type: ProxyType) -> Result<Ssl> {
    let mut ctx_builder = SslContextBuilder::new(SslMethod::tls())?;

    // for HU/headunit we need to act as a MD/mobiledevice, so load "md" key and cert
    // and vice versa
    let prefix = match proxy_type {
        ProxyType::HeadUnit => "md",
        ProxyType::MobileDevice => "hu",
    };
    ctx_builder.set_certificate_file(format!("{KEYS_PATH}/{prefix}_cert.pem"), SslFiletype::PEM)?;
    ctx_builder.set_private_key_file(format!("{KEYS_PATH}/{prefix}_key.pem"), SslFiletype::PEM)?;
    ctx_builder.check_private_key()?;
    // trusted root certificates:
    ctx_builder.set_ca_file(format!("{KEYS_PATH}/galroot_cert.pem"))?;

    ctx_builder.set_min_proto_version(Some(openssl::ssl::SslVersion::TLS1_2))?;
    ctx_builder.set_options(openssl::ssl::SslOptions::NO_TLSV1_3);

    let openssl_ctx = ctx_builder.build();
    let mut ssl = Ssl::new(&openssl_ctx)?;
    if proxy_type == ProxyType::HeadUnit {
        ssl.set_accept_state(); // SSL server
    } else if proxy_type == ProxyType::MobileDevice {
        ssl.set_connect_state(); // SSL client
    }

    Ok(ssl)
}

/// reads all available data to VecDeque
async fn read_input_data<A: Endpoint<A>>(
    rbuf: &mut VecDeque<u8>,
    obj: &mut IoDevice<A>,
    incremental_read: bool,
) -> Result<usize> {
    let mut newdata = vec![0u8; BUFFER_LEN];
    let mut n;
    let mut len;

    match obj {
        IoDevice::UsbReader(device, _) => {
            let mut dev = device.borrow_mut();
            let retval = dev.read(&mut newdata);
            len = retval
                .await
                .context("read_input_data: UsbReader read error")?;
        }
        IoDevice::EndpointIo(device) => {
            if incremental_read {
                // read header
                newdata = vec![0u8; HEADER_LENGTH];
                let retval = device.read(newdata);
                (n, newdata) = timeout(Duration::from_millis(15000), retval)
                    .await
                    .context("read_input_data/header: EndpointIo timeout")?;
                len = n.context("read_input_data/header: EndpointIo read error")?;

                // fill the output/read buffer with the obtained header data
                rbuf.write(&newdata.clone().slice(..len))?;

                // compute payload size
                let mut payload_size = (newdata[3] as u16 + ((newdata[2] as u16) << 8)) as usize;
                if (newdata[1] & FRAME_TYPE_MASK) == FRAME_TYPE_FIRST {
                    // header is 8 bytes; need to read 4 more bytes
                    payload_size += 4;
                }
                // prepare buffer for the payload and continue normally
                newdata = vec![0u8; payload_size];
            }
            let retval = device.read(newdata);
            (n, newdata) = timeout(Duration::from_millis(15000), retval)
                .await
                .context("read_input_data: EndpointIo timeout")?;
            len = n.context("read_input_data: EndpointIo read error")?;
        }
        IoDevice::TcpStreamIo(device) => {
            let retval = device.read(newdata);
            (n, newdata) = timeout(Duration::from_millis(15000), retval)
                .await
                .context("read_input_data: TcpStreamIo timeout")?;
            len = n.context("read_input_data: TcpStreamIo read error")?;
            if len == 0 {
                // TCP EOF means the peer closed the connection; propagate as disconnect.
                return Err("read_input_data: TcpStreamIo EOF".into());
            }
        }
        _ => todo!(),
    }
    if len > 0 {
        rbuf.write(&newdata.slice(..len))?;
    }
    Ok(len)
}

/// runtime musl detection
fn is_musl() -> bool {
    std::path::Path::new("/lib/ld-musl-riscv64.so.1").exists()
}

/// main reader thread for a device
pub async fn endpoint_reader<A: Endpoint<A>>(
    mut device: IoDevice<A>,
    tx: Sender<Packet>,
    hu: bool,
) -> Result<()> {
    let mut rbuf: VecDeque<u8> = VecDeque::new();
    let incremental_read = if !hu && is_musl() { true } else { false };
    loop {
        read_input_data(&mut rbuf, &mut device, incremental_read).await?;
        // check if we have complete packet available
        loop {
            // Accept packets as soon as we have the complete fixed header.
            // Using >= is required for valid zero-payload frames (frame_size == HEADER_LENGTH).
            if rbuf.len() >= HEADER_LENGTH {
                let channel = rbuf[0];
                let flags = rbuf[1];

                // FIRST frames carry an extended 8-byte header. If only 4 bytes
                // are buffered, wait for the remaining header bytes before parsing.
                if (flags & FRAME_TYPE_MASK) == FRAME_TYPE_FIRST && rbuf.len() < 8 {
                    break;
                }

                let mut header_size = HEADER_LENGTH;
                let mut final_length = None;
                let payload_size = (rbuf[3] as u16 + ((rbuf[2] as u16) << 8)) as usize;
                if rbuf.len() >= 8 && (flags & FRAME_TYPE_MASK) == FRAME_TYPE_FIRST {
                    header_size += 4;
                    final_length = Some(
                        ((rbuf[4] as u32) << 24)
                            + ((rbuf[5] as u32) << 16)
                            + ((rbuf[6] as u32) << 8)
                            + (rbuf[7] as u32),
                    );
                }
                let frame_size = header_size + payload_size;
                if rbuf.len() >= frame_size {
                    let mut frame = vec![0u8; frame_size];
                    rbuf.read_exact(&mut frame)?;
                    // we now have all header data analyzed/read, so remove
                    // the header from frame to have payload only left
                    frame.drain(..header_size);
                    let pkt = Packet {
                        channel,
                        flags,
                        final_length,
                        payload: frame,
                    };
                    // send packet to main thread for further process
                    tx.send(pkt).await?;
                    // check if we have another packet
                    continue;
                }
            }
            // no more complete packets available
            break;
        }
    }
}

/// checking if there was a true fatal SSL error
/// Note that the error may not be fatal. For example if the underlying
/// stream is an asynchronous one then `HandshakeError::WouldBlock` may
/// just mean to wait for more I/O to happen later.
fn ssl_check_failure<T>(res: std::result::Result<T, openssl::ssl::Error>) -> Result<()> {
    if let Err(err) = res {
        match err.code() {
            ErrorCode::WANT_READ | ErrorCode::WANT_WRITE | ErrorCode::SYSCALL => Ok(()),
            _ => return Err(Box::new(err)),
        }
    } else {
        Ok(())
    }
}

/// main thread doing all packet processing of an endpoint/device
pub async fn proxy<A: Endpoint<A> + 'static>(
    proxy_type: ProxyType,
    mut device: IoDevice<A>,
    bytes_written: Arc<AtomicUsize>,
    tx: Sender<Packet>,
    mut rx: Receiver<Packet>,
    mut rxr: Receiver<Packet>,
    mut config: SharedConfig,
    sensor_channel: Arc<tokio::sync::Mutex<Option<u8>>>,
    input_channel: Arc<tokio::sync::Mutex<Option<u8>>>,
    ev_tx: Sender<EvTaskCommand>,
    media_sinks: HashMap<u8, MediaSink>,
) -> Result<()> {
    let cfg = config.read().await.clone();
    let passthrough = !cfg.mitm;
    let hex_requested = cfg.hexdump_level;

    // in full_frames/passthrough mode we only directly pass packets from one endpoint to the other
    if passthrough {
        loop {
            tokio::select! {
            // handling data from opposite device's thread, which needs to be transmitted
            Some(pkt) = rx.recv() => {
                debug!("{} rx.recv", get_name(proxy_type));
                let _ = pkt_debug(proxy_type, HexdumpLevel::RawOutput, hex_requested, &pkt).await;

                transmit_packet_runtime(
                    proxy_type,
                    "passthrough-rx-to-endpoint",
                    &pkt,
                    None,
                    &mut device,
                )
                    .await?;

                // Increment byte counters for statistics
                // fixme: compute final_len for precise stats
                bytes_written.fetch_add(HEADER_LENGTH + pkt.payload.len(), Ordering::Relaxed);
            }

            // handling input data from the reader thread
            Some(pkt) = rxr.recv() => {
                debug!("{} rxr.recv", get_name(proxy_type));
                let _ = pkt_debug(proxy_type, HexdumpLevel::RawOutput, hex_requested, &pkt).await;

                tx.send(pkt).await?;
            }
            }
        }
    }

    let ssl = ssl_builder(proxy_type).await?;

    let mut mem_buf = SslMemBuf {
        client_stream: Arc::new(Mutex::new(VecDeque::new())),
        server_stream: Arc::new(Mutex::new(VecDeque::new())),
    };
    let mut server = openssl::ssl::SslStream::new(ssl, mem_buf.clone())?;

    // initial phase: passing version and doing SSL handshake for both HU and MD
    // this guard intentionally fails fast with explicit stage/queue context
    let startup_res: Result<()> = async {
        if proxy_type == ProxyType::HeadUnit {
            // waiting for initial version frame (HU is starting transmission)
            let pkt = recv_packet_with_timeout(
                proxy_type,
                "rxr",
                "initial version request from HU endpoint",
                &mut rxr,
            )
            .await?;
            let _ = pkt_debug(
                proxy_type,
                HexdumpLevel::DecryptedInput, // the packet is not encrypted
                hex_requested,
                &pkt,
            )
            .await;
            // sending to the MD
            tx.send(pkt).await?;
            // waiting for MD reply
            let pkt = recv_packet_with_timeout(
                proxy_type,
                "rx",
                "initial version response from MD side",
                &mut rx,
            )
            .await?;
            // sending reply back to the HU
            let _ = pkt_debug(proxy_type, HexdumpLevel::RawOutput, hex_requested, &pkt).await;
                pkt.transmit(&mut device)
                    .await
                    .with_context(|| {
                        format!(
                            "{} transmit failed (initial version response to HU endpoint)",
                            get_name(proxy_type)
                        )
                    })?;

            // doing SSL handshake
            const STEPS: u8 = 2;
            for i in 1..=STEPS {
                let pkt = recv_packet_with_timeout(
                    proxy_type,
                    "rxr",
                    &format!("SSL handshake input step {}/{}", i, STEPS),
                    &mut rxr,
                )
                .await?;
                let _ = pkt_debug(proxy_type, HexdumpLevel::RawInput, hex_requested, &pkt).await;
                pkt.ssl_decapsulate_write(&mut mem_buf).await?;
                ssl_check_failure(server.accept())?;
                info!(
                    "{} 🔒 stage #{} of {}: SSL handshake: {}",
                    get_name(proxy_type),
                    i,
                    STEPS,
                    server.ssl().state_string_long(),
                );
                if server.ssl().is_init_finished() {
                    info!(
                        "{} 🔒 SSL init complete, negotiated cipher: <b><blue>{}</>",
                        get_name(proxy_type),
                        server.ssl().current_cipher().unwrap().name(),
                    );
                }
                let pkt = ssl_encapsulate(mem_buf.clone()).await?;
                let _ = pkt_debug(proxy_type, HexdumpLevel::RawOutput, hex_requested, &pkt).await;
                    pkt.transmit(&mut device)
                        .await
                        .with_context(|| {
                            format!(
                                "{} transmit failed (SSL handshake output step {}/{})",
                                get_name(proxy_type),
                                i,
                                STEPS
                            )
                        })?;
            }
        } else if proxy_type == ProxyType::MobileDevice {
            // expecting version request from the HU here...
            let pkt = recv_packet_with_timeout(
                proxy_type,
                "rx",
                "initial version request from HU side",
                &mut rx,
            )
            .await?;
            // sending to the MD
            let _ = pkt_debug(proxy_type, HexdumpLevel::RawOutput, hex_requested, &pkt).await;
                pkt.transmit(&mut device)
                    .await
                    .with_context(|| {
                        format!(
                            "{} transmit failed (initial version request to MD endpoint)",
                            get_name(proxy_type)
                        )
                    })?;
            // waiting for MD reply, skipping stale encrypted packets (0x1703) left over from a
            // prior session on the phone side — these are not meaningful to the HU which never
            // participated in that old session, so we absorb them here in the MITM layer.
            let pkt = loop {
                let pkt = recv_packet_with_timeout(
                    proxy_type,
                    "rxr",
                    "initial version response from MD endpoint",
                    &mut rxr,
                )
                .await?;
                if packet_message_id(&pkt) == Some(MESSAGE_VERSION_RESPONSE as u16) {
                    break pkt;
                }
                warn!(
                    "{} ignoring stale pre-handshake packet from phone (msg_id=0x{:04X}), waiting for VERSION_RESPONSE",
                    get_name(proxy_type),
                    packet_message_id(&pkt).unwrap_or(0),
                );
            };
            let _ = pkt_debug(
                proxy_type,
                HexdumpLevel::DecryptedInput, // the packet is not encrypted
                hex_requested,
                &pkt,
            )
            .await;
            // sending reply back to the HU
            tx.send(pkt).await?;

            // doing SSL handshake
            const STEPS: u8 = 3;
            for i in 1..=STEPS {
                ssl_check_failure(server.do_handshake())?;
                info!(
                    "{} 🔒 stage #{} of {}: SSL handshake: {}",
                    get_name(proxy_type),
                    i,
                    STEPS,
                    server.ssl().state_string_long(),
                );
                if server.ssl().is_init_finished() {
                    info!(
                        "{} 🔒 SSL init complete, negotiated cipher: <b><blue>{}</>",
                        get_name(proxy_type),
                        server.ssl().current_cipher().unwrap().name(),
                    );
                }
                if i == 3 {
                    // this was the last handshake step, need to break here
                    break;
                };
                let pkt = ssl_encapsulate(mem_buf.clone()).await?;
                let _ = pkt_debug(proxy_type, HexdumpLevel::RawOutput, hex_requested, &pkt).await;
                    pkt.transmit(&mut device)
                        .await
                        .with_context(|| {
                            format!(
                                "{} transmit failed (SSL handshake output step {}/{})",
                                get_name(proxy_type),
                                i,
                                STEPS
                            )
                        })?;

                let pkt = recv_packet_with_timeout(
                    proxy_type,
                    "rxr",
                    &format!("SSL handshake input step {}/{}", i, STEPS),
                    &mut rxr,
                )
                .await?;
                let _ = pkt_debug(proxy_type, HexdumpLevel::RawInput, hex_requested, &pkt).await;
                pkt.ssl_decapsulate_write(&mut mem_buf).await?;
            }
        }
        Ok(())
    }
    .await;

    if let Err(e) = startup_res {
        error!(
            "{} startup guard failed before service discovery: {}",
            get_name(proxy_type),
            e
        );
        return Err(e);
    }

    // main data processing/transfer loop
    let mut ctx = ModifyContext {
        sensor_channel: None,
        nav_channel: None,
        audio_channels: vec![],
        ev_tx,
        media_sinks,
        media_channels: HashMap::new(),
        media_service_labels: HashMap::new(),
        media_fragments: HashMap::new(),
        hu_service_ids: HashSet::new(),
        injected_service_ids: HashSet::new(),
        injected_open_seen: 0,
        injected_open_forwarded_to_hu: 0,
        injected_channels: HashSet::new(),
        injected_media_display: HashMap::new(),
        injected_media_state: HashMap::new(),
        injected_media_connect_gen: HashMap::new(),
        injected_media_had_tap_client: HashMap::new(),
        injected_media_counters: InjectedMediaCounters::default(),
        md_sdr_forwarded: false,
        md_control_after_sdr: 0,
        md_last_control_after_sdr: None,
        md_recent_control_from_md: VecDeque::new(),
        md_recent_control_to_md: VecDeque::new(),
        md_recent_media_from_md: VecDeque::new(),
        md_recent_media_to_md: VecDeque::new(),
        md_pending_channel_opens: VecDeque::new(),
        md_audio_focus_release_seen: false,
        md_audio_focus_gain_seen: false,
        md_last_audio_focus_request: None,
        hu_audio_focus_gain_seen: false,
        hu_last_audio_focus_state: None,
        hu_last_audio_focus_unsolicited: None,
        md_media_startup_seen: false,
        hu_ping_requests_forwarded_to_md: 0,
        md_ping_responses_seen: 0,
        sdu_from_endpoint: 0,
        sdu_to_endpoint: 0,
        open_req_from_endpoint: 0,
        open_req_to_endpoint: 0,
        open_rsp_from_endpoint: 0,
        open_rsp_to_endpoint: 0,
    };
    let mut control_stats = ControlRuntimeStats::default();
    let mut focus_poll = tokio::time::interval(Duration::from_millis(100));
    focus_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    focus_poll.tick().await;
    loop {
        tokio::select! {
        // handling data from opposite device's thread, which needs to be transmitted
        Some(mut pkt) = rx.recv() => {
            let is_control = pkt.channel == 0;
            if is_control {
                control_stats.rx_to_endpoint.note_ingress();
            }

            debug_injected_probe(proxy_type, "rx", &pkt, &ctx);

            if proxy_type == ProxyType::HeadUnit {
                maybe_emit_pending_injected_focus(proxy_type, &mut ctx, &cfg, &tx)?;
            }

            // Keep injected-only channels invisible to HU.
            if proxy_type == ProxyType::HeadUnit
                && pkt.channel != 0
                && ctx.injected_channels.contains(&pkt.channel)
            {
                let had_fragment_state = ctx.media_fragments.contains_key(&pkt.channel);
                // Injected media packets were already tapped on the MobileDevice ingress path.
                // Reassemble here only so ACK synthesis can track whole media messages.
                let reassembled_frame = reassemble_media_packet(&mut ctx.media_fragments, &pkt);

                if emulate_injected_media_packet(
                    proxy_type,
                    &mut pkt,
                    &mut ctx,
                    reassembled_frame.as_deref(),
                    had_fragment_state,
                    &cfg,
                )? {
                    debug!(
                        "{} synthesized injected media reply on channel <b>{:#04x}</>",
                        get_name(proxy_type),
                        pkt.channel
                    );
                    let queue_started = Instant::now();
                    tx.send(pkt).await?;

                    maybe_emit_pending_injected_focus(proxy_type, &mut ctx, &cfg, &tx)?;

                    if is_control {
                        control_stats.rx_to_endpoint.note_egress();
                        control_stats
                            .rx_to_endpoint
                            .note_queue_wait(queue_started.elapsed().as_millis() as u64);
                        control_stats.maybe_log(proxy_type);
                    }
                    continue;
                }

                let msg_id = packet_message_id(&pkt)
                    .map(|id| format!("0x{:04X}", id))
                    .unwrap_or_else(|| "none".to_string());

                debug!(
                    "{} dropping injected channel packet towards HU on channel <b>{:#04x}</> message_id={}",
                    get_name(proxy_type),
                    pkt.channel,
                    msg_id
                );
                ctx.injected_media_counters.passthrough_drops =
                    ctx.injected_media_counters.passthrough_drops.saturating_add(1);
                let display_type = ctx
                    .injected_media_display
                    .get(&pkt.channel)
                    .copied()
                    .unwrap_or(DisplayType::DISPLAY_TYPE_CLUSTER);
                maybe_log_injected_media_counters(
                    proxy_type,
                    &ctx.injected_media_counters,
                    pkt.channel,
                    display_type,
                );
                continue;
            }

            let handled = pkt_modify_hook(
                proxy_type,
                PacketFlow::ToEndpoint,
                &mut pkt,
                &mut ctx,
                false,
                sensor_channel.clone(),
                input_channel.clone(),
                &cfg,
                &mut config,
            )
            .await?;
            let _ = pkt_debug(
                proxy_type,
                HexdumpLevel::DecryptedOutput,
                hex_requested,
                &pkt,
            )
            .await;

            if handled {
                debug!(
                    "{} pkt_modify_hook: message has been handled, sending reply packet only...",
                    get_name(proxy_type)
                );
                let queue_started = Instant::now();
                tx.send(pkt).await?;
                if is_control {
                    control_stats.rx_to_endpoint.note_egress();
                    control_stats
                        .rx_to_endpoint
                        .note_queue_wait(queue_started.elapsed().as_millis() as u64);
                    control_stats.maybe_log(proxy_type);
                }
            } else {
                let logical_msg_id = packet_message_id(&pkt);
                pkt.encrypt_payload(&mut mem_buf, &mut server).await?;
                let _ = pkt_debug(proxy_type, HexdumpLevel::RawOutput, hex_requested, &pkt).await;
                let tx_started = Instant::now();
                let first_tx = transmit_packet_runtime(
                    proxy_type,
                    "main-rx-to-endpoint",
                    &pkt,
                    logical_msg_id,
                    &mut device,
                )
                .await;

                if let Err(first_err) = first_tx {
                    let is_md_ping_request = proxy_type == ProxyType::MobileDevice
                        && is_control
                        && logical_msg_id == Some(MESSAGE_PING_REQUEST as u16);

                    if is_md_ping_request {
                        warn!(
                            "{} <yellow>runtime transmit retry:</> first attempt failed for ping request msg_id=0x{:04X}; retrying once",
                            get_name(proxy_type),
                            MESSAGE_PING_REQUEST as u16
                        );

                        match transmit_packet_runtime(
                            proxy_type,
                            "main-rx-to-endpoint-retry",
                            &pkt,
                            logical_msg_id,
                            &mut device,
                        )
                        .await
                        {
                            Ok(()) => {
                                info!(
                                    "{} <blue>runtime transmit retry:</> recovered after one retry for ping request",
                                    get_name(proxy_type)
                                );
                            }
                            Err(retry_err) => {
                                error!(
                                    "{} <red>runtime transmit retry:</> second attempt failed for ping request: {}",
                                    get_name(proxy_type),
                                    retry_err
                                );
                                if proxy_type == ProxyType::MobileDevice && is_control {
                                    error!(
                                        "{} <red>timeout snapshot:</> {}",
                                        get_name(proxy_type),
                                        format_md_startup_snapshot(&ctx)
                                    );
                                }
                                return Err(retry_err);
                            }
                        }
                    } else {
                        if proxy_type == ProxyType::MobileDevice && is_control {
                            error!(
                                "{} <red>timeout snapshot:</> {}",
                                get_name(proxy_type),
                                format_md_startup_snapshot(&ctx)
                            );
                        }
                        return Err(first_err);
                    }
                }
                if is_control {
                    control_stats.rx_to_endpoint.note_egress();
                    control_stats
                        .rx_to_endpoint
                        .note_tx(tx_started.elapsed().as_millis() as u64);
                    control_stats.maybe_log(proxy_type);
                }

                // Increment byte counters for statistics
                // fixme: compute final_len for precise stats
                bytes_written.fetch_add(HEADER_LENGTH + pkt.payload.len(), Ordering::Relaxed);
            }
        }

        // handling input data from the reader thread
        Some(mut pkt) = rxr.recv() => {
            let is_control = pkt.channel == 0;
            if is_control {
                control_stats.endpoint_to_tx.note_ingress();
            }

            let _ = pkt_debug(proxy_type, HexdumpLevel::RawInput, hex_requested, &pkt).await;
            match pkt.decrypt_payload(&mut mem_buf, &mut server).await {
                Ok(_) => {
                    debug_injected_probe(proxy_type, "rxr", &pkt, &ctx);
                    let _ = pkt_modify_hook(
                        proxy_type,
                        PacketFlow::FromEndpoint,
                        &mut pkt,
                        &mut ctx,
                        proxy_type == ProxyType::MobileDevice,
                        sensor_channel.clone(),
                        input_channel.clone(),
                        &cfg,
                        &mut config,
                    )
                    .await?;
                    let _ = pkt_debug(
                        proxy_type,
                        HexdumpLevel::DecryptedInput,
                        hex_requested,
                        &pkt,
                    )
                    .await;
                    let queue_started = Instant::now();
                    tx.send(pkt).await?;
                    if is_control {
                        control_stats.endpoint_to_tx.note_egress();
                        control_stats
                            .endpoint_to_tx
                            .note_queue_wait(queue_started.elapsed().as_millis() as u64);
                        control_stats.maybe_log(proxy_type);
                    }
                }
                Err(e) => error!("decrypt_payload: {:?}", e),
            }
        }

        _ = focus_poll.tick(), if proxy_type == ProxyType::HeadUnit => {
            maybe_emit_pending_injected_focus(proxy_type, &mut ctx, &cfg, &tx)?;
        }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn make_video_service(id: i32, display_type: DisplayType, display_id: u32) -> Service {
        let mut svc = Service::new();
        svc.set_id(id);

        let mut media = MediaSinkService::new();
        media.set_available_type(MediaCodecType::MEDIA_CODEC_VIDEO_H264_BP);
        media.set_display_type(display_type);
        media.set_display_id(display_id);

        let mut video_cfg = VideoConfiguration::new();
        video_cfg.set_codec_resolution(VideoCodecResolutionType::VIDEO_1280x720);
        video_cfg.set_frame_rate(VideoFrameRateType::VIDEO_FPS_30);
        media.video_configs.push(video_cfg);

        svc.media_sink_service = Some(media).into();
        svc
    }

    fn make_input_service(id: i32, display_id: u32) -> Service {
        let mut svc = Service::new();
        svc.set_id(id);

        let mut input = InputSourceService::new();
        input.set_display_id(display_id);
        svc.input_source_service = Some(input).into();
        svc
    }

    fn test_ctx() -> ModifyContext {
        let (ev_tx, _) = mpsc::channel(1);
        ModifyContext {
            sensor_channel: None,
            nav_channel: None,
            audio_channels: vec![],
            ev_tx,
            media_sinks: HashMap::new(),
            media_channels: HashMap::new(),
            media_fragments: HashMap::new(),
            hu_service_ids: HashSet::new(),
            injected_service_ids: HashSet::new(),
            injected_open_seen: 0,
            injected_open_forwarded_to_hu: 0,
            injected_channels: HashSet::new(),
            injected_media_display: HashMap::new(),
            injected_media_state: HashMap::new(),
            injected_media_connect_gen: HashMap::new(),
            injected_media_had_tap_client: HashMap::new(),
            injected_media_counters: InjectedMediaCounters::default(),
            md_sdr_forwarded: false,
            md_control_after_sdr: 0,
            md_last_control_after_sdr: None,
            md_recent_control_from_md: VecDeque::new(),
            md_recent_control_to_md: VecDeque::new(),
            md_recent_media_from_md: VecDeque::new(),
            md_recent_media_to_md: VecDeque::new(),
            md_pending_channel_opens: VecDeque::new(),
            md_audio_focus_release_seen: false,
            md_audio_focus_gain_seen: false,
            md_last_audio_focus_request: None,
            hu_audio_focus_gain_seen: false,
            hu_last_audio_focus_state: None,
            hu_last_audio_focus_unsolicited: None,
            md_media_startup_seen: false,
            hu_ping_requests_forwarded_to_md: 0,
            md_ping_responses_seen: 0,
            media_service_labels: HashMap::new(),
            sdu_from_endpoint: 0,
            sdu_to_endpoint: 0,
            open_req_from_endpoint: 0,
            open_req_to_endpoint: 0,
            open_rsp_from_endpoint: 0,
            open_rsp_to_endpoint: 0,
        }
    }

    fn test_packet(channel: u8, flags: u8, final_length: Option<u32>, payload: &[u8]) -> Packet {
        Packet {
            channel,
            flags,
            final_length,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn media_tap_keeps_single_frame_packets_intact() {
        let mut ctx = test_ctx();
        let pkt = test_packet(
            0x21,
            FRAME_TYPE_FIRST | FRAME_TYPE_LAST,
            None,
            &[0x00, 0x01, 0xAA, 0xBB],
        );

        let assembled = reassemble_media_packet(&mut ctx.media_fragments, &pkt);

        assert_eq!(assembled, Some(vec![0x00, 0x01, 0xAA, 0xBB]));
        assert!(!ctx.media_fragments.contains_key(&0x21));
    }

    #[test]
    fn media_tap_reassembles_fragmented_packets() {
        let mut ctx = test_ctx();
        let first = test_packet(0x21, FRAME_TYPE_FIRST, Some(6), &[0x00, 0x01, 0xAA]);
        let middle = test_packet(0x21, 0, None, &[0xBB]);
        let last = test_packet(0x21, FRAME_TYPE_LAST, None, &[0xCC, 0xDD]);

        assert_eq!(reassemble_media_packet(&mut ctx.media_fragments, &first), None);
        assert_eq!(reassemble_media_packet(&mut ctx.media_fragments, &middle), None);
        assert_eq!(
            reassemble_media_packet(&mut ctx.media_fragments, &last),
            Some(vec![0x00, 0x01, 0xAA, 0xBB, 0xCC, 0xDD])
        );
        assert!(!ctx.media_fragments.contains_key(&0x21));
    }

    #[test]
    fn media_tap_drops_length_mismatches() {
        let mut ctx = test_ctx();
        let first = test_packet(0x21, FRAME_TYPE_FIRST, Some(7), &[0x00, 0x01, 0xAA]);
        let last = test_packet(0x21, FRAME_TYPE_LAST, None, &[0xBB, 0xCC]);

        assert_eq!(reassemble_media_packet(&mut ctx.media_fragments, &first), None);
        assert_eq!(reassemble_media_packet(&mut ctx.media_fragments, &last), None);
        assert!(!ctx.media_fragments.contains_key(&0x21));
    }

    #[test]
    fn add_display_services_adds_cluster_media_and_input_when_opted_in() {
        let mut cfg = AppConfig::default();
        cfg.mitm = true;
        cfg.inject_display_types = crate::config_types::InjectDisplayTypes(Some(vec![
            DisplayType::DISPLAY_TYPE_CLUSTER,
        ]));
        cfg.inject_add_input_sources = true;

        let mut msg = ServiceDiscoveryResponse::new();
        msg.services
            .push(make_video_service(1, DisplayType::DISPLAY_TYPE_MAIN, 0));

        let added = add_display_services(&mut msg, &cfg);

        assert_eq!(added, 2);
        assert!(has_video_display(&msg, DisplayType::DISPLAY_TYPE_CLUSTER));
        assert!(has_input_display(&msg, cfg.inject_cluster_display_id.into()));
    }

    #[test]
    fn auxiliary_input_service_keeps_touchscreen() {
        let mut cfg = AppConfig::default();
        cfg.mitm = true;
        cfg.inject_display_types = crate::config_types::InjectDisplayTypes(Some(vec![
            DisplayType::DISPLAY_TYPE_AUXILIARY,
        ]));
        cfg.inject_add_input_sources = true;

        let mut msg = ServiceDiscoveryResponse::new();
        msg.services
            .push(make_video_service(1, DisplayType::DISPLAY_TYPE_MAIN, 0));

        let added = add_display_services(&mut msg, &cfg);

        assert_eq!(added, 2);
        let aux_input = msg
            .services
            .iter()
            .find(|svc| svc.input_source_service.display_id() == u32::from(cfg.inject_aux_display_id))
            .unwrap();
        assert_eq!(aux_input.input_source_service.touchscreen.len(), 1);
        assert!(aux_input.input_source_service.touchscreen[0].is_secondary());
    }

    #[test]
    fn add_display_services_does_not_duplicate_existing_cluster() {
        let mut cfg = AppConfig::default();
        cfg.mitm = true;
        cfg.inject_display_types = crate::config_types::InjectDisplayTypes(Some(vec![
            DisplayType::DISPLAY_TYPE_CLUSTER,
        ]));
        cfg.inject_add_input_sources = true;

        let mut msg = ServiceDiscoveryResponse::new();
        msg.services
            .push(make_video_service(1, DisplayType::DISPLAY_TYPE_MAIN, 0));
        msg.services
            .push(make_video_service(2, DisplayType::DISPLAY_TYPE_CLUSTER, 1));
        msg.services.push(make_input_service(3, 1));

        let added = add_display_services(&mut msg, &cfg);

        assert_eq!(added, 0);
        let cluster_count = msg
            .services
            .iter()
            .filter(|svc| {
                !svc.media_sink_service.video_configs.is_empty()
                    && svc.media_sink_service.display_type()
                        == DisplayType::DISPLAY_TYPE_CLUSTER
            })
            .count();
        assert_eq!(cluster_count, 1);
    }

    #[test]
    fn add_display_services_adds_cluster_without_input_when_not_opted_in() {
        let mut cfg = AppConfig::default();
        cfg.mitm = true;
        cfg.inject_display_types = crate::config_types::InjectDisplayTypes(Some(vec![
            DisplayType::DISPLAY_TYPE_CLUSTER,
        ]));
        cfg.inject_add_input_sources = false;

        let mut msg = ServiceDiscoveryResponse::new();
        msg.services
            .push(make_video_service(1, DisplayType::DISPLAY_TYPE_MAIN, 0));

        let added = add_display_services(&mut msg, &cfg);

        assert_eq!(added, 1);
        assert!(has_video_display(&msg, DisplayType::DISPLAY_TYPE_CLUSTER));
        assert!(!has_input_display(&msg, cfg.inject_cluster_display_id.into()));
    }

    #[test]
    fn injected_media_setup_is_rewritten_to_config_ready() {
        let mut ctx = test_ctx();
        ctx.injected_channels.insert(0x2A);

        let mut setup = Setup::new();
        setup.set_type(MediaCodecType::MEDIA_CODEC_VIDEO_H264_BP);
        let mut payload = setup.write_to_bytes().unwrap();
        payload.insert(0, ((MEDIA_MESSAGE_SETUP as u16) >> 8) as u8);
        payload.insert(1, ((MEDIA_MESSAGE_SETUP as u16) & 0xff) as u8);
        let mut pkt = test_packet(0x2A, ENCRYPTED | FRAME_TYPE_FIRST | FRAME_TYPE_LAST, None, &payload);

        assert!(emulate_injected_media_packet(
            ProxyType::HeadUnit,
            &mut pkt,
            &mut ctx,
            None,
            false,
            &AppConfig::default(),
        )
        .unwrap());

        let msg_id = u16::from_be_bytes([pkt.payload[0], pkt.payload[1]]) as i32;
        assert_eq!(msg_id, MEDIA_MESSAGE_CONFIG.value());
        assert_eq!(pkt.final_length, None);
        assert_eq!(pkt.flags & FRAME_TYPE_MASK, FRAME_TYPE_FIRST | FRAME_TYPE_LAST);
        let cfg = protos::Config::parse_from_bytes(&pkt.payload[2..]).unwrap();
        assert_eq!(cfg.status(), protos::config::Status::STATUS_READY);
        assert_eq!(cfg.max_unacked(), 1);
    }

    #[test]
    fn injected_media_data_after_start_is_rewritten_to_ack() {
        let mut ctx = test_ctx();
        ctx.injected_channels.insert(0x2A);
        ctx.injected_media_display
            .insert(0x2A, DisplayType::DISPLAY_TYPE_CLUSTER);
        ctx.injected_media_state
            .insert(0x2A, InjectedMediaState::default());

        let mut start = Start::new();
        start.set_session_id(7);
        start.set_configuration_index(0);
        let mut start_payload = start.write_to_bytes().unwrap();
        start_payload.insert(0, ((MEDIA_MESSAGE_START as u16) >> 8) as u8);
        start_payload.insert(1, ((MEDIA_MESSAGE_START as u16) & 0xff) as u8);
        let mut start_pkt =
            test_packet(0x2A, ENCRYPTED | FRAME_TYPE_FIRST | FRAME_TYPE_LAST, None, &start_payload);

        assert!(!emulate_injected_media_packet(
            ProxyType::HeadUnit,
            &mut start_pkt,
            &mut ctx,
            None,
            false,
            &AppConfig::default(),
        )
        .unwrap());

        let data_payload = vec![
            ((MEDIA_MESSAGE_DATA as u16) >> 8) as u8,
            ((MEDIA_MESSAGE_DATA as u16) & 0xff) as u8,
            0x00,
            0x01,
        ];
        let mut data_pkt =
            test_packet(0x2A, ENCRYPTED | FRAME_TYPE_FIRST | FRAME_TYPE_LAST, None, &data_payload);

        assert!(emulate_injected_media_packet(
            ProxyType::HeadUnit,
            &mut data_pkt,
            &mut ctx,
            Some(&data_payload),
            false,
            &AppConfig::default(),
        )
        .unwrap());

        let msg_id = u16::from_be_bytes([data_pkt.payload[0], data_pkt.payload[1]]) as i32;
        assert_eq!(msg_id, MEDIA_MESSAGE_ACK.value());
        assert_eq!(data_pkt.final_length, None);
        assert_eq!(
            data_pkt.flags & FRAME_TYPE_MASK,
            FRAME_TYPE_FIRST | FRAME_TYPE_LAST
        );
        let ack = Ack::parse_from_bytes(&data_pkt.payload[2..]).unwrap();
        assert_eq!(ack.session_id(), 7);
        assert_eq!(ack.ack(), 1);
    }

    #[test]
    fn fragmented_injected_media_data_waits_for_reassembly_before_ack() {
        let mut ctx = test_ctx();
        ctx.injected_channels.insert(0x2A);
        ctx.injected_media_display
            .insert(0x2A, DisplayType::DISPLAY_TYPE_CLUSTER);

        let mut start = Start::new();
        start.set_session_id(11);
        start.set_configuration_index(0);
        let mut start_payload = start.write_to_bytes().unwrap();
        start_payload.insert(0, ((MEDIA_MESSAGE_START as u16) >> 8) as u8);
        start_payload.insert(1, ((MEDIA_MESSAGE_START as u16) & 0xff) as u8);
        let mut start_pkt =
            test_packet(0x2A, ENCRYPTED | FRAME_TYPE_FIRST | FRAME_TYPE_LAST, None, &start_payload);
        assert!(!emulate_injected_media_packet(
            ProxyType::HeadUnit,
            &mut start_pkt,
            &mut ctx,
            None,
            false,
            &AppConfig::default(),
        )
        .unwrap());

        let full_frame = vec![
            ((MEDIA_MESSAGE_DATA as u16) >> 8) as u8,
            ((MEDIA_MESSAGE_DATA as u16) & 0xff) as u8,
            0xAA,
            0xBB,
            0xCC,
            0xDD,
        ];
        let mut first_pkt = test_packet(
            0x2A,
            ENCRYPTED | FRAME_TYPE_FIRST,
            Some(full_frame.len() as u32),
            &full_frame[..4],
        );
        let first_frame = reassemble_media_packet(&mut ctx.media_fragments, &first_pkt);
        assert!(!emulate_injected_media_packet(
            ProxyType::HeadUnit,
            &mut first_pkt,
            &mut ctx,
            first_frame.as_deref(),
            false,
            &AppConfig::default(),
        )
        .unwrap());

        let mut last_pkt = test_packet(
            0x2A,
            ENCRYPTED | FRAME_TYPE_LAST,
            None,
            &full_frame[4..],
        );
        let last_frame = reassemble_media_packet(&mut ctx.media_fragments, &last_pkt);
        assert!(emulate_injected_media_packet(
            ProxyType::HeadUnit,
            &mut last_pkt,
            &mut ctx,
            last_frame.as_deref(),
            true,
            &AppConfig::default(),
        )
        .unwrap());

        let msg_id = u16::from_be_bytes([last_pkt.payload[0], last_pkt.payload[1]]) as i32;
        assert_eq!(msg_id, MEDIA_MESSAGE_ACK.value());
        assert_eq!(last_pkt.final_length, None);
        assert_eq!(last_pkt.flags & FRAME_TYPE_MASK, FRAME_TYPE_FIRST | FRAME_TYPE_LAST);
        let ack = Ack::parse_from_bytes(&last_pkt.payload[2..]).unwrap();
        assert_eq!(ack.session_id(), 11);
        assert_eq!(ack.ack(), 1);
    }

    #[test]
    fn deferred_injected_focus_does_not_block_when_queue_is_full() {
        let mut ctx = test_ctx();
        ctx.injected_media_state.insert(
            0x2A,
            InjectedMediaState {
                phase: InjectedMediaPhase::SetupSeen,
                last_flags: ENCRYPTED | FRAME_TYPE_FIRST | FRAME_TYPE_LAST,
                ..Default::default()
            },
        );

        let mut cfg = AppConfig::default();
        cfg.inject_force_focus_without_tap = true;

        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(test_packet(0, 0, None, &[])).unwrap();

        maybe_emit_pending_injected_focus(ProxyType::HeadUnit, &mut ctx, &cfg, &tx).unwrap();

        assert!(ctx
            .injected_media_state
            .get(&0x2A)
            .is_some_and(|state| state.phase == InjectedMediaPhase::SetupSeen));
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn injected_aux_media_setup_uses_aux_profile_unacked_window() {
        let mut ctx = test_ctx();
        ctx.injected_media_display
            .insert(0x2B, DisplayType::DISPLAY_TYPE_AUXILIARY);

        let mut setup = Setup::new();
        setup.set_type(MediaCodecType::MEDIA_CODEC_VIDEO_H264_BP);
        let mut payload = setup.write_to_bytes().unwrap();
        payload.insert(0, ((MEDIA_MESSAGE_SETUP as u16) >> 8) as u8);
        payload.insert(1, ((MEDIA_MESSAGE_SETUP as u16) & 0xff) as u8);
        let mut pkt =
            test_packet(0x2B, ENCRYPTED | FRAME_TYPE_FIRST | FRAME_TYPE_LAST, None, &payload);

        assert!(emulate_injected_media_packet(
            ProxyType::HeadUnit,
            &mut pkt,
            &mut ctx,
            None,
            false,
            &AppConfig::default(),
        )
        .unwrap());

        let cfg = protos::Config::parse_from_bytes(&pkt.payload[2..]).unwrap();
        assert_eq!(cfg.max_unacked(), 2);
        assert_eq!(cfg.configuration_indices, vec![0]);
    }
}
