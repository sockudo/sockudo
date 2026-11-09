//! Proxy-backed history, presence history, versioned messages and annotations.
//!
//! Every call POSTs a JSON body `{"channel", "action", ...}` to your backend
//! (or hands it to a [`ProxyHandler`]), which forwards it to the Sockudo REST
//! API with server credentials and returns the API response unchanged.
//!
//! | Method | `action` | Extra body fields |
//! | --- | --- | --- |
//! | [`Channel::history`] | `channel_history` (`history` on a dedicated channel-history proxy) | `params` |
//! | [`Channel::get_message`] | `get_message` | `messageSerial` |
//! | [`Channel::get_message_versions`] | `get_message_versions` | `messageSerial`, `params` |
//! | [`Channel::publish_annotation`] | `publish_annotation` | `messageSerial`, `annotation` |
//! | [`Channel::delete_annotation`] | `delete_annotation` | `messageSerial`, `annotationSerial`, `socketId` |
//! | [`Channel::list_annotations`] | `list_annotations` | `messageSerial`, `params` |
//! | [`Channel::presence_history`] | `history` (presence proxy) | `params` |
//! | [`Channel::presence_snapshot`] | `snapshot` (presence proxy) | `params` |
//! | [`VersionedMessages`] | `create` / `append` / `update` / `delete` | `params` |

use std::fmt;
use std::future::Future;
use std::sync::{Arc, Weak};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sonic_rs::{JsonValueTrait, Value};

use crate::channel::{Channel, ChannelKind};
use crate::client::Shared;
use crate::error::{Error, Result};
use crate::event::{Event, Extras, de_opt_u64};
use crate::options::ProxyOptions;
use crate::token::BoxFuture;

/// A proxy request handed to a [`ProxyHandler`].
#[derive(Debug, Clone, PartialEq)]
pub struct ProxyRequest {
    /// The `action` field, for routing.
    pub action: String,
    /// The full JSON body.
    pub body: Value,
}

/// Custom proxy transport. Implemented for
/// `Fn(ProxyRequest) -> impl Future<Output = Result<Value>>`.
pub trait ProxyHandler: Send + Sync {
    /// Perform the request and return the JSON response body.
    fn call(&self, request: ProxyRequest) -> BoxFuture<'static, Result<Value>>;
}

impl<F, Fut> ProxyHandler for F
where
    F: Fn(ProxyRequest) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Value>> + Send + 'static,
{
    fn call(&self, request: ProxyRequest) -> BoxFuture<'static, Result<Value>> {
        Box::pin(self(request))
    }
}

#[derive(Clone, Copy)]
enum Proxy {
    Presence,
    ChannelHistory,
    Versioned,
}

fn proxy_options(shared: &Shared, proxy: Proxy) -> Option<&ProxyOptions> {
    match proxy {
        Proxy::Presence => shared.config.presence_history.as_ref(),
        Proxy::ChannelHistory => shared.config.channel_history.as_ref(),
        Proxy::Versioned => shared.config.versioned_messages.as_ref(),
    }
}

fn not_configured(what: &str, option: &str) -> Error {
    Error::unsupported(format!(
        "{option} must be configured to use {what}; it should proxy requests to the Sockudo server REST API"
    ))
}

async fn call(shared: &Shared, options: &ProxyOptions, action: &str, body: Value) -> Result<Value> {
    if let Some(handler) = options.handler.as_ref() {
        let request = ProxyRequest {
            action: action.to_owned(),
            body,
        };
        return tokio::time::timeout(options.timeout, handler.call(request))
            .await
            .map_err(|_| Error::Timeout)?;
    }
    #[cfg(feature = "http")]
    if let Some(endpoint) = options.endpoint.as_ref() {
        return crate::http::post_proxy(&shared.http, endpoint, &body, options.timeout).await;
    }
    let _ = shared;
    Err(Error::unsupported(
        "proxy has neither a handler nor (with the `http` feature) an endpoint",
    ))
}

fn to_value<T: Serialize + ?Sized>(value: &T) -> Result<Value> {
    sonic_rs::to_value(value).map_err(|error| Error::Encode(error.to_string()))
}

fn from_value<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T> {
    // `sonic_rs::from_value` cannot fill structs that contain `Value` fields,
    // so go through bytes; proxy calls are network-bound anyway.
    let bytes = sonic_rs::to_vec(value).map_err(|error| Error::Decode(error.to_string()))?;
    sonic_rs::from_slice(&bytes)
        .map_err(|error| Error::Decode(format!("unexpected proxy response: {error}")))
}

/// Page ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Newest first.
    NewestFirst,
    /// Oldest first.
    OldestFirst,
}

/// Channel history query.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct ChannelHistoryParams {
    /// Ordering.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<Direction>,
    /// Page size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Opaque cursor from a previous page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Lower serial bound.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_serial: Option<u64>,
    /// Upper serial bound.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_serial: Option<u64>,
    /// Lower time bound (ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time_ms: Option<u64>,
    /// Upper time bound (ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time_ms: Option<u64>,
    /// Read history up to the subscription's attach point (gap-free late join).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub until_attach: bool,
    /// Attach serial; defaults to the channel's when `until_attach` is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attach_serial: Option<u64>,
}

impl ChannelHistoryParams {
    /// Default parameters.
    pub fn new() -> Self {
        Self::default()
    }
    /// Set the ordering (builder style).
    pub fn direction(mut self, direction: Direction) -> Self {
        self.direction = Some(direction);
        self
    }
    /// Set the page size (builder style).
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
    /// Continue from a cursor (builder style).
    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }
    /// Bound by serials (builder style).
    pub fn serial_range(mut self, start: Option<u64>, end: Option<u64>) -> Self {
        self.start_serial = start;
        self.end_serial = end;
        self
    }
    /// Bound by time in ms (builder style).
    pub fn time_range_ms(mut self, start: Option<u64>, end: Option<u64>) -> Self {
        self.start_time_ms = start;
        self.end_time_ms = end;
        self
    }
    /// Read up to the attach point (builder style).
    pub fn until_attach(mut self) -> Self {
        self.until_attach = true;
        self
    }
}

/// A page of channel history.
#[derive(Clone, Deserialize)]
#[non_exhaustive]
pub struct ChannelHistoryPage {
    /// Raw history items (message envelopes).
    #[serde(default)]
    pub items: Vec<Value>,
    /// Ordering of this page.
    #[serde(default)]
    pub direction: Option<String>,
    /// Page size used.
    #[serde(default)]
    pub limit: Option<u64>,
    /// Whether more pages exist.
    #[serde(default)]
    pub has_more: bool,
    /// Cursor for the next page.
    #[serde(default)]
    pub next_cursor: Option<String>,
    /// Attach serial echoed by the server.
    #[serde(default, deserialize_with = "de_opt_u64")]
    pub attach_serial: Option<u64>,
    /// Page bounds.
    #[serde(default)]
    pub bounds: Value,
    /// Retention/continuity metadata.
    #[serde(default)]
    pub continuity: Value,
    /// Stream state, when reported.
    #[serde(default)]
    pub stream_state: Option<Value>,
    #[serde(skip)]
    next: Option<NextChannelHistory>,
}

#[derive(Clone)]
struct NextChannelHistory {
    channel: Channel,
    params: ChannelHistoryParams,
}

impl fmt::Debug for ChannelHistoryPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelHistoryPage")
            .field("items", &self.items.len())
            .field("has_more", &self.has_more)
            .field("next_cursor", &self.next_cursor)
            .finish_non_exhaustive()
    }
}

impl ChannelHistoryPage {
    /// Whether [`ChannelHistoryPage::next`] will return a page.
    pub fn has_next(&self) -> bool {
        self.has_more && self.next_cursor.is_some()
    }

    /// Fetch the next page.
    pub async fn next(&self) -> Result<Self> {
        let (Some(cursor), Some(next), true) =
            (self.next_cursor.clone(), self.next.as_ref(), self.has_more)
        else {
            return Err(Error::NoMorePages);
        };
        let mut params = next.params.clone();
        params.cursor = Some(cursor);
        next.channel.history(params).await
    }

    /// Decode items into [`Event`]s shaped like live deliveries (for example
    /// to fold them with [`crate::versioned::reduce_mutable_message_events`]).
    ///
    /// Durable history wraps each message as
    /// `{"serial", "message_id", "stream_id", "message": {...}}`; the
    /// message's `event` is the versioned envelope (`sockudo:message.create`)
    /// and [`Event::name`] carries the publisher's event name. Items that are
    /// not message envelopes are skipped.
    pub fn events(&self) -> Vec<Event> {
        self.items.iter().filter_map(history_item_event).collect()
    }
}

fn history_item_event(item: &Value) -> Option<Event> {
    let message = item
        .get("message")
        .filter(|message| message.is_object())
        .unwrap_or(item);
    let bytes = sonic_rs::to_vec(message).ok()?;
    let mut event = crate::codec::json::decode(Bytes::from(bytes)).ok()?;
    if event.event.is_empty() {
        event.event = item
            .get("event_name")
            .and_then(|name| name.as_str())?
            .to_owned();
    }
    let text = |key: &str| {
        item.get(key)
            .and_then(|value| value.as_str())
            .map(str::to_owned)
    };
    if event.message_id.is_none() {
        event.message_id = text("message_id");
    }
    if event.stream_id.is_none() {
        event.stream_id = text("stream_id");
    }
    if event.serial.is_none() {
        event.serial = item.get("serial").and_then(crate::event::value_to_u64);
    }
    Some(event)
}

/// Message-version query.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct MessageVersionsParams {
    /// Ordering.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<Direction>,
    /// Page size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl MessageVersionsParams {
    /// Default parameters.
    pub fn new() -> Self {
        Self::default()
    }
    /// Set the ordering (builder style).
    pub fn direction(mut self, direction: Direction) -> Self {
        self.direction = Some(direction);
        self
    }
    /// Set the page size (builder style).
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// The latest version of one message.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct GetMessageResponse {
    /// Channel.
    #[serde(default)]
    pub channel: String,
    /// The message item.
    #[serde(default)]
    pub item: Value,
}

/// A page of message versions.
#[derive(Clone, Deserialize)]
#[non_exhaustive]
pub struct MessageVersionsPage {
    /// Channel.
    #[serde(default)]
    pub channel: String,
    /// Ordering.
    #[serde(default)]
    pub direction: Option<String>,
    /// Page size.
    #[serde(default)]
    pub limit: Option<u64>,
    /// Whether more pages exist.
    #[serde(default)]
    pub has_more: bool,
    /// Next cursor.
    #[serde(default)]
    pub next_cursor: Option<String>,
    /// Versions.
    #[serde(default)]
    pub items: Vec<Value>,
    #[serde(skip)]
    next: Option<(Channel, String, MessageVersionsParams)>,
}

impl fmt::Debug for MessageVersionsPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageVersionsPage")
            .field("items", &self.items.len())
            .field("has_more", &self.has_more)
            .finish_non_exhaustive()
    }
}

impl MessageVersionsPage {
    /// Whether another page exists.
    pub fn has_next(&self) -> bool {
        self.has_more && self.next_cursor.is_some()
    }

    /// Fetch the next page.
    pub async fn next(&self) -> Result<Self> {
        let (Some(cursor), Some((channel, serial, params)), true) =
            (self.next_cursor.clone(), self.next.as_ref(), self.has_more)
        else {
            return Err(Error::NoMorePages);
        };
        let mut params = params.clone();
        params.cursor = Some(cursor);
        channel.get_message_versions(serial, params).await
    }
}

/// An annotation to publish on a message.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct PublishAnnotation {
    /// Annotation type, e.g. `reaction:distinct.v1`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Annotation name, e.g. an emoji.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Publishing client id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// Publishing socket id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_id: Option<String>,
    /// Count for multiple-count types.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
    /// Arbitrary data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    /// Data encoding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
}

impl PublishAnnotation {
    /// An annotation of `kind`.
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            ..Self::default()
        }
    }
    /// Set the name (builder style).
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
    /// Set the count (builder style).
    pub fn count(mut self, count: u64) -> Self {
        self.count = Some(count);
        self
    }
    /// Set data (builder style).
    pub fn data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }
    /// Set the client id (builder style).
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }
}

/// Result of publishing an annotation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct PublishAnnotationResponse {
    /// Serial of the new annotation.
    pub annotation_serial: String,
}

/// Result of deleting an annotation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DeleteAnnotationResponse {
    /// Serial of the delete event.
    pub annotation_serial: String,
    /// Serial of the annotation that was deleted.
    pub deleted_annotation_serial: String,
}

/// Annotation listing query.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AnnotationEventsParams {
    /// Filter by type.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Page size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Continue after this serial.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_serial: Option<String>,
    /// Socket id (for own-annotation visibility rules).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_id: Option<String>,
}

impl AnnotationEventsParams {
    /// Default parameters.
    pub fn new() -> Self {
        Self::default()
    }
    /// Filter by type (builder style).
    pub fn kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }
    /// Set the page size (builder style).
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// One raw annotation event.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AnnotationEvent {
    /// `annotation.create` or `annotation.delete`.
    pub action: String,
    /// Annotation id.
    #[serde(default)]
    pub id: Option<String>,
    /// Annotation serial.
    pub serial: String,
    /// Annotated message serial.
    pub message_serial: String,
    /// Annotation type.
    #[serde(rename = "type")]
    pub kind: String,
    /// Annotation name.
    #[serde(default)]
    pub name: Option<String>,
    /// Publishing client id.
    #[serde(default)]
    pub client_id: Option<String>,
    /// Count.
    #[serde(default)]
    pub count: Option<u64>,
    /// Data.
    #[serde(default)]
    pub data: Option<Value>,
    /// Data encoding.
    #[serde(default)]
    pub encoding: Option<String>,
    /// Timestamp (ms).
    #[serde(default)]
    pub timestamp: u64,
}

/// A page of annotation events.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AnnotationEventsPage {
    /// Channel.
    #[serde(default)]
    pub channel: String,
    /// Annotated message serial.
    #[serde(default)]
    pub message_serial: String,
    /// Page size.
    #[serde(default)]
    pub limit: Option<u64>,
    /// Whether more pages exist.
    #[serde(default)]
    pub has_more: bool,
    /// Cursor (a `fromSerial` for the next page).
    #[serde(default)]
    pub next_cursor: Option<String>,
    /// Events.
    #[serde(default)]
    pub items: Vec<AnnotationEvent>,
    #[serde(skip)]
    next: Option<(Channel, String, AnnotationEventsParams)>,
}

impl fmt::Debug for AnnotationEventsPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnnotationEventsPage")
            .field("items", &self.items.len())
            .field("has_more", &self.has_more)
            .finish_non_exhaustive()
    }
}

impl AnnotationEventsPage {
    /// Whether another page exists.
    pub fn has_next(&self) -> bool {
        self.has_more && self.next_cursor.is_some()
    }

    /// Fetch the next page.
    pub async fn next(&self) -> Result<Self> {
        let (Some(cursor), Some((channel, serial, params)), true) =
            (self.next_cursor.clone(), self.next.as_ref(), self.has_more)
        else {
            return Err(Error::NoMorePages);
        };
        let mut params = params.clone();
        params.from_serial = Some(cursor);
        channel.list_annotations(serial, params).await
    }
}

/// Presence history query.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct PresenceHistoryParams {
    /// Ordering.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<Direction>,
    /// Page size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Lower serial bound.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_serial: Option<u64>,
    /// Upper serial bound.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_serial: Option<u64>,
    /// Lower time bound (ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time_ms: Option<u64>,
    /// Upper time bound (ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time_ms: Option<u64>,
}

impl PresenceHistoryParams {
    /// Default parameters.
    pub fn new() -> Self {
        Self::default()
    }
    /// Set the ordering (builder style).
    pub fn direction(mut self, direction: Direction) -> Self {
        self.direction = Some(direction);
        self
    }
    /// Set the page size (builder style).
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Retention and continuity of presence history.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct PresenceHistoryContinuity {
    /// Stream id.
    pub stream_id: Option<String>,
    /// Oldest retained serial.
    #[serde(deserialize_with = "de_opt_u64")]
    pub oldest_available_serial: Option<u64>,
    /// Newest retained serial.
    #[serde(deserialize_with = "de_opt_u64")]
    pub newest_available_serial: Option<u64>,
    /// Oldest retained timestamp (ms).
    #[serde(deserialize_with = "de_opt_u64")]
    pub oldest_available_published_at_ms: Option<u64>,
    /// Newest retained timestamp (ms).
    #[serde(deserialize_with = "de_opt_u64")]
    pub newest_available_published_at_ms: Option<u64>,
    /// Retained event count.
    pub retained_events: u64,
    /// Retained bytes.
    pub retained_bytes: u64,
    /// History is degraded (some transitions may be missing).
    pub degraded: bool,
    /// The requested range is complete.
    pub complete: bool,
    /// Retention truncated the range.
    pub truncated_by_retention: bool,
}

/// Bounds of a presence history page.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct PresenceHistoryBounds {
    /// First serial.
    #[serde(deserialize_with = "de_opt_u64")]
    pub start_serial: Option<u64>,
    /// Last serial.
    #[serde(deserialize_with = "de_opt_u64")]
    pub end_serial: Option<u64>,
    /// First timestamp (ms).
    #[serde(deserialize_with = "de_opt_u64")]
    pub start_time_ms: Option<u64>,
    /// Last timestamp (ms).
    #[serde(deserialize_with = "de_opt_u64")]
    pub end_time_ms: Option<u64>,
}

/// One presence transition.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[non_exhaustive]
pub struct PresenceHistoryItem {
    /// Stream id.
    #[serde(default)]
    pub stream_id: String,
    /// Serial.
    pub serial: u64,
    /// Publication time (ms).
    #[serde(default)]
    pub published_at_ms: u64,
    /// `member_added` / `member_removed` / `member_updated`.
    pub event: String,
    /// Cause (join, leave, disconnect, node failure…).
    #[serde(default)]
    pub cause: String,
    /// User id.
    pub user_id: String,
    /// Connection id.
    #[serde(default)]
    pub connection_id: Option<String>,
    /// Dead node id for node-failure removals.
    #[serde(default)]
    pub dead_node_id: Option<String>,
    /// Payload size.
    #[serde(default)]
    pub payload_size_bytes: u64,
    /// The presence event as delivered.
    #[serde(default)]
    pub presence_event: Value,
}

/// A page of presence history.
#[derive(Clone, Deserialize)]
#[non_exhaustive]
pub struct PresenceHistoryPage {
    /// Transitions.
    #[serde(default)]
    pub items: Vec<PresenceHistoryItem>,
    /// Ordering.
    #[serde(default)]
    pub direction: Option<String>,
    /// Page size.
    #[serde(default)]
    pub limit: Option<u64>,
    /// Whether more pages exist.
    #[serde(default)]
    pub has_more: bool,
    /// Next cursor.
    #[serde(default)]
    pub next_cursor: Option<String>,
    /// Bounds.
    #[serde(default)]
    pub bounds: PresenceHistoryBounds,
    /// Continuity.
    #[serde(default)]
    pub continuity: PresenceHistoryContinuity,
    #[serde(skip)]
    next: Option<(Channel, PresenceHistoryParams)>,
}

impl fmt::Debug for PresenceHistoryPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PresenceHistoryPage")
            .field("items", &self.items.len())
            .field("has_more", &self.has_more)
            .finish_non_exhaustive()
    }
}

impl PresenceHistoryPage {
    /// Whether another page exists.
    pub fn has_next(&self) -> bool {
        self.has_more && self.next_cursor.is_some()
    }

    /// Fetch the next page.
    pub async fn next(&self) -> Result<Self> {
        let (Some(cursor), Some((channel, params)), true) =
            (self.next_cursor.clone(), self.next.as_ref(), self.has_more)
        else {
            return Err(Error::NoMorePages);
        };
        let mut params = params.clone();
        params.cursor = Some(cursor);
        channel.presence_history(params).await
    }
}

/// Presence snapshot query.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct PresenceSnapshotParams {
    /// Snapshot at this time (ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at_time_ms: Option<u64>,
    /// Snapshot at this serial.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at_serial: Option<u64>,
}

impl PresenceSnapshotParams {
    /// Latest snapshot.
    pub fn new() -> Self {
        Self::default()
    }
    /// At a time (builder style).
    pub fn at_time_ms(mut self, at: u64) -> Self {
        self.at_time_ms = Some(at);
        self
    }
    /// At a serial (builder style).
    pub fn at_serial(mut self, serial: u64) -> Self {
        self.at_serial = Some(serial);
        self
    }
}

/// A member in a presence snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[non_exhaustive]
pub struct PresenceSnapshotMember {
    /// User id.
    pub user_id: String,
    /// Last transition.
    pub last_event: String,
    /// Serial of the last transition.
    pub last_event_serial: u64,
    /// Time of the last transition (ms).
    pub last_event_at_ms: u64,
}

/// Membership reconstructed at a point in time.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[non_exhaustive]
pub struct PresenceSnapshot {
    /// Channel.
    #[serde(default)]
    pub channel: String,
    /// Members.
    #[serde(default)]
    pub members: Vec<PresenceSnapshotMember>,
    /// Member count.
    #[serde(default)]
    pub member_count: u64,
    /// Transitions replayed to build the snapshot.
    #[serde(default)]
    pub events_replayed: u64,
    /// Serial of the snapshot.
    #[serde(default, deserialize_with = "de_opt_u64")]
    pub snapshot_serial: Option<u64>,
    /// Time of the snapshot (ms).
    #[serde(default, deserialize_with = "de_opt_u64")]
    pub snapshot_time_ms: Option<u64>,
    /// Continuity.
    #[serde(default)]
    pub continuity: PresenceHistoryContinuity,
}

/// Acknowledgement of a versioned-message mutation.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct VersionedMessageAck {
    /// Action performed.
    pub action: String,
    /// Channel.
    pub channel: String,
    /// Event name.
    pub event: Option<String>,
    /// Message id.
    pub message_id: Option<String>,
    /// Version id.
    pub version_id: Option<String>,
    /// Message serial.
    pub message_serial: Option<u64>,
    /// Version serial.
    pub version_serial: Option<u64>,
    /// History serial.
    pub history_serial: Option<u64>,
    /// Delivery serial.
    pub delivery_serial: Option<u64>,
    /// The raw acknowledgement.
    pub raw: Value,
}

impl VersionedMessageAck {
    fn from_response(payload: &Value, action: &str, channel: &str) -> Self {
        let ack = payload
            .get("ack")
            .filter(|ack| ack.is_object())
            .unwrap_or(payload);
        let text = |names: &[&str]| {
            names
                .iter()
                .find_map(|name| ack.get(*name).and_then(|value| value.as_str()))
                .map(str::to_owned)
        };
        let serial = |name: &str| ack.get(name).and_then(crate::event::value_to_u64);
        Self {
            action: text(&["action"]).unwrap_or_else(|| action.to_owned()),
            channel: text(&["channel"]).unwrap_or_else(|| channel.to_owned()),
            event: text(&["event"]),
            message_id: text(&["message_id", "id"]),
            version_id: text(&["version_id", "version"]),
            message_serial: serial("message_serial"),
            version_serial: serial("version_serial"),
            history_serial: serial("history_serial"),
            delivery_serial: serial("delivery_serial"),
            raw: ack.clone(),
        }
    }
}

impl Channel {
    fn proxy_shared(&self) -> Result<Arc<Shared>> {
        self.shared()
    }

    async fn versioned_call(
        &self,
        what: &str,
        action: &str,
        mut body: sonic_rs::Object,
    ) -> Result<Value> {
        let shared = self.proxy_shared()?;
        let options = proxy_options(&shared, Proxy::Versioned)
            .ok_or_else(|| not_configured(what, "versioned_messages"))?;
        body.insert("channel", Value::from(self.name()));
        body.insert("action", Value::from(action));
        call(&shared, options, action, Value::from(body)).await
    }

    /// Fetch a page of channel history.
    pub async fn history(&self, params: ChannelHistoryParams) -> Result<ChannelHistoryPage> {
        let shared = self.proxy_shared()?;
        let mut wire = params.clone();
        if wire.until_attach && wire.attach_serial.is_none() {
            wire.attach_serial = self.attach_serial();
        }
        let (options, action) = match proxy_options(&shared, Proxy::ChannelHistory) {
            Some(options) => (options, "history"),
            None => (
                proxy_options(&shared, Proxy::Versioned).ok_or_else(|| {
                    not_configured("channel.history()", "channel_history or versioned_messages")
                })?,
                "channel_history",
            ),
        };
        let body = sonic_rs::json!({
            "channel": self.name(),
            "params": to_value(&wire)?,
            "action": action,
        });
        let response = call(&shared, options, action, body).await?;
        let mut page: ChannelHistoryPage = from_value(&response)?;
        page.next = Some(NextChannelHistory {
            channel: self.clone(),
            params,
        });
        Ok(page)
    }

    /// Fetch the latest version of a message.
    pub async fn get_message(&self, message_serial: &str) -> Result<GetMessageResponse> {
        let mut body = sonic_rs::Object::new();
        body.insert("messageSerial", Value::from(message_serial));
        let response = self
            .versioned_call("channel.get_message()", "get_message", body)
            .await?;
        from_value(&response)
    }

    /// Fetch the version history of a message.
    pub async fn get_message_versions(
        &self,
        message_serial: &str,
        params: MessageVersionsParams,
    ) -> Result<MessageVersionsPage> {
        let mut body = sonic_rs::Object::new();
        body.insert("messageSerial", Value::from(message_serial));
        body.insert("params", to_value(&params)?);
        let response = self
            .versioned_call(
                "channel.get_message_versions()",
                "get_message_versions",
                body,
            )
            .await?;
        let mut page: MessageVersionsPage = from_value(&response)?;
        page.next = Some((self.clone(), message_serial.to_owned(), params));
        Ok(page)
    }

    /// Publish an annotation on a message.
    pub async fn publish_annotation(
        &self,
        message_serial: &str,
        annotation: PublishAnnotation,
    ) -> Result<PublishAnnotationResponse> {
        let mut body = sonic_rs::Object::new();
        body.insert("messageSerial", Value::from(message_serial));
        body.insert("annotation", to_value(&annotation)?);
        let response = self
            .versioned_call("channel.publish_annotation()", "publish_annotation", body)
            .await?;
        from_value(&response)
    }

    /// Delete an annotation.
    pub async fn delete_annotation(
        &self,
        message_serial: &str,
        annotation_serial: &str,
        socket_id: Option<&str>,
    ) -> Result<DeleteAnnotationResponse> {
        let mut body = sonic_rs::Object::new();
        body.insert("messageSerial", Value::from(message_serial));
        body.insert("annotationSerial", Value::from(annotation_serial));
        if let Some(socket_id) = socket_id {
            body.insert("socketId", Value::from(socket_id));
        }
        let response = self
            .versioned_call("channel.delete_annotation()", "delete_annotation", body)
            .await?;
        from_value(&response)
    }

    /// List raw annotation events of a message.
    pub async fn list_annotations(
        &self,
        message_serial: &str,
        params: AnnotationEventsParams,
    ) -> Result<AnnotationEventsPage> {
        let mut body = sonic_rs::Object::new();
        body.insert("messageSerial", Value::from(message_serial));
        body.insert("params", to_value(&params)?);
        let response = self
            .versioned_call("channel.list_annotations()", "list_annotations", body)
            .await?;
        let mut page: AnnotationEventsPage = from_value(&response)?;
        page.next = Some((self.clone(), message_serial.to_owned(), params));
        Ok(page)
    }

    async fn presence_call(&self, what: &str, action: &str, params: Value) -> Result<Value> {
        if self.kind() != ChannelKind::Presence {
            return Err(Error::unsupported(format!(
                "{what} requires a presence channel"
            )));
        }
        let shared = self.proxy_shared()?;
        let options = proxy_options(&shared, Proxy::Presence)
            .ok_or_else(|| not_configured(what, "presence_history"))?;
        let body = sonic_rs::json!({
            "channel": self.name(),
            "params": params,
            "action": action,
        });
        call(&shared, options, action, body).await
    }

    /// Fetch a page of presence history (presence channels).
    pub async fn presence_history(
        &self,
        params: PresenceHistoryParams,
    ) -> Result<PresenceHistoryPage> {
        let response = self
            .presence_call("channel.presence_history()", "history", to_value(&params)?)
            .await?;
        let mut page: PresenceHistoryPage = from_value(&response)?;
        page.next = Some((self.clone(), params));
        Ok(page)
    }

    /// Reconstruct membership at a point in time (presence channels).
    pub async fn presence_snapshot(
        &self,
        params: PresenceSnapshotParams,
    ) -> Result<PresenceSnapshot> {
        let response = self
            .presence_call(
                "channel.presence_snapshot()",
                "snapshot",
                to_value(&params)?,
            )
            .await?;
        from_value(&response)
    }

    /// Create a versioned message through the proxy.
    pub async fn create_versioned_message<T: Serialize + ?Sized>(
        &self,
        event: &str,
        data: &T,
        extras: Option<&Extras>,
    ) -> Result<VersionedMessageAck> {
        VersionedMessages::new(self.inner().shared.clone())
            .create(self.name(), event, data, extras)
            .await
    }

    /// Append to a versioned message through the proxy.
    pub async fn append_versioned_message<T: Serialize + ?Sized>(
        &self,
        message_id: &str,
        data: &T,
        extras: Option<&Extras>,
    ) -> Result<VersionedMessageAck> {
        VersionedMessages::new(self.inner().shared.clone())
            .append(self.name(), message_id, data, extras)
            .await
    }

    /// Replace a versioned message through the proxy.
    pub async fn update_versioned_message<T: Serialize + ?Sized>(
        &self,
        message_id: &str,
        data: &T,
        extras: Option<&Extras>,
    ) -> Result<VersionedMessageAck> {
        VersionedMessages::new(self.inner().shared.clone())
            .update(self.name(), message_id, data, extras)
            .await
    }

    /// Delete a versioned message through the proxy.
    pub async fn delete_versioned_message(
        &self,
        message_id: &str,
        reason: Option<&str>,
    ) -> Result<VersionedMessageAck> {
        VersionedMessages::new(self.inner().shared.clone())
            .delete(self.name(), message_id, reason)
            .await
    }
}

/// Versioned-message mutations through the `versioned_messages` proxy.
#[derive(Clone)]
pub struct VersionedMessages {
    shared: Weak<Shared>,
}

impl fmt::Debug for VersionedMessages {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VersionedMessages")
    }
}

impl VersionedMessages {
    pub(crate) fn new(shared: Weak<Shared>) -> Self {
        Self { shared }
    }

    async fn mutate(
        &self,
        channel: &str,
        action: &str,
        params: sonic_rs::Object,
    ) -> Result<VersionedMessageAck> {
        let shared = self.shared.upgrade().ok_or(Error::Closed)?;
        let options = proxy_options(&shared, Proxy::Versioned)
            .ok_or_else(|| not_configured("versioned_messages()", "versioned_messages"))?;
        let body = sonic_rs::json!({
            "channel": channel,
            "params": Value::from(params),
            "action": action,
        });
        let response = call(&shared, options, action, body).await?;
        Ok(VersionedMessageAck::from_response(
            &response, action, channel,
        ))
    }

    fn with_payload<T: Serialize + ?Sized>(
        mut params: sonic_rs::Object,
        data: &T,
        extras: Option<&Extras>,
    ) -> Result<sonic_rs::Object> {
        params.insert("data", to_value(data)?);
        if let Some(extras) = extras {
            params.insert("extras", to_value(extras)?);
        }
        Ok(params)
    }

    /// Create a message.
    pub async fn create<T: Serialize + ?Sized>(
        &self,
        channel: &str,
        event: &str,
        data: &T,
        extras: Option<&Extras>,
    ) -> Result<VersionedMessageAck> {
        let mut params = sonic_rs::Object::new();
        params.insert("event", Value::from(event));
        self.mutate(channel, "create", Self::with_payload(params, data, extras)?)
            .await
    }

    /// Append a fragment to a message.
    pub async fn append<T: Serialize + ?Sized>(
        &self,
        channel: &str,
        message_id: &str,
        data: &T,
        extras: Option<&Extras>,
    ) -> Result<VersionedMessageAck> {
        let mut params = sonic_rs::Object::new();
        params.insert("message_id", Value::from(message_id));
        self.mutate(channel, "append", Self::with_payload(params, data, extras)?)
            .await
    }

    /// Replace a message's data.
    pub async fn update<T: Serialize + ?Sized>(
        &self,
        channel: &str,
        message_id: &str,
        data: &T,
        extras: Option<&Extras>,
    ) -> Result<VersionedMessageAck> {
        let mut params = sonic_rs::Object::new();
        params.insert("message_id", Value::from(message_id));
        self.mutate(channel, "update", Self::with_payload(params, data, extras)?)
            .await
    }

    /// Delete a message.
    pub async fn delete(
        &self,
        channel: &str,
        message_id: &str,
        reason: Option<&str>,
    ) -> Result<VersionedMessageAck> {
        let mut params = sonic_rs::Object::new();
        params.insert("message_id", Value::from(message_id));
        if let Some(reason) = reason {
            params.insert("reason", Value::from(reason));
        }
        self.mutate(channel, "delete", params).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_params_serialize_compactly() {
        let params = ChannelHistoryParams::new()
            .direction(Direction::NewestFirst)
            .limit(50)
            .until_attach();
        assert_eq!(
            sonic_rs::to_string(&params).unwrap(),
            r#"{"direction":"newest_first","limit":50,"until_attach":true}"#
        );
        assert_eq!(
            sonic_rs::to_string(&ChannelHistoryParams::new()).unwrap(),
            "{}"
        );
    }

    #[test]
    fn annotation_payloads_use_camel_case() {
        let annotation = PublishAnnotation::new("reaction:distinct.v1")
            .name("👍")
            .count(1);
        assert_eq!(
            sonic_rs::to_string(&annotation).unwrap(),
            r#"{"type":"reaction:distinct.v1","name":"👍","count":1}"#
        );
        let params = AnnotationEventsParams::new().kind("reaction").limit(10);
        assert_eq!(
            sonic_rs::to_string(&params).unwrap(),
            r#"{"type":"reaction","limit":10}"#
        );
    }

    #[test]
    fn decodes_pages_and_acks() {
        let page: ChannelHistoryPage = from_value(&sonic_rs::json!({
            "items": [{"event": "e", "channel": "c", "data": "x", "serial": 3}],
            "direction": "oldest_first", "limit": 10, "has_more": true, "next_cursor": "abc",
            "attach_serial": "7"
        }))
        .unwrap();
        assert!(page.has_next());
        assert_eq!(page.attach_serial, Some(7));
        assert_eq!(page.events()[0].serial, Some(3));

        // The durable-history item shape returned by the server.
        let durable: ChannelHistoryPage = from_value(&sonic_rs::json!({
            "items": [{
                "serial": 4, "message_id": "m4", "stream_id": "s1", "event_name": "sockudo:message.create",
                "message": {"event": "sockudo:message.create", "name": "chat", "channel": "room",
                             "data": "{\"n\":4}", "action": "create",
                             "extras": {"headers": {"sockudo_action": "message.create", "sockudo_message_serial": "ms4"}}}
            }]
        }))
        .unwrap();
        let event = &durable.events()[0];
        assert_eq!(event.event, "sockudo:message.create");
        assert_eq!(event.logical_name(), "chat");
        assert_eq!(
            (event.serial, event.message_id.as_deref()),
            (Some(4), Some("m4"))
        );
        assert_eq!(event.stream_id.as_deref(), Some("s1"));
        assert_eq!(event.value()["n"].as_i64(), Some(4));
        assert!(crate::versioned::is_mutable_message_event(event));

        let ack = VersionedMessageAck::from_response(
            &sonic_rs::json!({"ack": {"id": "m1", "version": "v2", "message_serial": 5}}),
            "update",
            "room",
        );
        assert_eq!(ack.action, "update");
        assert_eq!(ack.channel, "room");
        assert_eq!(ack.message_id.as_deref(), Some("m1"));
        assert_eq!(ack.version_id.as_deref(), Some("v2"));
        assert_eq!(ack.message_serial, Some(5));

        let presence: PresenceHistoryPage = from_value(&sonic_rs::json!({
            "items": [{"stream_id": "s", "serial": 1, "published_at_ms": 2, "event": "member_added",
                        "cause": "join", "user_id": "u", "payload_size_bytes": 3, "presence_event": {}}],
            "has_more": false,
            "continuity": {"retained_events": 1, "complete": true}
        }))
        .unwrap();
        assert_eq!(presence.items[0].user_id, "u");
        assert!(presence.continuity.complete);
        assert!(!presence.has_next());
    }
}
