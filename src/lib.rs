//! Core abstractions for Hyperware's publish/subscribe transport layer.
//!
//! This crate intentionally focuses on data types and traits that concrete
//! broker implementations can build upon. It does not ship with a default
//! broker but instead provides strongly typed envelopes, subscription
//! contracts, and result types consumed by both servers and SDKs.

use std::{
    collections::BTreeMap,
    fmt,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub mod router;
pub mod storage;

/// Headers that travel alongside an envelope.
pub type HeaderMap = BTreeMap<String, String>;

/// Stream of deliveries yielded by subscriptions.
pub type DeliveryStream =
    Pin<Box<dyn Stream<Item = Result<Delivery, BrokerError>> + Send + 'static>>;

/// Stream of snapshot chunks useful for bootstrap operations.
pub type SnapshotStream =
    Pin<Box<dyn Stream<Item = Result<SnapshotChunk, BrokerError>> + Send + 'static>>;

/// Identifier for a topic.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicId(String);

impl TopicId {
    /// Creates a new topic identifier from a string-like value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Access the raw string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TopicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for TopicId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for TopicId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Unique identifier for a single message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(Uuid);

impl MessageId {
    /// Generates a random identifier.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wraps an existing UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Access the raw UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Reference to a different message, often used for threading or dedupe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageRef {
    pub topic: TopicId,
    pub id: MessageId,
}

/// Envelope that travels across brokers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub id: MessageId,
    pub topic: TopicId,
    pub parent: Option<MessageRef>,
    pub headers: HeaderMap,
    pub payload: Bytes,
    pub created_at: SystemTime,
    pub correlation_id: Option<String>,
}

impl Envelope {
    /// Helper constructor with reasonable defaults.
    pub fn new(topic: impl Into<TopicId>, payload: Bytes) -> Self {
        Self {
            id: MessageId::new(),
            topic: topic.into(),
            parent: None,
            headers: HeaderMap::default(),
            payload,
            created_at: SystemTime::now(),
            correlation_id: None,
        }
    }
}

/// Strategy for retaining messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Persistence {
    Ephemeral,
    Durable,
}

/// Fanout scope for a publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FanoutScope {
    Broadcast,
    Audience(Audience),
}

impl Default for FanoutScope {
    fn default() -> Self {
        Self::Broadcast
    }
}

/// Audience labels are interpreted by routing policies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Audience {
    pub label: String,
    pub feature: Option<String>,
}

impl Audience {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            feature: None,
        }
    }
}

/// Dedupe scope indicates how duplicate detection should behave.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DedupeScope {
    None,
    ByMessage,
    Custom { key: String },
}

impl Default for DedupeScope {
    fn default() -> Self {
        Self::ByMessage
    }
}

/// Options provided by a publisher with each envelope.
#[derive(Clone, Debug)]
pub struct PublishOptions {
    pub persistence: Persistence,
    pub fanout: FanoutScope,
    pub dedupe: DedupeScope,
    pub ttl: Option<Duration>,
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            persistence: Persistence::Durable,
            fanout: FanoutScope::default(),
            dedupe: DedupeScope::default(),
            ttl: None,
        }
    }
}

/// Flow control settings requested by a subscriber.
#[derive(Clone, Debug)]
pub struct FlowControl {
    pub max_inflight: usize,
    pub batch_size: usize,
}

impl Default for FlowControl {
    fn default() -> Self {
        Self {
            max_inflight: 1024,
            batch_size: 64,
        }
    }
}

/// Cursor representing where consumption should start.
#[derive(Clone, Debug)]
pub enum Cursor {
    Start,
    End,
    Offset(u64),
    Message(MessageRef),
}

impl Default for Cursor {
    fn default() -> Self {
        Self::End
    }
}

/// Field-level filters.
#[derive(Clone, Debug, Default)]
pub struct AttributeFilter {
    pub equals: HeaderMap,
}

/// Request used to register a subscription.
#[derive(Clone, Debug)]
pub struct SubscribeRequest {
    pub topic: TopicId,
    pub cursor: Cursor,
    pub filter: AttributeFilter,
    pub flow_control: FlowControl,
    pub audience: Option<String>,
    pub feature: Option<String>,
}

impl SubscribeRequest {
    pub fn new(topic: impl Into<TopicId>) -> Self {
        Self {
            topic: topic.into(),
            cursor: Cursor::default(),
            filter: AttributeFilter::default(),
            flow_control: FlowControl::default(),
            audience: None,
            feature: None,
        }
    }
}

/// Identifier assigned to a subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(Uuid);

impl SubscriptionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl Default for SubscriptionId {
    fn default() -> Self {
        Self::new()
    }
}

/// A delivery plus acknowledgement handle.
#[derive(Debug)]
pub struct Delivery {
    pub envelope: Envelope,
    pub ack: AckHandle,
}

/// Handle consumers call to confirm processing.
pub struct AckHandle {
    inner: Arc<dyn DeliveryAck>,
}

impl AckHandle {
    pub fn new(ack: Arc<dyn DeliveryAck>) -> Self {
        Self { inner: ack }
    }

    pub async fn ack(&mut self) -> Result<(), BrokerError> {
        self.inner.ack().await
    }

    pub async fn nack(&mut self, reason: Option<String>) -> Result<(), BrokerError> {
        self.inner.nack(reason).await
    }

    pub async fn retry(&mut self, delay: Option<Duration>) -> Result<(), BrokerError> {
        self.inner.retry(delay).await
    }
}

impl fmt::Debug for AckHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AckHandle").finish()
    }
}

/// Trait implemented by backend-specific acknowledgement tokens.
#[async_trait]
pub trait DeliveryAck: Send + Sync + 'static {
    async fn ack(&self) -> Result<(), BrokerError>;
    async fn nack(&self, reason: Option<String>) -> Result<(), BrokerError>;
    async fn retry(&self, delay: Option<Duration>) -> Result<(), BrokerError>;
}

/// Handle returned when a subscription is registered.
pub struct SubscriptionHandle {
    id: SubscriptionId,
    stream: DeliveryStream,
}

impl SubscriptionHandle {
    pub fn new(id: SubscriptionId, stream: DeliveryStream) -> Self {
        Self { id, stream }
    }

    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    pub fn into_stream(self) -> DeliveryStream {
        self.stream
    }
}

/// Request for snapshot data, typically used by new hubs.
#[derive(Clone, Debug)]
pub struct SnapshotRequest {
    pub topic: TopicId,
    pub cursor: Cursor,
    pub limit: Option<usize>,
}

/// Snapshot data chunk.
#[derive(Clone, Debug)]
pub struct SnapshotChunk {
    pub cursor: Cursor,
    pub envelopes: Vec<Envelope>,
}

/// Information returned after a publish call.
#[derive(Clone, Debug)]
pub struct PublishResult {
    pub message_id: MessageId,
    pub persisted: bool,
    pub deduplicated: bool,
}

impl PublishResult {
    pub fn new(message_id: MessageId) -> Self {
        Self {
            message_id,
            persisted: true,
            deduplicated: false,
        }
    }
}

/// Errors surfaced by broker implementations.
#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("topic not found: {0}")]
    TopicNotFound(TopicId),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("subscription rejected: {0}")]
    SubscriptionRejected(String),
    #[error("publish rejected: {0}")]
    PublishRejected(String),
    #[error("operation unsupported: {0}")]
    Unsupported(String),
    #[error("I/O error: {0}")]
    Io(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("cursor invalid: {0}")]
    CursorInvalid(String),
}

/// Trait implemented by broker frontends.
#[async_trait]
pub trait Broker: Send + Sync {
    async fn publish(
        &self,
        envelope: Envelope,
        options: PublishOptions,
    ) -> Result<PublishResult, BrokerError>;

    async fn subscribe(&self, request: SubscribeRequest)
    -> Result<SubscriptionHandle, BrokerError>;

    async fn snapshot(&self, request: SnapshotRequest) -> Result<SnapshotStream, BrokerError>;

    async fn revoke(&self, _id: SubscriptionId) -> Result<(), BrokerError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn envelope_helper_sets_defaults() {
        let payload = Bytes::from_static(b"hello");
        let envelope = Envelope::new("topic.alpha", payload.clone());

        assert_eq!(envelope.topic.as_str(), "topic.alpha");
        assert_eq!(envelope.payload, payload);
        assert!(envelope.correlation_id.is_none());
    }

    #[test]
    fn publish_options_default_to_durable() {
        let opts = PublishOptions::default();
        assert!(matches!(opts.persistence, Persistence::Durable));
    }
}
