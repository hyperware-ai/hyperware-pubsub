//! Storage abstractions for persisting pub/sub messages.

use std::{collections::HashMap, sync::Mutex};

use async_trait::async_trait;

use crate::{BrokerError, Cursor, Envelope, MessageRef, TopicId};

/// Stored message with offset metadata.
#[derive(Clone, Debug)]
pub struct StoredMessage {
    pub offset: u64,
    pub envelope: Envelope,
}

#[async_trait]
pub trait QueueStore: Send + Sync {
    async fn append(&self, envelope: Envelope) -> Result<StoredMessage, BrokerError>;

    async fn snapshot(
        &self,
        topic: &TopicId,
        cursor: Cursor,
        limit: Option<usize>,
    ) -> Result<Vec<StoredMessage>, BrokerError>;

    async fn len(&self, topic: &TopicId) -> Result<usize, BrokerError>;
}

#[derive(Default)]
pub struct InMemoryQueueStore {
    topics: Mutex<HashMap<TopicId, Vec<StoredMessage>>>,
}

impl InMemoryQueueStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl QueueStore for InMemoryQueueStore {
    async fn append(&self, envelope: Envelope) -> Result<StoredMessage, BrokerError> {
        let mut guard = self.topics.lock().expect("store poisoned");
        let entry = guard.entry(envelope.topic.clone()).or_default();
        let offset = entry.last().map(|m| m.offset + 1).unwrap_or(0);
        let stored = StoredMessage { offset, envelope };
        entry.push(stored.clone());
        Ok(stored)
    }

    async fn snapshot(
        &self,
        topic: &TopicId,
        cursor: Cursor,
        limit: Option<usize>,
    ) -> Result<Vec<StoredMessage>, BrokerError> {
        let guard = self.topics.lock().expect("store poisoned");
        let messages = match guard.get(topic) {
            Some(messages) => messages,
            None => return Ok(Vec::new()),
        };

        let start = resolve_start_index(messages, cursor, topic)?;
        let limit = limit.unwrap_or(messages.len());
        Ok(messages.iter().skip(start).take(limit).cloned().collect())
    }

    async fn len(&self, topic: &TopicId) -> Result<usize, BrokerError> {
        let guard = self.topics.lock().expect("store poisoned");
        Ok(guard.get(topic).map(|v| v.len()).unwrap_or(0))
    }
}

fn resolve_start_index(
    messages: &[StoredMessage],
    cursor: Cursor,
    topic: &TopicId,
) -> Result<usize, BrokerError> {
    match cursor {
        Cursor::Start => Ok(0),
        Cursor::End => Ok(messages.len()),
        Cursor::Offset(offset) => Ok(offset.min(messages.len() as u64) as usize),
        Cursor::Message(reference) => reference_index(messages, reference, topic),
    }
}

fn reference_index(
    messages: &[StoredMessage],
    reference: MessageRef,
    topic: &TopicId,
) -> Result<usize, BrokerError> {
    if &reference.topic != topic {
        return Err(BrokerError::CursorInvalid(
            "reference topic does not match snapshot topic".into(),
        ));
    }

    messages
        .iter()
        .position(|stored| stored.envelope.id == reference.id)
        .ok_or_else(|| BrokerError::CursorInvalid("message reference not found".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Envelope, TopicId};
    use bytes::Bytes;

    #[test]
    fn append_assigns_offsets() {
        futures::executor::block_on(async {
            let store = InMemoryQueueStore::new();
            let topic = TopicId::new("topic");

            let msg = Envelope::new(topic.clone(), Bytes::from_static(b"1"));
            let stored = store.append(msg).await.unwrap();
            assert_eq!(stored.offset, 0);

            let msg = Envelope::new(topic.clone(), Bytes::from_static(b"2"));
            let stored = store.append(msg).await.unwrap();
            assert_eq!(stored.offset, 1);
        });
    }

    #[test]
    fn snapshot_uses_cursor() {
        futures::executor::block_on(async {
            let store = InMemoryQueueStore::new();
            let topic = TopicId::new("topic");

            for i in 0..3 {
                let payload = format!("{i}").into_bytes();
                let envelope = Envelope::new(topic.clone(), Bytes::from(payload));
                let _ = store.append(envelope).await.unwrap();
            }

            let slice = store
                .snapshot(&topic, Cursor::Offset(1), Some(1))
                .await
                .unwrap();
            assert_eq!(slice.len(), 1);
            assert_eq!(slice[0].offset, 1);
        });
    }
}
