//! Delivery reliability utilities: attempt queues, backoff policies, idempotence caches, and recovery tracking.

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    time::{Duration, Instant},
};

use uuid::Uuid;

use crate::{
    Cursor, MessageId, SnapshotRequest, SubscriptionId, TopicId,
    observability::{self, MetricsHandle},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DeliveryKey(Uuid);

impl DeliveryKey {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    pub fn from_str(value: &str) -> Option<Self> {
        Uuid::parse_str(value).ok().map(DeliveryKey)
    }
}

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub initial_delay: Duration,
    pub multiplier: f64,
    pub max_delay: Duration,
}

impl RetryPolicy {
    pub fn delay_for(&self, attempts: u32) -> Duration {
        if attempts == 0 {
            return self.initial_delay;
        }
        let exponent = attempts.saturating_sub(1) as i32;
        let base = self.initial_delay.as_secs_f64();
        let delay = base * self.multiplier.powi(exponent);
        Duration::from_secs_f64(delay).min(self.max_delay)
    }
}

#[derive(Clone, Debug)]
pub enum EnqueueResult {
    Scheduled(DeliveryKey),
    Duplicate,
}

#[derive(Clone, Debug)]
pub struct DeliveryAttempt {
    pub key: DeliveryKey,
    pub subscription: SubscriptionId,
    pub message_id: MessageId,
    pub attempts: u32,
    pub ack_deadline: Instant,
}

#[derive(Clone, Debug)]
struct QueueEntry {
    key: DeliveryKey,
    subscription: SubscriptionId,
    message_id: MessageId,
    attempts: u32,
    next_attempt: Instant,
}

#[derive(Clone, Debug)]
struct ScheduledEntry {
    entry: QueueEntry,
}

impl PartialEq for ScheduledEntry {
    fn eq(&self, other: &Self) -> bool {
        self.entry.next_attempt.eq(&other.entry.next_attempt)
    }
}

impl Eq for ScheduledEntry {}

impl Ord for ScheduledEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.entry.next_attempt.cmp(&self.entry.next_attempt)
    }
}

impl PartialOrd for ScheduledEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct InFlight {
    entry: QueueEntry,
    ack_deadline: Instant,
}

pub struct AttemptQueue {
    policy: RetryPolicy,
    ack_timeout: Duration,
    pending: BinaryHeap<ScheduledEntry>,
    inflight: HashMap<DeliveryKey, InFlight>,
    idempotence: IdempotenceCache,
    metrics: Option<MetricsHandle>,
}

impl AttemptQueue {
    pub fn new(
        policy: RetryPolicy,
        ack_timeout: Duration,
        cache_ttl: Duration,
        metrics: Option<MetricsHandle>,
    ) -> Self {
        Self {
            policy,
            ack_timeout,
            pending: BinaryHeap::new(),
            inflight: HashMap::new(),
            idempotence: IdempotenceCache::new(cache_ttl),
            metrics,
        }
    }

    fn update_depth(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.record_queue_depth(self.depth());
        }
    }

    fn depth(&self) -> usize {
        self.pending.len() + self.inflight.len()
    }

    pub fn enqueue(
        &mut self,
        subscription: SubscriptionId,
        message_id: MessageId,
        idempotence_key: Option<&str>,
        now: Instant,
    ) -> EnqueueResult {
        if let Some(key) = idempotence_key {
            if self.idempotence.is_duplicate(key, now) {
                if let Some(metrics) = &self.metrics {
                    metrics.record_drop();
                }
                observability::trace_drop(message_id, "duplicate");
                return EnqueueResult::Duplicate;
            }
        }

        let key = DeliveryKey::new();
        let entry = QueueEntry {
            key,
            subscription,
            message_id,
            attempts: 0,
            next_attempt: now,
        };
        self.pending.push(ScheduledEntry { entry });
        self.update_depth();
        EnqueueResult::Scheduled(key)
    }

    pub fn pop_ready(&mut self, now: Instant) -> Option<DeliveryAttempt> {
        if let Some(top) = self.pending.peek() {
            if top.entry.next_attempt > now {
                return None;
            }
        } else {
            return None;
        }

        let mut scheduled = self.pending.pop().unwrap().entry;
        scheduled.attempts += 1;
        let ack_deadline = now + self.ack_timeout;
        let key = scheduled.key;
        self.inflight.insert(
            key,
            InFlight {
                entry: scheduled.clone(),
                ack_deadline,
            },
        );
        self.update_depth();

        Some(DeliveryAttempt {
            key,
            subscription: scheduled.subscription,
            message_id: scheduled.message_id,
            attempts: scheduled.attempts,
            ack_deadline,
        })
    }

    pub fn ack(&mut self, key: &DeliveryKey) -> bool {
        let removed = self.inflight.remove(key).is_some();
        if removed {
            self.update_depth();
        }
        removed
    }

    pub fn nack(&mut self, key: &DeliveryKey, now: Instant) -> bool {
        if let Some(inflight) = self.inflight.remove(key) {
            self.schedule_retry(inflight.entry, now);
            true
        } else {
            false
        }
    }

    pub fn sweep_timeouts(&mut self, now: Instant) -> Vec<DeliveryKey> {
        let timed_out: Vec<DeliveryKey> = self
            .inflight
            .iter()
            .filter(|(_, entry)| entry.ack_deadline <= now)
            .map(|(key, _)| *key)
            .collect();

        for key in &timed_out {
            if let Some(inflight) = self.inflight.remove(key) {
                self.schedule_retry(inflight.entry, now);
            }
        }

        timed_out
    }

    pub fn inflight(&self) -> usize {
        self.inflight.len()
    }

    fn schedule_retry(&mut self, mut entry: QueueEntry, now: Instant) {
        let delay = self.policy.delay_for(entry.attempts);
        entry.next_attempt = now + delay;
        self.pending.push(ScheduledEntry {
            entry: entry.clone(),
        });
        self.update_depth();
        if let Some(metrics) = &self.metrics {
            metrics.record_retry();
        }
        observability::trace_retry(entry.subscription, entry.attempts, delay);
    }
}

pub struct IdempotenceCache {
    ttl: Duration,
    entries: HashMap<String, Instant>,
}

impl IdempotenceCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: HashMap::new(),
        }
    }

    pub fn is_duplicate(&mut self, key: &str, now: Instant) -> bool {
        self.sweep(now);
        if self.entries.contains_key(key) {
            true
        } else {
            self.entries.insert(key.to_owned(), now + self.ttl);
            false
        }
    }

    fn sweep(&mut self, now: Instant) {
        self.entries.retain(|_, expires_at| *expires_at > now);
    }
}

#[derive(Default)]
pub struct RecoveryTracker {
    latest: HashMap<SubscriptionId, Cursor>,
}

impl RecoveryTracker {
    pub fn update(&mut self, subscription: SubscriptionId, cursor: Cursor) {
        self.latest.insert(subscription, cursor);
    }

    pub fn resume_cursor(&self, subscription: SubscriptionId) -> Cursor {
        self.latest
            .get(&subscription)
            .cloned()
            .unwrap_or(Cursor::End)
    }

    pub fn snapshot_request(
        &self,
        topic: TopicId,
        subscription: SubscriptionId,
        limit: Option<usize>,
    ) -> SnapshotRequest {
        SnapshotRequest {
            topic,
            cursor: self.resume_cursor(subscription),
            limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn retries_after_nack() {
        let mut queue = AttemptQueue::new(
            RetryPolicy {
                initial_delay: Duration::from_millis(10),
                multiplier: 2.0,
                max_delay: Duration::from_millis(100),
            },
            Duration::from_millis(50),
            Duration::from_secs(5),
            None,
        );
        let subscription = SubscriptionId::new();
        let message = MessageId::new();
        let now = Instant::now();
        assert!(matches!(
            queue.enqueue(subscription, message, None, now),
            EnqueueResult::Scheduled(_)
        ));

        let attempt = queue.pop_ready(now).expect("ready");
        assert_eq!(attempt.attempts, 1);
        queue.nack(&attempt.key, now);
        let later = now + Duration::from_millis(5);
        assert!(queue.pop_ready(later).is_none());
        let later = now + Duration::from_millis(11);
        assert!(queue.pop_ready(later).is_some());
    }

    #[test]
    fn dedupe_prevents_duplicates() {
        let mut cache = IdempotenceCache::new(Duration::from_secs(1));
        let now = Instant::now();
        assert!(!cache.is_duplicate("abc", now));
        assert!(cache.is_duplicate("abc", now));
    }

    #[test]
    fn recovery_tracker_defaults_to_end() {
        let tracker = RecoveryTracker::default();
        let cursor = tracker.resume_cursor(SubscriptionId::new());
        matches!(cursor, Cursor::End);
    }
}
