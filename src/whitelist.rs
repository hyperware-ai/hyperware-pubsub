//! Static whitelist-based access control shared across brokers.
//!
//! The whitelist is expected to be replicated via the group's CRDT so that every
//! hub sees the same membership decisions. Brokers can query the whitelist to
//! decide if a node may publish or subscribe to a topic.

use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};

use serde::{Deserialize, Serialize};

use crate::{Audience, TopicId};

/// Identity for a trusted node (hub or subscriber) as provided by Hyperdrive.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

/// Describes which topics a node may access.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopicPattern {
    Exact(String),
    Prefix(String),
}

impl TopicPattern {
    fn matches(&self, topic: &TopicId) -> bool {
        match self {
            TopicPattern::Exact(path) => topic.as_str() == path,
            TopicPattern::Prefix(prefix) => topic.as_str().starts_with(prefix),
        }
    }
}

/// Permissions granted to a node.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeAccess {
    pub publish: Vec<TopicPattern>,
    pub subscribe: Vec<TopicPattern>,
    pub audiences: HashSet<String>,
    pub features: HashSet<String>,
    pub expires_at: Option<SystemTime>,
}

impl NodeAccess {
    pub fn allow_all_publish() -> Vec<TopicPattern> {
        Vec::new()
    }

    pub fn allow_all_subscribe() -> Vec<TopicPattern> {
        Vec::new()
    }

    fn is_active(&self, now: SystemTime) -> bool {
        match self.expires_at {
            Some(exp) => now < exp,
            None => true,
        }
    }

    fn matches_publish(&self, topic: &TopicId) -> bool {
        matches_topic(&self.publish, topic)
    }

    fn matches_subscribe(&self, topic: &TopicId) -> bool {
        matches_topic(&self.subscribe, topic)
    }
}

fn matches_topic(patterns: &[TopicPattern], topic: &TopicId) -> bool {
    if patterns.is_empty() {
        return true;
    }
    patterns.iter().any(|pattern| pattern.matches(topic))
}

/// Snapshot of the whitelist for replication.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WhitelistSnapshot {
    pub version: u64,
    pub entries: HashMap<NodeId, NodeAccess>,
}

/// Access scope returned when a node is allowed to interact with a topic.
#[derive(Clone, Debug)]
pub struct AccessScope {
    pub audiences: Vec<String>,
    pub features: Vec<String>,
}

/// Whitelist that brokers consult before accepting publishes or subscriptions.
#[derive(Clone, Debug, Default)]
pub struct Whitelist {
    version: u64,
    entries: HashMap<NodeId, NodeAccess>,
}

impl Whitelist {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn entries(&self) -> &HashMap<NodeId, NodeAccess> {
        &self.entries
    }

    pub fn grant(&mut self, node: NodeId, access: NodeAccess) {
        self.entries.insert(node, access);
        self.version = self.version.wrapping_add(1);
    }

    pub fn revoke(&mut self, node: &NodeId) -> bool {
        let removed = self.entries.remove(node).is_some();
        if removed {
            self.version = self.version.wrapping_add(1);
        }
        removed
    }

    pub fn merge_snapshot(&mut self, snapshot: WhitelistSnapshot) {
        if snapshot.version > self.version {
            self.entries = snapshot.entries;
            self.version = snapshot.version;
        }
    }

    pub fn publish_scope(
        &self,
        node: &NodeId,
        topic: &TopicId,
        now: SystemTime,
    ) -> Option<AccessScope> {
        let access = self.entries.get(node)?;
        if !access.is_active(now) || !access.matches_publish(topic) {
            return None;
        }
        Some(access_scope(access))
    }

    pub fn subscribe_scope(
        &self,
        node: &NodeId,
        topic: &TopicId,
        now: SystemTime,
    ) -> Option<AccessScope> {
        let access = self.entries.get(node)?;
        if !access.is_active(now) || !access.matches_subscribe(topic) {
            return None;
        }
        Some(access_scope(access))
    }

    pub fn audience_for(
        &self,
        node: &NodeId,
        topic: &TopicId,
        now: SystemTime,
    ) -> Option<Audience> {
        let scope = self.publish_scope(node, topic, now)?;
        let label = scope.audiences.first()?.clone();
        let feature = scope.features.first().cloned();
        Some(Audience { label, feature })
    }
}

fn access_scope(access: &NodeAccess) -> AccessScope {
    AccessScope {
        audiences: access.audiences.iter().cloned().collect(),
        features: access.features.iter().cloned().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashSet, time::Duration};

    #[test]
    fn grants_publish_when_pattern_matches() {
        let mut whitelist = Whitelist::new();
        let node = NodeId::new("node-a");
        whitelist.grant(
            node.clone(),
            NodeAccess {
                publish: vec![TopicPattern::Prefix("chat.group".into())],
                subscribe: Vec::new(),
                audiences: HashSet::from(["hub".into()]),
                features: HashSet::new(),
                expires_at: None,
            },
        );

        let topic = TopicId::new("chat.group.alpha.hubs");
        assert!(
            whitelist
                .publish_scope(&node, &topic, SystemTime::now())
                .is_some()
        );
    }

    #[test]
    fn rejects_expired_entries() {
        let mut whitelist = Whitelist::new();
        let node = NodeId::new("node-b");
        let expiry = SystemTime::now() - Duration::from_secs(1);
        whitelist.grant(
            node.clone(),
            NodeAccess {
                publish: Vec::new(),
                subscribe: Vec::new(),
                audiences: HashSet::new(),
                features: HashSet::new(),
                expires_at: Some(expiry),
            },
        );
        let topic = TopicId::new("chat");
        assert!(
            whitelist
                .publish_scope(&node, &topic, SystemTime::now())
                .is_none()
        );
    }
}
