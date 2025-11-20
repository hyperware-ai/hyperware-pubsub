//! Authentication and access control primitives shared by broker implementations.

use std::time::SystemTime;

use async_trait::async_trait;

use crate::{BrokerError, PublishOptions, SubscribeRequest, TopicId};

/// Signed capability token presented by publishers/subscribers.
#[derive(Clone, Debug)]
pub struct CapabilityToken {
    pub issuer: String,
    pub subject: String,
    pub scopes: Vec<String>,
    pub issued_at: SystemTime,
    pub expires_at: SystemTime,
    pub signature: Vec<u8>,
}

impl CapabilityToken {
    pub fn is_expired(&self, now: SystemTime) -> bool {
        now >= self.expires_at
    }
}

/// Claims extracted from a verified token.
#[derive(Clone, Debug)]
pub struct CapabilityClaims {
    pub subject: String,
    pub roles: Vec<String>,
    pub features: Vec<String>,
    pub expires_at: SystemTime,
}

/// Audit log entry describing an auth decision.
#[derive(Clone, Debug)]
pub struct AuditRecord {
    pub action: AuditAction,
    pub subject: String,
    pub topic: TopicId,
    pub granted: bool,
    pub reason: Option<String>,
}

#[derive(Clone, Debug)]
pub enum AuditAction {
    Publish,
    Subscribe,
}

/// Sink for audit events.
pub trait AccessAudit: Send + Sync {
    fn record(&self, record: AuditRecord);
}

/// Issuer plugins verify capability tokens and produce claims.
#[async_trait]
pub trait CapabilityIssuer: Send + Sync {
    async fn verify(&self, token: &CapabilityToken) -> Result<CapabilityClaims, BrokerError>;
}

/// Result of running an ACL check.
#[derive(Clone, Debug)]
pub struct AccessDecision {
    pub granted: bool,
    pub applied_audience: Option<String>,
    pub applied_feature: Option<String>,
    pub reason: Option<String>,
}

impl AccessDecision {
    pub fn grant(audience: Option<String>, feature: Option<String>) -> Self {
        Self {
            granted: true,
            applied_audience: audience,
            applied_feature: feature,
            reason: None,
        }
    }

    pub fn deny(reason: impl Into<String>) -> Self {
        Self {
            granted: false,
            applied_audience: None,
            applied_feature: None,
            reason: Some(reason.into()),
        }
    }
}

/// AccessController implementations enforce publish/subscribe ACLs.
#[async_trait]
pub trait AccessController: Send + Sync {
    async fn authorize_publish(
        &self,
        token: Option<&CapabilityToken>,
        topic: &TopicId,
        options: &PublishOptions,
    ) -> Result<AccessDecision, BrokerError>;

    async fn authorize_subscribe(
        &self,
        token: Option<&CapabilityToken>,
        request: &SubscribeRequest,
    ) -> Result<AccessDecision, BrokerError>;
}
