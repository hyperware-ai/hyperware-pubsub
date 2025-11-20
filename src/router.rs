//! Routing policies that determine which subscribers receive a publication.

use std::fmt;

use async_trait::async_trait;

use crate::{Audience, BrokerError, FanoutScope, PublishOptions, SubscriptionId};

/// Metadata captured for a single subscription used while computing routes.
#[derive(Clone, Debug)]
pub struct SubscriberInfo {
    pub id: SubscriptionId,
    pub audience: Option<String>,
    pub feature: Option<String>,
}

impl SubscriberInfo {
    pub fn new(id: SubscriptionId) -> Self {
        Self {
            id,
            audience: None,
            feature: None,
        }
    }
}

/// Context supplied to routers during publication.
pub struct PublicationContext<'a> {
    pub subscribers: &'a [SubscriberInfo],
    pub options: &'a PublishOptions,
}

impl<'a> PublicationContext<'a> {
    pub fn new(subscribers: &'a [SubscriberInfo], options: &'a PublishOptions) -> Self {
        Self {
            subscribers,
            options,
        }
    }
}

/// Result describing which destinations should receive the message.
#[derive(Clone, Default)]
pub struct RoutePlan {
    targets: Vec<RouteTarget>,
}

impl RoutePlan {
    pub fn new(targets: Vec<RouteTarget>) -> Self {
        Self { targets }
    }

    pub fn into_targets(self) -> Vec<RouteTarget> {
        self.targets
    }

    pub fn targets(&self) -> &[RouteTarget] {
        &self.targets
    }
}

impl fmt::Debug for RoutePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.targets.fmt(f)
    }
}

/// Supported routing destinations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteTarget {
    Subscription(SubscriptionId),
}

#[async_trait]
pub trait Router: Send + Sync {
    async fn route(&self, ctx: PublicationContext<'_>) -> Result<RoutePlan, BrokerError>;
}

/// Simple router that fans out according to the requested fanout scope.
pub struct DirectRouter;

#[async_trait]
impl Router for DirectRouter {
    async fn route(&self, ctx: PublicationContext<'_>) -> Result<RoutePlan, BrokerError> {
        let targets = select_targets(ctx);
        Ok(RoutePlan::new(
            targets.into_iter().map(RouteTarget::Subscription).collect(),
        ))
    }
}

/// Router that understands hub-vs-subscriber audiences.
pub struct HubSpokeRouter {
    hub_label: String,
    subscriber_label: String,
}

impl HubSpokeRouter {
    pub fn new(hub_label: impl Into<String>, subscriber_label: impl Into<String>) -> Self {
        Self {
            hub_label: hub_label.into(),
            subscriber_label: subscriber_label.into(),
        }
    }
}

#[async_trait]
impl Router for HubSpokeRouter {
    async fn route(&self, ctx: PublicationContext<'_>) -> Result<RoutePlan, BrokerError> {
        let targets = match &ctx.options.fanout {
            FanoutScope::Broadcast => ctx
                .subscribers
                .iter()
                .map(|s| s.id)
                .collect::<Vec<SubscriptionId>>(),
            FanoutScope::Audience(audience) => {
                let label = &audience.label;
                if label == &self.hub_label {
                    select_by_label(ctx.subscribers, label, audience.feature.as_deref())
                } else if label == &self.subscriber_label {
                    select_by_label(ctx.subscribers, label, audience.feature.as_deref())
                } else {
                    select_by_audience(ctx.subscribers, audience)
                }
            }
        };

        Ok(RoutePlan::new(
            targets.into_iter().map(RouteTarget::Subscription).collect(),
        ))
    }
}

fn select_targets(ctx: PublicationContext<'_>) -> Vec<SubscriptionId> {
    match &ctx.options.fanout {
        FanoutScope::Broadcast => ctx.subscribers.iter().map(|s| s.id).collect(),
        FanoutScope::Audience(audience) => select_by_audience(ctx.subscribers, audience),
    }
}

fn select_by_audience(subscribers: &[SubscriberInfo], audience: &Audience) -> Vec<SubscriptionId> {
    select_by_label(subscribers, &audience.label, audience.feature.as_deref())
}

fn select_by_label(
    subscribers: &[SubscriberInfo],
    label: &str,
    feature: Option<&str>,
) -> Vec<SubscriptionId> {
    subscribers
        .iter()
        .filter(|sub| {
            matches_audience(sub.audience.as_deref(), label)
                && feature_matches(feature, sub.feature.as_deref())
        })
        .map(|s| s.id)
        .collect()
}

fn matches_audience(sub_label: Option<&str>, label: &str) -> bool {
    sub_label
        .map(|candidate| candidate == label)
        .unwrap_or(false)
}

fn feature_matches(target: Option<&str>, sub: Option<&str>) -> bool {
    match target {
        Some(req) => sub.map(|have| have == req).unwrap_or(false),
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Audience, FanoutScope, PublishOptions};
    use uuid::Uuid;

    fn make_sub(id: u128, audience: Option<&str>) -> SubscriberInfo {
        SubscriberInfo {
            id: SubscriptionId::from_uuid(Uuid::from_u128(id + 1)),
            audience: audience.map(|s| s.to_owned()),
            feature: None,
        }
    }

    #[test]
    fn direct_router_broadcasts() {
        let subscribers = vec![make_sub(1, Some("hub")), make_sub(2, Some("sub"))];
        let options = PublishOptions {
            fanout: FanoutScope::Broadcast,
            ..PublishOptions::default()
        };
        let ctx = PublicationContext::new(&subscribers, &options);
        let router = DirectRouter;

        let plan = futures::executor::block_on(router.route(ctx)).unwrap();
        assert_eq!(plan.targets().len(), 2);
    }

    #[test]
    fn direct_router_filters_audience() {
        let subscribers = vec![make_sub(1, Some("hub")), make_sub(2, Some("sub"))];
        let options = PublishOptions {
            fanout: FanoutScope::Audience(Audience {
                label: "hub".into(),
                feature: None,
            }),
            ..PublishOptions::default()
        };
        let ctx = PublicationContext::new(&subscribers, &options);
        let router = DirectRouter;

        let plan = futures::executor::block_on(router.route(ctx)).unwrap();
        assert_eq!(plan.targets().len(), 1);
    }

    #[test]
    fn hub_spoke_router_distinguishes_labels() {
        let subscribers = vec![make_sub(1, Some("hub")), make_sub(2, Some("subscriber"))];
        let options = PublishOptions {
            fanout: FanoutScope::Audience(Audience {
                label: "hub".into(),
                feature: None,
            }),
            ..PublishOptions::default()
        };

        let ctx = PublicationContext::new(&subscribers, &options);
        let router = HubSpokeRouter::new("hub", "subscriber");

        let plan = futures::executor::block_on(router.route(ctx)).unwrap();
        assert_eq!(plan.targets().len(), 1);
    }
}
