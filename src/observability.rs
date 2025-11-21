//! Telemetry hooks for metrics and tracing.

use std::{sync::Arc, time::Duration};

use tracing::{Span, info_span, trace_span};

use crate::{MessageId, SubscriptionId, TopicId};

pub trait MetricsRecorder: Send + Sync {
    fn record_publish(&self, latency: Duration);
    fn record_queue_depth(&self, depth: usize);
    fn record_retry(&self);
    fn record_drop(&self);
}

pub type MetricsHandle = Arc<dyn MetricsRecorder>;

#[derive(Default)]
pub struct NoopMetrics;

impl MetricsRecorder for NoopMetrics {
    fn record_publish(&self, _latency: Duration) {}
    fn record_queue_depth(&self, _depth: usize) {}
    fn record_retry(&self) {}
    fn record_drop(&self) {}
}

pub fn publish_span(topic: &TopicId) -> Span {
    info_span!(target: "hyperware_pubsub::publish", "publish", topic = topic.as_str())
}

pub fn subscribe_span(topic: &TopicId) -> Span {
    info_span!(target: "hyperware_pubsub::subscribe", "subscribe", topic = topic.as_str())
}

pub fn trace_retry(subscription: SubscriptionId, attempts: u32, delay: Duration) {
    let span = trace_span!(
        target: "hyperware_pubsub::reliability",
        "retry",
        subscriber = %subscription,
        attempts,
        delay_millis = delay.as_millis()
    );
    span.in_scope(|| {});
}

pub fn trace_drop(message_id: MessageId, reason: &str) {
    let span = trace_span!(
        target: "hyperware_pubsub::drops",
        "drop",
        message = %message_id,
        reason
    );
    span.in_scope(|| {});
}

#[cfg(feature = "prometheus-exporter")]
mod prometheus_impl {
    use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry};

    use super::{Duration, MetricsRecorder};

    pub struct PrometheusMetrics {
        registry: Registry,
        publish_latency: Histogram,
        queue_depth: IntGauge,
        retry_total: IntCounter,
        drop_total: IntCounter,
    }

    impl PrometheusMetrics {
        pub fn new() -> Self {
            let registry = Registry::new();

            let publish_latency = Histogram::with_opts(HistogramOpts::new(
                "hyperware_pubsub_publish_latency_seconds",
                "Latency of publish calls",
            ))
            .expect("publish latency histogram");

            let queue_depth = IntGauge::with_opts(Opts::new(
                "hyperware_pubsub_queue_depth",
                "Number of deliveries pending or inflight",
            ))
            .expect("queue gauge");

            let retry_total = IntCounter::with_opts(Opts::new(
                "hyperware_pubsub_retry_total",
                "Number of retries scheduled",
            ))
            .expect("retry counter");

            let drop_total = IntCounter::with_opts(Opts::new(
                "hyperware_pubsub_drop_total",
                "Number of envelopes dropped due to dedupe or TTL",
            ))
            .expect("drop counter");

            registry
                .register(Box::new(publish_latency.clone()))
                .expect("publish latency register");
            registry
                .register(Box::new(queue_depth.clone()))
                .expect("queue depth register");
            registry
                .register(Box::new(retry_total.clone()))
                .expect("retry register");
            registry
                .register(Box::new(drop_total.clone()))
                .expect("drop register");

            Self {
                registry,
                publish_latency,
                queue_depth,
                retry_total,
                drop_total,
            }
        }

        pub fn registry(&self) -> &Registry {
            &self.registry
        }
    }

    impl MetricsRecorder for PrometheusMetrics {
        fn record_publish(&self, latency: Duration) {
            self.publish_latency.observe(latency.as_secs_f64());
        }

        fn record_queue_depth(&self, depth: usize) {
            self.queue_depth.set(depth as i64);
        }

        fn record_retry(&self) {
            self.retry_total.inc();
        }

        fn record_drop(&self) {
            self.drop_total.inc();
        }
    }

    pub use PrometheusMetrics as Exporter;
}

#[cfg(feature = "prometheus-exporter")]
pub use prometheus_impl::Exporter as PrometheusMetrics;
