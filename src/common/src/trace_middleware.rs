use axum::{extract::Request, http::HeaderMap, middleware::Next, response::Response};
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use std::collections::HashMap;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Axum middleware that extracts OpenTelemetry trace context from incoming HTTP headers
/// and attaches it to the current tracing span.
pub async fn trace_context_middleware(
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    // Extract trace context from HTTP headers
    let parent_context = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor::new(&headers))
    });

    // Attach the parent context to the current span
    Span::current().set_parent(parent_context);

    next.run(request).await
}

/// Helper struct to extract trace context from HTTP headers
pub struct HeaderExtractor<'a> {
    headers: &'a HeaderMap,
}

impl<'a> HeaderExtractor<'a> {
    pub fn new(headers: &'a HeaderMap) -> Self {
        Self { headers }
    }
}

impl<'a> Extractor for HeaderExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.headers.get(key)?.to_str().ok()
    }

    fn keys(&self) -> Vec<&str> {
        self.headers.keys().map(|k| k.as_str()).collect()
    }
}

/// Helper function to inject trace context into HTTP headers for outgoing requests
pub fn inject_trace_context(headers: &mut HashMap<String, String>) {
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(
            &tracing_opentelemetry::OpenTelemetrySpanExt::context(&Span::current()),
            &mut HeaderInjector::new(headers),
        )
    });
}

/// Helper struct to inject trace context into HTTP headers
struct HeaderInjector<'a> {
    headers: &'a mut HashMap<String, String>,
}

impl<'a> HeaderInjector<'a> {
    fn new(headers: &'a mut HashMap<String, String>) -> Self {
        Self { headers }
    }
}

impl<'a> opentelemetry::propagation::Injector for HeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.headers.insert(key.to_string(), value);
    }
}

/// Helper function to inject trace context into reqwest::RequestBuilder
pub fn inject_trace_context_reqwest(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let mut headers = HashMap::new();
    inject_trace_context(&mut headers);

    let mut builder = builder;
    for (key, value) in headers {
        builder = builder.header(key, value);
    }
    builder
}
