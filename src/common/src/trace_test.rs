#[cfg(test)]
mod tests {
    use crate::trace_middleware::*;
    use opentelemetry::propagation::Extractor;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_trace_context_injection() {
        // Test header injection (may be empty without active trace context)
        let mut headers = HashMap::new();
        inject_trace_context(&mut headers);

        // The function should not panic, even if no headers are injected
        // Without an active OpenTelemetry tracer, headers may be empty
        println!("Injected headers: {:?}", headers);

        // This test mainly verifies the injection mechanism works without errors
        // In a real environment with active traces, we would see traceparent headers
    }

    #[test]
    fn test_header_extractor() {
        use axum::http::{HeaderMap, HeaderName, HeaderValue};

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_static("00-0af7651916cd43dd8448eb211c80319c-b9c7c989f97918e1-01"),
        );

        let extractor = HeaderExtractor::new(&headers);

        // Test extraction
        let traceparent = extractor.get("traceparent");
        assert_eq!(
            traceparent,
            Some("00-0af7651916cd43dd8448eb211c80319c-b9c7c989f97918e1-01")
        );

        // Test keys iteration
        let keys = extractor.keys();
        assert!(keys.contains(&"traceparent"));
    }

    #[tokio::test]
    async fn test_reqwest_injection() {
        // Create a mock reqwest client
        let client = reqwest::Client::new();
        let builder = client.get("http://example.com");

        // Test that injection doesn't panic
        let _builder_with_trace = inject_trace_context_reqwest(builder);

        // In a real test, we would verify the headers are added,
        // but reqwest::RequestBuilder doesn't expose headers for inspection
    }
}
