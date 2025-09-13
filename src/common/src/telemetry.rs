use anyhow::Result;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use std::env;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_telemetry(service_name: &'static str) {
    // Set W3C trace context propagator
    global::set_text_map_propagator(TraceContextPropagator::new());

    // Check if OTLP is enabled
    let otlp_enabled = env::var("OTEL_TRACES_EXPORTER")
        .map(|v| v == "otlp")
        .unwrap_or(false);

    let tracer = if otlp_enabled {
        // Use OTLP exporter for Jaeger/Tempo
        let otlp_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:4317".to_string());

        match create_otlp_tracer(&otlp_endpoint, service_name) {
            Ok(tracer) => tracer,
            Err(e) => {
                eprintln!(
                    "Failed to initialize OTLP tracer for {}: {}",
                    service_name, e
                );
                eprintln!("Falling back to stdout exporter");
                init_stdout_tracer(service_name)
            }
        }
    } else {
        eprintln!(
            "Using stdout tracer for {} (set OTEL_TRACES_EXPORTER=otlp for OTLP)",
            service_name
        );
        init_stdout_tracer(service_name)
    };

    let otel = OpenTelemetryLayer::new(tracer);
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env().add_directive("nanokv=info".parse().unwrap()))
        .with(tracing_subscriber::fmt::layer().compact())
        .with(otel)
        .init();
}

fn create_otlp_tracer(
    endpoint: &str,
    service_name: &'static str,
) -> Result<opentelemetry_sdk::trace::Tracer> {
    use opentelemetry_otlp::WithExportConfig;

    // Parse endpoint to determine if it's HTTP or gRPC
    let use_http = endpoint.contains("4318") || endpoint.contains("/v1/traces");

    if use_http {
        // Use HTTP exporter
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()?;

        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();

        Ok(provider.tracer(service_name))
    } else {
        // For gRPC (port 4317), fall back to stdout for now since the API is complex
        eprintln!(
            "gRPC OTLP not fully implemented, using stdout. Use HTTP endpoint (port 4318) for OTLP"
        );
        Err(anyhow::anyhow!("gRPC OTLP not implemented"))
    }
}

fn init_stdout_tracer(service_name: &'static str) -> opentelemetry_sdk::trace::Tracer {
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
        .build();

    provider.tracer(service_name)
}
