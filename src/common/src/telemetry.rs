use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use tracing_opentelemetry::OpenTelemetryLayer;
use std::env;

pub fn init_telemetry(service_name: &'static str) {
    // Set W3C trace context propagator
    global::set_text_map_propagator(TraceContextPropagator::new());

    // For now, use stdout exporter for development
    // OTLP support can be added later when API is more stable
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
        .build();
    
    let tracer = provider.tracer(service_name);

    let otel = OpenTelemetryLayer::new(tracer);
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env().add_directive("nanokv=info".parse().unwrap()))
        .with(tracing_subscriber::fmt::layer().compact())
        .with(otel)
        .init();
        
    // Log configuration
    let otlp_enabled = env::var("OTEL_TRACES_EXPORTER")
        .map(|v| v == "otlp")
        .unwrap_or(false);
    
    if otlp_enabled {
        eprintln!("Note: OTLP export requested but using stdout for now. OTLP support coming soon.");
    }
    
    eprintln!("Telemetry initialized for service: {}", service_name);
}
