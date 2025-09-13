use anyhow::anyhow;
use url::Url;
use std::net::SocketAddr;

pub fn sanitize_url(url: &str) -> anyhow::Result<String> {
    // Trim whitespace and check for empty input
    let url = url.trim();
    if url.is_empty() {
        return Err(anyhow!("URL cannot be empty"));
    }

    // Check for obviously malicious patterns
    if url.contains('\0') || url.contains('\r') || url.contains('\n') {
        return Err(anyhow!("URL contains invalid control characters"));
    }

    // Parse the URL using the url crate for proper validation
    let parsed_url = Url::parse(url).map_err(|e| anyhow!("Invalid URL format: {}", e))?;

    // Only allow http and https schemes
    match parsed_url.scheme() {
        "http" | "https" => {}
        other => return Err(anyhow!("Unsupported URL scheme: {}", other)),
    }

    let url_str = parsed_url.to_string();
    let trimmed = url_str.trim_end_matches('/');

    // Return the normalized URL string
    Ok(trimmed.to_string())
}

pub fn node_id_from_url(u: &str) -> String {
    u.trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}

pub fn parse_socket_addr(public_url: &str) -> anyhow::Result<SocketAddr> {
    // Try to parse with url crate if it looks like a full URL
    let url = if public_url.starts_with("http://") || public_url.starts_with("https://") {
        Url::parse(public_url)?
    } else {
        // Fallback: prepend scheme so url crate can handle it
        Url::parse(&format!("http://{}", public_url))?
    };

    // Extract host
    let host = url
        .host_str()
        .ok_or(anyhow!("missing host in public_url"))?
        .to_string();

    // Extract port (default to 80 if missing)
    let port = url.port().unwrap_or(80);

    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;
    Ok(addr)
}