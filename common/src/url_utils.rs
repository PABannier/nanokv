use anyhow::anyhow;
use url::Url;

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
    let parsed_url = Url::parse(url)
        .map_err(|e| anyhow!("Invalid URL format: {}", e))?;

    // Only allow http and https schemes
    match parsed_url.scheme() {
        "http" | "https" => {},
        other => return Err(anyhow!("Unsupported URL scheme: {}", other)),
    }

    // Return the normalized URL string
    Ok(parsed_url.to_string())
}
