use nix::sys::statvfs::statvfs;
use std::path::Path;

pub fn disk_usage(root: &Path) -> anyhow::Result<(Option<u64>, Option<u64>)> {
    let v = statvfs(root)?;
    let cap = v.blocks() as u64 * v.fragment_size() as u64;
    let free = v.blocks_available() as u64 * v.fragment_size() as u64;
    let used = cap.saturating_sub(free);
    Ok((Some(used), Some(cap)))
}
