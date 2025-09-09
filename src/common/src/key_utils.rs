use crate::constants::META_KEY_PREFIX;
use crate::error::KeyError;
use percent_encoding::{self, NON_ALPHANUMERIC, percent_decode_str};

pub fn meta_key_for(user_key_enc: &str) -> String {
    // user_key_enc is the percent-encoded file name
    format!("{}:{}", META_KEY_PREFIX, user_key_enc)
}

pub fn get_key_enc_from_meta_key(meta_key: &str) -> String {
    meta_key
        .strip_prefix(&format!("{}:", META_KEY_PREFIX))
        .unwrap()
        .to_string()
}

const MAX_KEY_LEN: usize = 4096;

pub struct Key {
    decoded: Vec<u8>,
    enc: String, // canonical, lowercase
}

impl Key {
    pub fn from_percent_encoded(s: &str) -> Result<Self, KeyError> {
        // strict percent-decoding to bytes
        let decoded = percent_decode_str(s)
            .decode_utf8()
            .map(|cow| cow.into_owned().into_bytes())
            .or_else(|_| Ok::<Vec<u8>, KeyError>(percent_decode_str(s).collect::<Vec<u8>>()))
            .map_err(|_| KeyError::BadEncoding)?; // will not happen; kept for clarity

        if decoded.is_empty() || decoded.len() > MAX_KEY_LEN {
            return Err(KeyError::Length);
        }
        if decoded.iter().any(|&b| b == 0 || b < 0x20 || b == b'/') {
            return Err(KeyError::Forbidden);
        }

        let enc = percent_encoding::percent_encode(&decoded, NON_ALPHANUMERIC)
            .to_string()
            .to_lowercase(); // canonical

        Ok(Key { decoded, enc })
    }

    pub fn enc(&self) -> &str {
        &self.enc
    }
    pub fn bytes(&self) -> &[u8] {
        &self.decoded
    }
}
