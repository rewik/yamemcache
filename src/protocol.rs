//! Protocol implementation
//!
//! reference: [`protocol.txt`](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

use crate::error::MemcacheError;
use crate::AsyncReadWriteUnpin;

use log::{error, info};

/// Data that can be represented when storing or reading a value
pub struct FrameData {
    /// Raw data as stored in memcached.
    pub data: Vec<u8>,
    /// Flags associated with the key
    pub flags: u32,
    /// Time for the value to expire in seconds, None if it shouldn't expire (NOTE: memcached MAY remove the key ANYWAY if it reaches the memory limit)
    pub time: Option<u32>,
    /// Key used for Compare-And-Store operations. Not used yet.
    pub cas: Option<u32>,
}

impl std::convert::From<Vec<u8>> for FrameData {
    fn from(v: Vec<u8>) -> Self {
        Self {
            data: v,
            flags: 0,
            time: None,
            cas: None,
        }
    }
}
impl FrameData {
    pub fn set_time(mut self, t: Option<u32>) -> Self {
        self.time = t;
        self
    }

    pub fn set_cas(mut self, c: Option<u32>) -> Self {
        self.cas = c;
        self
    }

    pub fn set_flags(mut self, f: u32) -> Self {
        self.flags = f;
        self
    }
}

/// Fake object representing the META protocol (TEXT protocol extended with additional commands)
pub struct Meta {}

/*
* flags set:
*  T = time remaing to expiration
*  F = flags (u32 as text)
*  S = size (usize as text)
*/

/*
* flags get:
*  t = time remaing to expiration (0) = infinite
*  f = flags (u32 as text)
*  s = data size
*  v = value
*  h = 0/1 if requested since stored
*  l = time since last access
*  k = key in response
*  c = CAS
*  T = UPDATE time remaining
*/

/// key cannot contain control characters or space
fn check_key_invalid(key: &str) -> bool {
    for b in key.bytes() {
        if b <= 32 || b >= 127 {
            return true;
        }
    }
    false
}

impl Meta {
    pub fn new() -> Self {
        Meta {}
    }

    /// GET a value from memcached
    /// returns Ok(Some(x)) when key is found
    /// returns Ok(None) if key was not found
    pub async fn get<T: AsyncReadWriteUnpin>(
        &self,
        io: &mut T,
        key: &str,
    ) -> Result<Option<FrameData>, MemcacheError> {
        // key cannot contain control characters or space
        if check_key_invalid(&key) {
            return Err(MemcacheError::BadKey);
        }
        let request = format!("mg {} f v\r\n", key).into_bytes();
        io.write_all(&request)
            .await
            .and(io.flush().await)
            .map_err(|x| MemcacheError::IOError(x))?;

        let mut response_hdr: Vec<u8> = Vec::new();
        let _ = io
            .read_until(0xA, &mut response_hdr)
            .await
            .map_err(|x| MemcacheError::IOError(x))?;
        //info!("RESPONSE HDR: {}", hex::encode(&response_hdr));
        if response_hdr.len() >= 2 {
            response_hdr.truncate(response_hdr.len()-2);
        }

        // header shoul be just ASCII
        let Ok(response_hdr_base) = String::from_utf8(response_hdr) else {
            return Err(MemcacheError::BadServerResponse);
        };
        let mut response_hdr = response_hdr_base.split_ascii_whitespace();

        let Some(response_cmd) = response_hdr.next() else {
            //error!("HEADER error (EMPTY): {}", response_hdr_base);
            return Err(MemcacheError::BadServerResponse);
        };
        if response_cmd == "EN" {
            return Ok(None);
        } else if response_cmd != "VA" {
            //error!("HEADER error (NOT VA): {}", response_cmd);
            return Err(MemcacheError::BadServerResponse);
        }

        let Some(data_length) = response_hdr.next().map(|x| usize::from_str_radix(x,10).ok()).flatten() else {
            //error!("HEADER error: bad data_length");
            return Err(MemcacheError::BadServerResponse);
        };

        let Some(flags) = response_hdr.next().map(
            |x| {
                if x.bytes().nth(0) == Some(b'f') {
                    return u32::from_str_radix(&x[1..],10).ok()
                } else {
                    return None
                }
            }).flatten() else {
            //error!("HEADER error: missing flags");
            return Err(MemcacheError::BadServerResponse);
        };

        if response_hdr.next().is_some() {
            //error!("HEADER error: header too long");
            return Err(MemcacheError::BadServerResponse);
        };

        let mut response_data: Vec<u8> = Vec::with_capacity(data_length + 2);
        response_data.resize(data_length + 2, 0);
        let _ = io
            .read_exact(&mut response_data)
            .await
            .map_err(|x| MemcacheError::IOError(x))?;
        //info!("RESPONSE RAW: {}", hex::encode(&response_data));
        response_data.truncate(data_length);
        //info!("RESPONSE: {}", String::from_utf8_lossy(&response_data));

        Ok(Some(FrameData {
            data: response_data,
            flags: flags,
            time: None,
            cas: None,
        }))
    }

    /// STORE function. Stores provided data using the provided key.
    /// data.time determines for how many seconds memcached should keep the data. Setting it to
    /// None will make memcached keep the data for as long as possible (data may still be dropped
    /// if memcached reaches its memory limit)
    /// WARNING: CAS is not yet supported.
    pub async fn set<T: AsyncReadWriteUnpin>(
        &self,
        io: &mut T,
        key: &str,
        data: &FrameData,
    ) -> Result<(), MemcacheError> {
        // key cannot contain control characters or space
        if check_key_invalid(&key) {
            error!("invalid key");
            return Err(MemcacheError::BadKey);
        }
        let request = format!(
            "ms {} S{} T{} F{}\r\n",
            key,
            data.data.len(),
            data.time.unwrap_or(0),
            data.flags
        );
        info!("REQUEST: {}", request);
        let request = request.into_bytes();
        let marker = [0x0D, 0x0A];
        io.write_all(&request)
            .await
            .and(io.write_all(&data.data).await)
            .and(io.write_all(&marker).await)
            .and(io.flush().await)
            .map_err(|x| MemcacheError::IOError(x))?;

        info!("wait for response");
        let mut response_hdr: Vec<u8> = Vec::new();
        let _ = io
            .read_until(0xA, &mut response_hdr)
            .await
            .map_err(|x| MemcacheError::IOError(x))?;
        //info!("RESPONSE HDR: {}", hex::encode(&response_hdr));
        if response_hdr.len() >= 2 {
            response_hdr.truncate(response_hdr.len()-2);
        }

        // header shoul be just ASCII
        let Ok(response_hdr) = String::from_utf8(response_hdr) else {
            error!("bad header");
            return Err(MemcacheError::BadServerResponse);
        };
        let mut response_hdr = response_hdr.split_ascii_whitespace();

        let Some(response_cmd) = response_hdr.next() else {
            return Err(MemcacheError::BadServerResponse);
        };
        match response_cmd {
            "OK" => Ok(()),
            "HD" => Ok(()),
            "CLIENT_ERROR" => Err(MemcacheError::BadQuery),
            x => {
                error!("STORE returned {}", x);
                Err(MemcacheError::BadServerResponse)
            }
        }
    }

    /// Removes a key from memcached
    pub async fn delete<T: AsyncReadWriteUnpin>(
        &self,
        io: &mut T,
        key: &str,
    ) -> Result<Option<()>, MemcacheError> {
        // key cannot contain control characters or space
        if check_key_invalid(&key) {
            error!("invalid key");
            return Err(MemcacheError::BadKey);
        }
        let request = format!("delete {}\r\n", key).into_bytes();
        io.write_all(&request)
            .await
            .and(io.flush().await)
            .map_err(|x| MemcacheError::IOError(x))?;

        let mut response_hdr: Vec<u8> = Vec::new();
        let _ = io
            .read_until(0xA, &mut response_hdr)
            .await
            .map_err(|x| MemcacheError::IOError(x))?;
        if response_hdr.len() >= 2 {
            response_hdr.truncate(response_hdr.len()-2);
        }

        if response_hdr == b"DELETED" {
            return Ok(Some(()));
        } else if response_hdr == b"NOT_FOUND" {
            return Ok(None);
        }
        info!("Improper reponse: {}", String::from_utf8_lossy(&response_hdr));
        Err(MemcacheError::BadServerResponse)
    }

    /// Checks memcached server version and returns it as a string.
    pub async fn version<T: AsyncReadWriteUnpin>(
        &self,
        io: &mut T,
    ) -> Result<String, MemcacheError> {
        let request = b"version\r\n";
        io.write_all(request)
            .await
            .and(io.flush().await)
            .map_err(|x| MemcacheError::IOError(x))?;

        let mut response_hdr: Vec<u8> = Vec::new();
        let _ = io
            .read_until(0xA, &mut response_hdr)
            .await
            .map_err(|x| MemcacheError::IOError(x))?;
        let Ok(response_hdr) = String::from_utf8(response_hdr) else {
            return Err(MemcacheError::BadServerResponse);
        };
        let response_hdr = response_hdr.trim();
        if response_hdr.len() > 8 && response_hdr.starts_with("VERSION ") {
            Ok(response_hdr[8..].to_string())
        } else {
            Err(MemcacheError::BadServerResponse)
        }
    }
}
