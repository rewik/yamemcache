//! Protocol implementation
//!
//! reference: [`protocol.txt`](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

use crate::error::MemcacheError;
use crate::AsyncReadWriteUnpin;

use log::{error, debug};

pub type FrameData = RawValue;

/// Data that can be represented when storing or reading a value
pub struct RawValue {
    /// Raw data as stored in memcached.
    pub data: Vec<u8>,
    /// Flags associated with the key
    pub flags: u32,
    /// Time for the value to expire in seconds, None if it shouldn't expire (NOTE: memcached MAY remove the key ANYWAY if it reaches the memory limit)
    pub time: Option<u32>,
    /// Key used for Compare-And-Store operations. Not used yet.
    pub cas: Option<u32>,
}

impl std::convert::From<Vec<u8>> for RawValue {
    fn from(v: Vec<u8>) -> Self {
        Self {
            data: v,
            flags: 0,
            time: None,
            cas: None,
        }
    }
}
impl RawValue {
    pub fn from_vec(v: Vec<u8>) -> Self {
        Self {
            data: v,
            flags: 0,
            time: None,
            cas: None,
        }
    }

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
    ) -> Result<Option<RawValue>, MemcacheError> {
        debug!("get {}", key);
        // key cannot contain control characters or space
        if check_key_invalid(&key) {
            error!("get: invalid key");
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
        //debug!("RESPONSE HDR: {}", hex::encode(&response_hdr));
        if response_hdr.len() >= 2 {
            response_hdr.truncate(response_hdr.len()-2);
        }

        // header shoul be just ASCII
        let Ok(response_hdr_base) = String::from_utf8(response_hdr) else {
            error!("get: non-ASCII response");
            return Err(MemcacheError::BadServerResponse);
        };
        let mut response_hdr = response_hdr_base.split_ascii_whitespace();

        let Some(response_cmd) = response_hdr.next() else {
            error!("get: malformed response {}", response_hdr_base);
            return Err(MemcacheError::BadServerResponse);
        };
        if response_cmd == "EN" {
            debug!("get: no key");
            return Ok(None);
        } else if response_cmd != "VA" {
            error!("get: malformed response key {}", response_cmd);
            return Err(MemcacheError::BadServerResponse);
        }

        let Some(data_length) = response_hdr.next().map(|x| usize::from_str_radix(x,10).ok()).flatten() else {
            error!("get: bad data_length");
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
            error!("get: missing flags");
            return Err(MemcacheError::BadServerResponse);
        };

        if response_hdr.next().is_some() {
            error!("get: header too long");
            return Err(MemcacheError::BadServerResponse);
        };

        let mut response_data: Vec<u8> = Vec::with_capacity(data_length + 2);
        response_data.resize(data_length + 2, 0);
        let _ = io
            .read_exact(&mut response_data)
            .await
            .map_err(|x| MemcacheError::IOError(x))?;
        response_data.truncate(data_length);

        debug!("get: received data");
        Ok(Some(RawValue {
            data: response_data,
            flags: flags,
            time: None,
            cas: None,
        }))
    }


    /// GET multiple values from memcached
    /// returns Ok(Vec((key,RawValue))) with a list of key-value tuples
    ///
    /// If a key is not found in the response then it does not exist currently
    /// in memcached
    pub async fn get_many<T: AsyncReadWriteUnpin>(
        &self,
        io: &mut T,
        key_list: &[&str],
    ) -> Result<Vec<(String, RawValue)>, MemcacheError> {
        let mut keysize = 0;
        for k in key_list {
            if check_key_invalid(&k) {
                error!("get_multi: invalid key");
                return Err(MemcacheError::BadKey);
            }
            keysize += k.len();
        }
        //get key_1 key_2 key_3\r\n
        //
        //VALUE key_1 FLAG SIZE\r\n
        //DATA\r\n
        //VALUE key_3 FLAG SIZE\r\n
        //DATA\r\n
        //END\r\n
        let mut send = String::with_capacity(10 + key_list.len() + keysize); // 5 should be enough, but
        // let's not chance it
        send.push_str("get");
        for k in key_list {
            send.push(' ');
            send.push_str(k);
        }
        send.push_str("\r\n");
        io.write_all(&send.into_bytes())
            .await
            .and(io.flush().await)
            .map_err(|x| MemcacheError::IOError(x))?;

        let mut retval = Vec::new();
        let mut buffer = Vec::new();
        loop {
            buffer.clear();
            let _ = io
                .read_until(0xA, &mut buffer)
                .await
                .map_err(|x| MemcacheError::IOError(x))?;
            if buffer.len() >= 2 {
                buffer.truncate(buffer.len()-2);
            }
            if buffer == b"END" {
                return Ok(retval);
            }
            let Ok(response) = String::from_utf8(buffer.clone()) else {
                //error!("get_multi: non-ASCII response: {}", hex::encode(buffer));
                error!("get_multi: non-ASCII response");
                return Err(MemcacheError::BadServerResponse);
            };
            let mut response_hdr = response.split_ascii_whitespace();
            let Some(response_cmd) = response_hdr.next() else {
                error!("get_mutli: malformed response {}", response);
                return Err(MemcacheError::BadServerResponse);
            };
            if response_cmd != "VALUE" {
                error!("get_multi: server response error: {}", response_cmd);
                return Err(MemcacheError::BadServerResponse);
            }

            let Some(key) = response_hdr.next() else {
                error!("get_multi: missing key");
                return Err(MemcacheError::BadServerResponse);
            };

            let Some(flags) = response_hdr.next().map(|x| u32::from_str_radix(x,10).ok()).flatten() else {
                error!("get_multi: bad flags");
                return Err(MemcacheError::BadServerResponse);
            };

            let Some(data_length) = response_hdr.next().map(|x| usize::from_str_radix(x,10).ok()).flatten() else {
                error!("get_multi: bad data_length");
                return Err(MemcacheError::BadServerResponse);
            };

            if response_hdr.next().is_some() {
                error!("get_multi: header too long");
                return Err(MemcacheError::BadServerResponse);
            };

            buffer.resize(data_length + 2, 0);
            let _ = io
                .read_exact(&mut buffer)
                .await
                .map_err(|x| MemcacheError::IOError(x))?;
            buffer.truncate(data_length);

            retval.push((key.to_string(), RawValue {
                data: buffer.clone(),
                flags: flags,
                time: None,
                cas: None,
            }));
        }
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
        data: &RawValue,
    ) -> Result<(), MemcacheError> {
        debug!("set {}", key);
        // key cannot contain control characters or space
        if check_key_invalid(&key) {
            error!("set: invalid key");
            return Err(MemcacheError::BadKey);
        }
        let request = format!(
            "ms {} S{} T{} F{}\r\n",
            key,
            data.data.len(),
            data.time.unwrap_or(0),
            data.flags
        );
        let request = request.into_bytes();
        let marker = [0x0D, 0x0A];
        io.write_all(&request)
            .await
            .and(io.write_all(&data.data).await)
            .and(io.write_all(&marker).await)
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

        let Ok(response_hdr) = String::from_utf8(response_hdr) else {
            error!("set: bad header");
            return Err(MemcacheError::BadServerResponse);
        };
        let mut response_hdr = response_hdr.split_ascii_whitespace();

        let Some(response_cmd) = response_hdr.next() else {
            return Err(MemcacheError::BadServerResponse);
        };
        match response_cmd {
            "OK" => { debug!("set: OK"); Ok(()) },
            "HD" => { debug!("set: OK"); Ok(()) },
            "CLIENT_ERROR" => { debug!("set: client error"); Err(MemcacheError::BadQuery) },
            x => {
                error!("set: unexpected reponse {}", x);
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
        debug!("delete: {}", key);
        // key cannot contain control characters or space
        if check_key_invalid(&key) {
            error!("delete: invalid key");
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
            debug!("delete: OK");
            return Ok(Some(()));
        } else if response_hdr == b"NOT_FOUND" {
            debug!("delete: NOT FOUND");
            return Ok(None);
        }
        error!("delte: malformed reponse {}", String::from_utf8_lossy(&response_hdr));
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
