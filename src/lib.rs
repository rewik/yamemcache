//! # Yet another memcached client
//!
//! This is yet another memcached client since none of the existing ones at the same time:
//! * use a new version of tokio (1.x)
//! * allow access to flags when accessing the data
//!
//! # Example usage
//! ```
//! #[tokio::main]
//! async fn main() {
//!   let Ok(stream) = tokio::net::TcpStream::connect("127.0.0.1:11211").await
//!     .map(tokio::io::BufStream::new) else {
//!     println!("Unable to connect");
//!     return;
//!   };
//!
//!   let mut client = yamemcache::Client::new(stream);
//!
//!   if let Ok(x) = client.get("hello").await {
//!     match x {
//!       Some(x) => println!("Received data: {}", String::from_utf8_lossy(&x.data)),
//!       None => println!("No data received"),
//!     }
//!   }
//! }
//! ```

pub mod error;
pub mod protocol;

use error::MemcacheError;
use protocol::RawValue;

/// Helper trait that combines all the required traits for the io
pub trait AsyncReadWriteUnpin:
    tokio::io::AsyncWrite + tokio::io::AsyncBufRead + std::marker::Unpin
{
}
impl<T: tokio::io::AsyncWrite + tokio::io::AsyncBufRead + std::marker::Unpin> AsyncReadWriteUnpin
    for T
{
}

/// Memcached client abstraction
#[derive(Debug)]
pub struct Client<T: AsyncReadWriteUnpin> {
    protocol: protocol::Meta,
    connection: T,
}

impl<T: AsyncReadWriteUnpin> Client<T> {
    /// Create a new Client instance
    pub fn new(connection: T) -> Self {
        Client {
            protocol: protocol::Meta::new(),
            connection,
        }
    }

    /// GET a value from memcached based on the provided key.
    pub async fn get(&mut self, key: &str) -> Result<Option<RawValue>, MemcacheError> {
        self.protocol.get(&mut self.connection, key).await
    }

    /// GET any number of values from memcached.
    /// The result is a vector of (key, value) tuples. If a key is not present in the vector then
    /// it was not found.
    pub async fn get_many(
        &mut self,
        key_list: &[&str],
    ) -> Result<Vec<(String, RawValue)>, MemcacheError> {
        self.protocol.get_many(&mut self.connection, key_list).await
    }

    /// STORE a value in memcached using the provided key.
    pub async fn set(&mut self, key: &str, data: &RawValue) -> Result<(), MemcacheError> {
        self.protocol.set(&mut self.connection, key, data).await
    }

    /// DELETE a value from memcached attached to the provided key
    pub async fn delete(&mut self, key: &str) -> Result<Option<()>, MemcacheError> {
        self.protocol.delete(&mut self.connection, key).await
    }

    /// Read memcached version.
    pub async fn version(&mut self) -> Result<String, MemcacheError> {
        self.protocol.version(&mut self.connection).await
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn basic_commands() {
        // connect
        let Ok(stream) = tokio::net::TcpStream::connect("127.0.0.1:11211").await
            .map(tokio::io::BufStream::new) else {
                assert!(false, "Unable to connect to memcached");
                return;
        };

        // check ::version()
        let mut client = Client::new(stream);
        assert!(client.version().await.is_ok(), "Client.version() failed");

        // prepare test data
        let value1 = RawValue::from_vec(vec![0, 1, 2, 3]).set_flags(33);
        let key1 = "testkey1";

        let value2 = RawValue::from_vec(vec![4, 5, 6, 7]).set_flags(42);
        let key2 = "testkey2";

        // check ::set()
        assert!(
            client.set(key1, &value1).await.is_ok(),
            "Client.set() failed"
        );
        assert!(
            client.set(key2, &value2).await.is_ok(),
            "Client.set() failed"
        );

        // check ::get_many()
        let Ok(retval) = client.get_many(&[key1, key2]).await else {
            assert!(false, "Client.get_many() failed");
            return;
        };
        // found acts as a bitmask
        let mut found: u32 = 0;
        for (key, val) in retval {
            if key == key1 {
                if val.data == value1.data && val.flags == value1.flags {
                    found |= 1;
                }
            } else if key == key2 {
                if val.data == value2.data && val.flags == value2.flags {
                    found |= 2;
                }
            } else {
                assert!(
                    false,
                    "{}",
                    format!("Client.get_many() returned a bad key: {}", key)
                );
            }
        }
        assert_eq!(found, 3, "Client.get_many() returned a different value.");

        // check ::get()
        let Ok(Some(retval)) = client.get(key1).await else {
            assert!(false, "Client::get() failed");
            return;
        };
        assert_eq!(
            retval.data, value1.data,
            "Client.get() returned a different value."
        );
        assert_eq!(
            retval.flags, value1.flags,
            "Client.get() returned a different value."
        );

        // check ::delete()
        assert!(client.delete(key1).await.is_ok(), "Client.delete() failed");
        let Ok(retval) = client.get(key1).await else {
            assert!(false, "Client.get() after .delete() failed");
            return;
        };
        assert!(
            retval.is_none(),
            "Client.get() returned a value after it was deleted"
        );

        // clean up the other test value
        assert!(client.delete(key2).await.is_ok(), "Client.delete() failed");
    }
}
