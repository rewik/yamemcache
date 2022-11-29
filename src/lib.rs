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
//!     .map(|x| tokio::io::BufStream::new(x)) else {
//!     println!("Unable to connect");
//!     return;
//!   }
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

pub mod error;
pub mod protocol;

use error::MemcacheError;
use protocol::FrameData;

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
pub struct Client<T: AsyncReadWriteUnpin> {
    protocol: protocol::Meta,
    connection: T,
}

impl<T: AsyncReadWriteUnpin> Client<T> {
    /// Create a new Client instance
    pub fn new(connection: T) -> Self {
        Client {
            protocol: protocol::Meta::new(),
            connection: connection,
        }
    }

    /// GET a value from memcached based on the provided key.
    pub async fn get(&mut self, key: &str) -> Result<Option<FrameData>, MemcacheError> {
        self.protocol.get(&mut self.connection, key).await
    }

    /// STORE a value in memcached using the provided key.
    pub async fn set(&mut self, key: &str, data: &FrameData) -> Result<(), MemcacheError> {
        self.protocol.set(&mut self.connection, key, data).await
    }

    /// Read memcached version.
    pub async fn version(&mut self) -> Result<String, MemcacheError> {
        self.protocol.version(&mut self.connection).await
    }
}
