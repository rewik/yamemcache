//! Error return value

/// Main error type returned by yamemcache
pub enum MemcacheError {
    /// Wrapper around the error returned by the underlying io functions
    IOError(std::io::Error),
    /// Key provided did not pass validation
    BadKey,
    /// Server responded in an unexptected way
    BadServerResponse,
    /// Server claims the query is invalid
    BadQuery,
}
