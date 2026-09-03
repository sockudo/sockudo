//! Transport adapters around [`rmcp`]: Streamable HTTP (`http` feature) and
//! stdio (`stdio` feature).

#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "stdio")]
pub mod stdio;
