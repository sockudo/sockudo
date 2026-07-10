//! JSON and MessagePack wire codecs.

use crate::protocol::{AblyFormat, AblyProtocolMessage};
use bytes::Bytes;

pub(crate) fn encode_protocol_bytes(
    message: &AblyProtocolMessage,
    format: AblyFormat,
) -> Result<Bytes, String> {
    match format {
        AblyFormat::Json => sonic_rs::to_vec(message)
            .map(Bytes::from)
            .map_err(|error| error.to_string()),
        AblyFormat::MsgPack => rmp_serde::to_vec_named(message)
            .map(Bytes::from)
            .map_err(|error| error.to_string()),
    }
}
