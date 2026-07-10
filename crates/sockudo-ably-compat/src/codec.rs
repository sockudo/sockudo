//! JSON and MessagePack wire codecs.

use crate::protocol::{AblyFormat, AblyProtocolMessage};
use sockudo_ws::Message;

pub(crate) fn encode_protocol_message(
    message: &AblyProtocolMessage,
    format: AblyFormat,
) -> Result<Message, String> {
    match format {
        AblyFormat::Json => sonic_rs::to_string(message)
            .map(Message::text)
            .map_err(|error| error.to_string()),
        AblyFormat::MsgPack => rmp_serde::to_vec_named(message)
            .map(Message::binary)
            .map_err(|error| error.to_string()),
    }
}
