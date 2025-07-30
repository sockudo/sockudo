pub const PROTOCOL_VERSION: u8 = 7;
pub const ACTIVITY_TIMEOUT: u64 = 120;
pub const PONG_TIMEOUT: u64 = 30;

pub const CHANNEL_NAME_MAX_LENGTH: usize = 200;
pub const CHANNEL_NAME_REGEX: &str = r"^[a-zA-Z0-9_\-=@,.;]+$";

pub const EVENT_NAME_MAX_LENGTH: usize = 200;
pub const CLIENT_EVENT_PREFIX: &str = "client-";
pub const DEFAULT_EVENT_NAME_MAX_LENGTH: i32 = 200;
pub const DEFAULT_CHANNEL_NAME_MAX_LENGTH: i32 = 200;
