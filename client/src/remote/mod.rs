
mod channel;
pub(crate) mod cluster;
mod member;
pub(crate) mod message;

const PROTOCOL_SEQUENCE: [u8; 3] = [0x43, 0x50, 0x32];  // CP2

const CLIENT_TYPE: &str = "Rust";
const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");
const PROTOCOL_VERSION: u8 = 1;

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;
const LENGTH_FIELD_ADJUSTMENT: isize = -4;
