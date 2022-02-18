use std::net::SocketAddr;

use derive_more::Display;
use uuid::Uuid;

use crate::codec::{Reader, Writer};
use crate::remote::message::{Message, Frame};

pub(crate) mod authentication;
pub(crate) mod error;
pub(crate) mod ping;
pub(crate) mod pn_counter;

pub(crate) trait Request: Writer {
    fn r#type() -> u32;

    fn partition_id(&self) -> i32 {
        -1
    }

    fn encoder() -> fn(u64, Self, Frame) -> Message;
}

// pub(crate) trait Response: Reader {
pub(crate) trait Response {
    fn r#type() -> u32;

    fn decoder() -> fn(Message) -> Self;
}

// #[derive(Writer, Reader, Eq, PartialEq, Hash, Display, Debug, Clone)]
#[derive(Writer, Eq, PartialEq, Hash, Display, Debug, Clone)]
#[display(fmt = "{}:{}", host, port)]
pub(crate) struct Address {
    host: String,
    port: u32,
}

impl Address {
    pub(crate) fn new(host: String, port: u32) -> Self {
        Address { host, port}
    }
}

impl From<&std::net::SocketAddr> for Address {
    fn from(address: &SocketAddr) -> Self {
        Address {
            host: address.ip().to_string(),
            port: address.port() as u32,
        }
    }
}

// #[derive(Reader, Eq, PartialEq, Debug)]
#[derive(Eq, PartialEq, Debug)]
pub(crate) struct ClusterMember {
    address: Address,
    id: String,
    lite: bool,
    attributes: Vec<AttributeEntry>,
}

// #[derive(Reader, Eq, PartialEq, Debug, Clone)]
#[derive(Eq, PartialEq, Debug, Clone)]
pub(crate) struct AttributeEntry {
    _key: String,
    _value: String,
}

#[derive(Writer, Reader, Eq, PartialEq, Debug, Clone)]
pub(crate) struct ReplicaTimestampEntry {
    key: Uuid,
    value: i64,
}

impl ReplicaTimestampEntry {
    pub(crate) fn new(key: Uuid, value: i64) -> Self {
        ReplicaTimestampEntry { key, value}
    }

    pub(crate) fn key(&self) -> Uuid {
        self.key
    }

    pub(crate) fn value(&self) -> i64 {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};

    use crate::codec::{Reader, Writer};

    use super::*;

    // #[test]
    // fn should_write_and_read_address() {
    //     let address = Address {
    //         host: "localhost".to_string(),
    //         port: 5701,
    //     };
    //
    //     let mut writeable = BytesMut::new();
    //     address.write_to(&mut writeable);
    //
    //     let readable = &mut writeable.to_bytes();
    //     assert_eq!(Address::read_from(readable), address);
    // }
    //
    // #[test]
    // fn should_read_cluster_member() {
    //     let address = Address {
    //         host: "localhost".to_string(),
    //         port: 5701,
    //     };
    //     let id = "id";
    //     let lite = true;
    //
    //     let writeable = &mut BytesMut::new();
    //     address.write_to(writeable);
    //     id.write_to(writeable);
    //     lite.write_to(writeable);
    //     0u32.write_to(writeable);
    //
    //     let readable = &mut writeable.to_bytes();
    //     assert_eq!(
    //         ClusterMember::read_from(readable),
    //         ClusterMember {
    //             address,
    //             id: id.to_string(),
    //             lite,
    //             attributes: vec!(),
    //         }
    //     );
    // }
    //
    // #[test]
    // fn should_read_attribute() {
    //     let key = "key";
    //     let value = "value";
    //
    //     let writeable = &mut BytesMut::new();
    //     key.write_to(writeable);
    //     value.write_to(writeable);
    //
    //     let readable = &mut writeable.to_bytes();
    //     assert_eq!(
    //         AttributeEntry::read_from(readable),
    //         AttributeEntry {
    //             _key: key.to_string(),
    //             _value: value.to_string(),
    //         }
    //     );
    // }
    //
    #[test]
    fn should_write_replica_timestamp_entry() {

        let key = Uuid::new_v4();
        let replica_timestamp = ReplicaTimestampEntry {
            key,
            value: 69,
        };
    
        let writeable = &mut BytesMut::new();
        replica_timestamp.write_to(writeable);
    
        let readable = &mut writeable.to_bytes();
        assert_eq!(Uuid::read_from(readable), replica_timestamp.key);
        assert_eq!(i64::read_from(readable), replica_timestamp.value);
    }
    
    #[test]
    fn should_read_replica_timestamp_entry() {
        let key = Uuid::new_v4();
        let value = 12;
    
        let writeable = &mut BytesMut::new();
        key.write_to(writeable);
        value.write_to(writeable);
    
        let readable = &mut writeable.to_bytes();
        assert_eq!(
            ReplicaTimestampEntry::read_from(readable),
            ReplicaTimestampEntry { key, value }
        );
    }
}
