use derive_more::Display;

use crate::messaging::{Address, ClusterMember};
use crate::remote::message::Message;
use uuid::Uuid;
use bytes::{Buf, Bytes, BytesMut};
use crate::remote::message::{Frame, FIXED_FIELD_OFFSET};
use core::mem;
use crate::codec::{Writer, util, Reader, custom};
use std::ops::Deref;

const REQUEST_TYPE: u32 = 256;

#[derive(Request, Eq, PartialEq, Debug)]
#[r#type = 0x100]
pub(crate) struct AuthenticationRequest<'a> {
    uuid: Uuid,
    serialization_version: u8,
    cluster_name: String,
    username: Option<String>,
    password: Option<String>,
    id: Option<&'a str>,
    owner_id: Option<&'a str>,
    owner_connection: bool,
    client_type: String,
    client_version: String,
    client_name: String,
    labels: Vec<String>
}

impl<'a> AuthenticationRequest<'a> {
    pub(crate) fn new(
        serialization_version: u8,
        cluster_name: String,
        username: Option<String>,
        password: Option<String>,
        client_type: String,
        client_version: String,
        client_name: String,
        labels: Vec<String>
    ) -> Self {
        AuthenticationRequest {
            uuid: Uuid::new_v4(),
            serialization_version,
            cluster_name,
            username,
            password,
            id: None,
            owner_id: None,
            owner_connection: true,
            client_type,
            client_version,
            client_name,
            labels
        }
    }
}

pub(crate) fn encode_request(id: u64, request: AuthenticationRequest, mut initial_frame: Frame) -> Message {

    let mut fields =
        BytesMut::with_capacity(mem::size_of::<i32>() + mem::size_of::<Uuid>() + 1);
    let partition_id = -1;
    partition_id.write_to(&mut fields);
    util::encode_uuid(&mut fields, request.uuid);
    request.serialization_version.write_to(&mut fields);
    initial_frame.append_content(fields);
    let mut message = Message::new(id, REQUEST_TYPE, initial_frame);
    util::encode_string(&mut message, request.cluster_name);
    util::encode_nullable(&mut message, request.username, util::encode_string);
    util::encode_nullable(&mut message, request.password, util::encode_string);
    util::encode_string(&mut message, request.client_type);
    util::encode_string(&mut message, request.client_version);
    util::encode_string(&mut message, request.client_name);
    util::encode_list(&mut message, request.labels, util::encode_string);

    message
}

#[derive(Display)]
pub(crate) enum AuthenticationStatus {
    Authenticated,
    CredentialsFailed,
    SerializationVersionMismatch,
    NotAllowedInCluster,
}

const RESPONSE_TYPE: u32 = 257;

#[derive(Response, Eq, PartialEq, Debug)]
#[r#type = 0x101]
pub(crate) struct AuthenticationResponse {
    status: u8,
    address: Option<Address>,
    pub(crate) member_uuid: Uuid,
    _serialization_version: u8,
    hz_server_version: String,
    partition_count: i32,
    pub(crate) cluster_id: Uuid,
    failover_supported: bool
}

impl AuthenticationResponse {
    pub(crate) fn status(&self) -> AuthenticationStatus {
        match &self.status {
            0 => AuthenticationStatus::Authenticated,
            1 => AuthenticationStatus::CredentialsFailed,
            2 => AuthenticationStatus::SerializationVersionMismatch,
            3 => AuthenticationStatus::NotAllowedInCluster,
            _ => panic!("unknown status - {}", &self.status),
        }
    }

    pub(crate) fn address(&self) -> &Option<Address> {
        &self.address
    }

}

pub(crate) fn decode_response(mut message: Message) -> AuthenticationResponse {
    let mut iter = message.iter_mut().peekable();
    let mut initial_content = iter
        .next()
        .expect("Cannot decode empty message!")
        .content();
    initial_content.advance(FIXED_FIELD_OFFSET);
    let status = u8::read_from(&mut initial_content);
    let member_uuid = Uuid::read_from(&mut initial_content);
    let serialization_version = u8::read_from(&mut initial_content);
    let partition_count = i32::read_from(&mut initial_content);
    let cluster_id = Uuid::read_from(&mut initial_content);
    let failover_supported = bool::read_from(&mut initial_content);
    let address = util::decode_nullable(&mut iter, custom::decode_address);
    let hz_server_version = util::decode_string(&mut iter);

    AuthenticationResponse {
        status,
        address,
        member_uuid,
        _serialization_version: serialization_version,
        hz_server_version,
        partition_count,
        cluster_id,
        failover_supported,
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};

    use crate::codec::{Reader, Writer};
    use crate::remote::message::{BEGIN_DATA_STRUCTURE_FLAG, DEFAULT_FLAGS, END_DATA_STRUCTURE_FLAG};

    use super::*;

    // #[test]
    // fn should_write_authentication_request() {
    //     let request = AuthenticationRequest::new(1, "username", "password", "Rust", "1.0.0");
    //
    //     let mut writeable = BytesMut::new();
    //     request.write_to(&mut writeable);
    //
    //     let readable = &mut writeable.to_bytes();
    //     Uuid::read_from(readable);
    //     assert_eq!(u8::read_from(readable), request.serialization_version);
    //     assert_eq!(String::read_from(readable), request.username);
    //     assert_eq!(String::read_from(readable), request.password);
    //     assert_eq!(bool::read_from(readable), true);
    //     assert_eq!(bool::read_from(readable), true);
    //     assert_eq!(bool::read_from(readable), true);
    //     assert_eq!(String::read_from(readable), request.client_type);
    //     assert_eq!(String::read_from(readable), request.client_version);
    // }

    // #[test]
    // fn should_read_authentication_response() {
    //     let status = 0u8;
    //     let address = Some(Address {
    //         host: "localhost".to_string(),
    //         port: 5701,
    //     });
    //     let id = Some("id");
    //     let owner_id = Some("owner-id");
    //     let protocol_version = 1;
    //
    //     let writeable = &mut BytesMut::new();
    //     status.write_to(writeable);
    //     address.write_to(writeable);
    //     id.write_to(writeable);
    //     owner_id.write_to(writeable);
    //     protocol_version.write_to(writeable);
    //     true.write_to(writeable);
    //
    //     let readable = &mut writeable.to_bytes();
    //     assert_eq!(
    //         AuthenticationResponse::read_from(readable),
    //         AuthenticationResponse {
    //             status,
    //             address,
    //             id: id.map(str::to_string),
    //             owner_id: owner_id.map(str::to_string),
    //             _serialization_version: protocol_version,
    //             _unregistered_cluster_members: None,
    //         }
    //     );
    // }

    #[test]
    fn test_encode_request() {
        let initial_frame = Frame::initial_frame(
            DEFAULT_FLAGS, REQUEST_TYPE, 1);
        let req = AuthenticationRequest::new(
            1, "dev".to_string(), Some("user".to_string()),
            Some("pass".to_string()), "rust".to_string(), "5.0".to_string(),
            "hz_client".to_string(), vec!["item".to_string()]);
        let message = encode_request(1,req, initial_frame);

        let mut message_iter = message.iter();
        message_iter.next().expect("Empty message!");
        message_iter.next().expect("Second frame should be name!");
        message_iter.next().expect("Should have username!");
        message_iter.next().expect("Should have password!");
        message_iter.next().expect("Should have client_type!");
        message_iter.next().expect("Should have hz_version!");
        message_iter.next().expect("Should have client_name!");
        message_iter.next().expect("Should have begin_data!");
        message_iter.next().expect("Should have data item!");
        message_iter.next().expect("Should have end_data!");
    }
    
    #[test]
    fn test_decode_response() {

        let status = 0u8;
        let address = Some(Address {
            host: "localhost".to_string(),
            port: 5701,
        });

        let mut initial_frame = Frame::initial_frame(DEFAULT_FLAGS, 0x101, 1);
        let mut fields = BytesMut::with_capacity(
            mem::size_of::<u8>()    // backup ack count
                + mem::size_of::<u8>()      // status
                + mem::size_of::<Uuid>()    // member UUID
                + mem::size_of::<u8>()      // serialization version
                + mem::size_of::<i32>()      // partition count
                + mem::size_of::<Uuid>()      // cluster id
                + mem::size_of::<bool>()      // failover supported
        );

        0u8.write_to(&mut fields);
        status.write_to(&mut fields);
        let member_uuid = Uuid::new_v4();
        member_uuid.write_to(&mut fields);
        1u8.write_to(&mut fields);
        271.write_to(&mut fields);
        let cluster_id = Uuid::new_v4();
        cluster_id.write_to(&mut fields);
        false.write_to(&mut fields);
        initial_frame.append_content(fields);
        let mut message = Message::new(1, RESPONSE_TYPE, initial_frame);

        // address
        message.add(Frame::new(BytesMut::new(), BEGIN_DATA_STRUCTURE_FLAG));
        let mut port_data = BytesMut::with_capacity(mem::size_of::<i32>());
        5701.write_to(&mut port_data);
        let port_frame = Frame::new(port_data, DEFAULT_FLAGS);
        message.add(port_frame);
        util::encode_string(&mut message, "localhost".to_string());
        message.add(Frame::new(BytesMut::new(), END_DATA_STRUCTURE_FLAG));

        util::encode_string(&mut message, "5.0".to_string());

        let actual = decode_response(message);
        assert_eq!(
            AuthenticationResponse {
                status,
                address,
                member_uuid,
                _serialization_version: 1u8,
                hz_server_version: "5.0".to_string(),
                partition_count: 271,
                cluster_id,
                failover_supported: false
            },
            actual
        );
    }
}
