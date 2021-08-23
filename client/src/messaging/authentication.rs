use derive_more::Display;

use crate::messaging::{Address, ClusterMember};
use crate::remote::message::Message;
use uuid::Uuid;
use bytes::{Bytes, BytesMut};
use crate::remote::message::{Frame, DEFAULT_FLAGS};
use core::mem;
use crate::codec::Writer;
use std::ops::Deref;

const REQUEST_TYPE: u32 = 256;

#[derive(Request, Eq, PartialEq, Debug)]
#[r#type = 0x100]
pub(crate) struct AuthenticationRequest<'a> {
    uuid: Uuid,
    serialization_version: u8,
    username: &'a str,
    password: &'a str,
    id: Option<&'a str>,
    owner_id: Option<&'a str>,
    owner_connection: bool,
    client_type: &'a str,
    client_version: &'a str,
}

impl<'a> AuthenticationRequest<'a> {
    pub(crate) fn new(
        serialization_version: u8,
        username: &'a str,
        password: &'a str,
        client_type: &'a str,
        client_version: &'a str,
    ) -> Self {
        AuthenticationRequest {
            uuid: Uuid::new_v4(),
            serialization_version,
            username,
            password,
            id: None,
            owner_id: None,
            owner_connection: true,
            client_type,
            client_version,
        }
    }
}

pub(crate) fn encode_request(cluster_name: String, username: Option<String>,
                             password: Option<String>, uuid: Uuid, client_type: String,
                             serialization_version: u8, client_version: String,
                             client_name: String, mut initial_frame: Frame) -> Message {
    use crate::codec::Writer;

    let mut fields =
        BytesMut::with_capacity(mem::size_of::<Uuid>() + 1);

    let partition_id = -1;
    partition_id.write_to(&mut fields);
    uuid.write_to(&mut fields);
    serialization_version.write_to(&mut fields);
    initial_frame.append_content(fields);
    let mut message = Message::new(initial_frame);
    let mut cluster_name_bytes =
        BytesMut::with_capacity(cluster_name.len());
    cluster_name.write_to(&mut cluster_name_bytes);
    let cluster_name_frame = cluster_name_bytes.into();
    message.add(cluster_name_frame);

    message
}

#[derive(Display)]
pub(crate) enum AuthenticationStatus {
    Authenticated,
    CredentialsFailed,
    SerializationVersionMismatch,
    NotAllowedInCluster,
}

#[derive(Response, Eq, PartialEq, Debug)]
#[r#type = 0x6B]
pub(crate) struct AuthenticationResponse {
    status: u8,
    address: Option<Address>,
    id: Option<String>,
    owner_id: Option<String>,
    _serialization_version: u8,
    _unregistered_cluster_members: Option<Vec<ClusterMember>>,
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

    pub(crate) fn id(&self) -> &Option<String> {
        &self.id
    }

    pub(crate) fn owner_id(&self) -> &Option<String> {
        &self.owner_id
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};

    use crate::codec::{Reader, Writer};

    use super::*;

    #[test]
    fn should_write_authentication_request() {
        let request = AuthenticationRequest::new(1, "username", "password", "Rust", "1.0.0");

        let mut writeable = BytesMut::new();
        request.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        Uuid::read_from(readable);
        assert_eq!(u8::read_from(readable), request.serialization_version);
        assert_eq!(String::read_from(readable), request.username);
        assert_eq!(String::read_from(readable), request.password);
        assert_eq!(bool::read_from(readable), true);
        assert_eq!(bool::read_from(readable), true);
        assert_eq!(bool::read_from(readable), true);
        assert_eq!(String::read_from(readable), request.client_type);
        assert_eq!(String::read_from(readable), request.client_version);
    }

    #[test]
    fn should_read_authentication_response() {
        let status = 0u8;
        let address = Some(Address {
            host: "localhost".to_string(),
            port: 5701,
        });
        let id = Some("id");
        let owner_id = Some("owner-id");
        let protocol_version = 1;

        let writeable = &mut BytesMut::new();
        status.write_to(writeable);
        address.write_to(writeable);
        id.write_to(writeable);
        owner_id.write_to(writeable);
        protocol_version.write_to(writeable);
        true.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            AuthenticationResponse::read_from(readable),
            AuthenticationResponse {
                status,
                address,
                id: id.map(str::to_string),
                owner_id: owner_id.map(str::to_string),
                _serialization_version: protocol_version,
                _unregistered_cluster_members: None,
            }
        );
    }

    #[test]
    fn test_encode_request() {
        let initial_frame = Frame::initial_frame(
            DEFAULT_FLAGS, REQUEST_TYPE, 1);
        let message = encode_request(
            "dev".to_string(), Some("user".to_string()),
            Some("pass".to_string()), Uuid::new_v4(),
            "rust".to_string(), 1, "5.0".to_string(),
            "hz_client".to_string(), initial_frame
        );

        let mut message_iter = message.iter();
        let initial_frame = message_iter.next().expect("Empty message!");
        let cluster_name_frame = message_iter.next().expect("Second frame should be name");
    }
}
