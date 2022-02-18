
pub mod pn_counter_get {

    use std::mem;

    use bytes::{BytesMut, Buf};
    use uuid::Uuid;

    use crate::codec::util::UUID_SIZE;
    use crate::codec::{Writer, util, custom, Reader};
    use crate::messaging::ReplicaTimestampEntry;
    use crate::remote::message::{Frame, FIXED_FIELD_OFFSET};
    use crate::remote::message::Message;

    pub(crate) const REQUEST_TYPE: u32 = 0x1D0100;
    #[cfg(test)]
    pub(crate) const RESPONSE_TYPE: u32 = 0x1D0101;

    #[derive(Request, Eq, PartialEq, Debug)]
    #[r#type = 0x1D0100]
    pub(crate) struct PnCounterGetRequest<'a> {
        name: &'a str,
        replica_timestamps: &'a [ReplicaTimestampEntry],
        target_uuid: &'a Uuid
    }

    impl<'a> PnCounterGetRequest<'a> {
        pub(crate) fn new(name: &'a str, replica_timestamps: &'a [ReplicaTimestampEntry], target_uuid: &'a Uuid) -> Self {
            PnCounterGetRequest {
                name,
                target_uuid,
                replica_timestamps,
            }
        }
    }

    pub(crate) fn encode_request(id: u64, 
            request: PnCounterGetRequest, 
            mut initial_frame: Frame) -> Message {
        
        let mut fields = BytesMut::with_capacity(
            mem::size_of::<i32>()
            + UUID_SIZE
        );
        let partition_id = -1;
        partition_id.write_to(&mut fields);
        util::encode_uuid(&mut fields, *request.target_uuid);
        initial_frame.append_content(fields);
        let mut message = Message::new(id, REQUEST_TYPE, initial_frame);
        util::encode_string(&mut message, request.name.to_string());
        custom::encode_replica_timestamp_list(&mut message, request.replica_timestamps);
        
        message
    }

    #[derive(Response, Eq, PartialEq, Debug)]
    #[r#type = 0x1D0101]
    pub(crate) struct PnCounterGetResponse {
        value: i64,
        replica_timestamps: Vec<ReplicaTimestampEntry>,
    }

    impl PnCounterGetResponse {
        
        #[cfg(test)]
        pub(crate) fn new(value: i64, replica_timestamps: Vec<ReplicaTimestampEntry>) -> Self {
            PnCounterGetResponse { value, replica_timestamps}
        }

        pub(crate) fn value(&self) -> i64 {
            self.value
        }

        pub(crate) fn replica_timestamps(&self) -> &[ReplicaTimestampEntry] {
            &self.replica_timestamps
        }
    }

    pub(crate) fn decode_response(mut message: Message) -> PnCounterGetResponse {
        let mut iter = message.iter_mut().peekable();
        let mut initial_content = iter
            .next()
            .expect("Cannot decode empty message!")
            .content();
        initial_content.advance(FIXED_FIELD_OFFSET);
        let value = i64::read_from(&mut initial_content);
        let _replica_count = i32::read_from(&mut initial_content);
        let replica_timestamps = custom::decode_replica_timestamp_list(&mut iter);
        PnCounterGetResponse { value, replica_timestamps }
    }
}

pub mod pn_counter_add {
    use std::mem;

    use bytes::{BytesMut, Buf};
    use uuid::Uuid;

    use crate::codec::{Writer, util, custom, Reader};
    use crate::codec::util::UUID_SIZE;
    use crate::messaging::{ReplicaTimestampEntry};
    use crate::remote::message::{Frame, FIXED_FIELD_OFFSET};
    use crate::remote::message::Message;

    pub(crate) const REQUEST_TYPE: u32 = 0x1D0200;
    #[cfg(test)]
    pub(crate) const RESPONSE_TYPE: u32 = 0x1D0201;

    #[derive(Request, Eq, PartialEq, Debug)]
    #[r#type = 0x1D0200]
    pub(crate) struct PnCounterAddRequest<'a> {
        name: &'a str,
        delta: i64,
        get_before_update: bool,
        replica_timestamps: &'a [ReplicaTimestampEntry],
        target_uuid: &'a Uuid
    }

    impl<'a> PnCounterAddRequest<'a> {
        pub(crate) fn new(
            name: &'a str,
            delta: i64,
            get_before_update: bool,
            replica_timestamps: &'a [ReplicaTimestampEntry],
            target_uuid: &'a Uuid
        ) -> Self {
            PnCounterAddRequest {
                name,
                target_uuid,
                delta,
                get_before_update,
                replica_timestamps,
            }
        }
    }

    pub(crate) fn encode_request(id: u64, request: PnCounterAddRequest, mut initial_frame: Frame) -> Message {

        let mut fields = BytesMut::with_capacity(
            mem::size_of::<i32>() +
            mem::size_of::<i64>() +
            mem::size_of::<bool>() +
            UUID_SIZE
        );
        let partition_id = -1;
        partition_id.write_to(&mut fields);
        request.delta.write_to(&mut fields);
        request.get_before_update.write_to(&mut fields);
        util::encode_uuid(&mut fields, *request.target_uuid);
        initial_frame.append_content(fields);
        let mut message = Message::new(id, REQUEST_TYPE, initial_frame);
        util::encode_string(&mut message, request.name.to_string());
        custom::encode_replica_timestamp_list(&mut message, request.replica_timestamps);
        
        message
    }

    #[derive(Response, Eq, PartialEq, Debug)]
    #[r#type = 0x1D0201]
    pub(crate) struct PnCounterAddResponse {
        value: i64,
        replica_timestamps: Vec<ReplicaTimestampEntry>,
        _replica_count: i32,
    }

    impl PnCounterAddResponse {
        pub(crate) fn new(value: i64, replica_timestamps: Vec<ReplicaTimestampEntry>, 
                _replica_count: i32) -> Self {
            PnCounterAddResponse { value, replica_timestamps, _replica_count }
        }

        pub(crate) fn value(&self) -> i64 {
            self.value
        }

        pub(crate) fn replica_timestamps(&self) -> &[ReplicaTimestampEntry] {
            &self.replica_timestamps
        }
    }

    pub(crate) fn decode_response(mut message: Message) -> PnCounterAddResponse {
        let mut iter = message.iter_mut().peekable();
        let mut initial_content = iter
            .next()
            .expect("Cannot decode empty message!")
            .content();
        initial_content.advance(FIXED_FIELD_OFFSET);
        let value = i64::read_from(&mut initial_content);
        let replica_count = i32::read_from(&mut initial_content);
        let replica_timestamps = custom::decode_replica_timestamp_list(&mut iter);
        PnCounterAddResponse::new(value, replica_timestamps, replica_count)
    }
}

pub mod pn_counter_get_replica_count {
    use std::mem;

    use bytes::Buf;
    use bytes::BytesMut;

    use crate::codec::{Writer, util, Reader};
    use crate::remote::message::FIXED_FIELD_OFFSET;
    use crate::remote::message::Frame;
    use crate::remote::message::Message;


    pub(crate) const REQUEST_TYPE: u32 = 0x1D0300;
    #[cfg(test)]
    pub(crate) const RESPONSE_TYPE: u32 = 0x1D0301;

    #[derive(Request, Eq, PartialEq, Debug)]
    #[r#type = 0x1D0300]
    pub(crate) struct PnCounterGetReplicaCountRequest<'a> {
        name: &'a str,
    }

    impl<'a> PnCounterGetReplicaCountRequest<'a> {
        pub(crate) fn new(name: &'a str) -> Self {
            PnCounterGetReplicaCountRequest { name }
        }
    }

    pub(crate) fn encode_request(id: u64, request: PnCounterGetReplicaCountRequest, mut initial_frame: Frame) -> Message {
        let mut fields = BytesMut::with_capacity(mem::size_of::<i32>());
        let partition_id = -1;
        partition_id.write_to(&mut fields);
        initial_frame.append_content(fields);
        let mut message = Message::new(id, REQUEST_TYPE, initial_frame);
        util::encode_string(&mut message, request.name.to_string());
        
        message
    }


    #[derive(Response, Eq, PartialEq, Debug)]
    #[r#type = 0x1D0301]
    pub(crate) struct PnCounterGetReplicaCountResponse {
        count: i32,
    }

    impl PnCounterGetReplicaCountResponse {
        pub(crate) fn new(count: i32) -> Self {
            PnCounterGetReplicaCountResponse { count }
        }

        pub(crate) fn count(&self) -> i32 {
            self.count
        }
    }

    pub(crate) fn decode_response(mut message: Message) -> PnCounterGetReplicaCountResponse {
        let mut iter = message.iter_mut().peekable();
        let mut initial_content = iter
            .next()
            .expect("Cannot decode empty message!")
            .content();
        initial_content.advance(FIXED_FIELD_OFFSET);
        let count = i32::read_from(&mut initial_content);
        PnCounterGetReplicaCountResponse::new(count)
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use bytes::BytesMut;
    use uuid::Uuid;

    use crate::{codec::{Writer, custom}, remote::message::{DEFAULT_FLAGS, Frame, Message}, messaging::pn_counter::{pn_counter_get::PnCounterGetResponse, pn_counter_add::PnCounterAddResponse, pn_counter_get_replica_count::PnCounterGetReplicaCountResponse}};

    use super::{pn_counter_get::{PnCounterGetRequest, self}, pn_counter_add::{PnCounterAddRequest, self}, pn_counter_get_replica_count::{self, PnCounterGetReplicaCountRequest}};

    #[test]
    fn test_encode_get_request() {
        let target_uuid = Uuid::new_v4();
        let initial_frame = Frame::initial_frame(
            DEFAULT_FLAGS, pn_counter_get::REQUEST_TYPE, 1);
        let req = PnCounterGetRequest::new(
            "my_pn_counter", 
            &[], 
            &target_uuid
        );
        let message = pn_counter_get::encode_request(1,req, initial_frame);

        let mut message_iter = message.iter();
        message_iter.next().expect("Empty message!");
        message_iter.next().expect("Second frame should be name!");
        message_iter.next().expect("Should have replicaTimestamps!");
    }

    #[test]
    fn test_decode_get_response() {
        let mut initial_frame = Frame::initial_frame(
            DEFAULT_FLAGS, 
            pn_counter_get::RESPONSE_TYPE, 
            1
        );
        let mut fields = BytesMut::with_capacity(
            mem::size_of::<u8>()  +     // backup_acks
            mem::size_of::<i64>() +     // value
            mem::size_of::<i32>()     // replica count
        );

        0u8.write_to(&mut fields);
        10i64.write_to(&mut fields);
        1i32.write_to(&mut fields);
        initial_frame.append_content(fields);
        let mut message = Message::new(1, pn_counter_get::RESPONSE_TYPE, initial_frame);

        custom::encode_replica_timestamp_list(&mut message, &[]);

        let actual = pn_counter_get::decode_response(message);
        assert_eq!(
            PnCounterGetResponse::new(10, Vec::new()),
            actual
        );
    }

    #[test]
    fn test_encode_add_request() {
        let target_uuid = Uuid::new_v4();
        let initial_frame = Frame::initial_frame(
            DEFAULT_FLAGS, pn_counter_add::REQUEST_TYPE, 1);
        let req = PnCounterAddRequest::new(
            "my_counter",
            45,
            true,
            &[],
            &target_uuid
        );
        let message = pn_counter_add::encode_request(1, req, initial_frame);

        let mut message_iter = message.iter();
        message_iter.next().expect("Empty message!");
        message_iter.next().expect("Second frame should be name!");
        message_iter.next().expect("Should have replicaTimestamps!");
    }

    #[test]
    fn test_decode_add_response() {
        let mut initial_frame = Frame::initial_frame(
            DEFAULT_FLAGS, 
            pn_counter_add::RESPONSE_TYPE, 
            1
        );
        let mut fields = BytesMut::with_capacity(
            mem::size_of::<u8>()  +     // backup_acks
            mem::size_of::<i64>() +     // value
            mem::size_of::<i32>()     // replica count
        );

        0u8.write_to(&mut fields);
        10i64.write_to(&mut fields);
        1i32.write_to(&mut fields);
        initial_frame.append_content(fields);
        let mut message = Message::new(1, pn_counter_add::RESPONSE_TYPE, initial_frame);

        custom::encode_replica_timestamp_list(&mut message, &[]);

        let actual = pn_counter_add::decode_response(message);
        assert_eq!(
            PnCounterAddResponse::new(10, Vec::new(), 1),
            actual
        );
    }

    #[test]
    fn test_encode_get_replica_count_request() {
        let initial_frame = Frame::initial_frame(
            DEFAULT_FLAGS, pn_counter_get_replica_count::REQUEST_TYPE, 1);
        let req = PnCounterGetReplicaCountRequest::new("my_counter",);
        let message = pn_counter_get_replica_count::encode_request(1, req, initial_frame);

        let mut message_iter = message.iter();
        message_iter.next().expect("Empty message!");
        message_iter.next().expect("Second frame should be name!");
    }

    #[test]
    fn test_decode_get_replica_count_response() {
        let mut initial_frame = Frame::initial_frame(
            DEFAULT_FLAGS, 
            pn_counter_get_replica_count::RESPONSE_TYPE, 
            1
        );
        let mut fields = BytesMut::with_capacity(
            mem::size_of::<u8>()  +     // backup_acks
            mem::size_of::<i32>()       // count
        );

        0u8.write_to(&mut fields);
        1i32.write_to(&mut fields);
        initial_frame.append_content(fields);
        let message = Message::new(1, pn_counter_get_replica_count::RESPONSE_TYPE, initial_frame);

        let actual = pn_counter_get_replica_count::decode_response(message);
        assert_eq!(
            PnCounterGetReplicaCountResponse::new(1),
            actual
        );
    }
}
