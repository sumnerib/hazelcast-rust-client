use std::mem;

use bytes::BytesMut;

use crate::codec::Writer;
use crate::remote::message::Frame;
use crate::remote::message::Message;

#[derive(Request, Eq, PartialEq, Debug)]
#[r#type = 0x000B00]
pub(crate) struct PingRequest {}

impl PingRequest {
    pub(crate) fn new() -> Self {
        PingRequest {}
    }
}

pub(crate) fn encode_request(id: u64, _request: PingRequest, mut initial_frame: Frame) -> Message {
    let mut fields = BytesMut::with_capacity(mem::size_of::<i32>());
    let partition_id = -1;
    partition_id.write_to(&mut fields);
    initial_frame.append_content(fields);
    Message::new(id, 0x000B00, initial_frame)
}

#[derive(Response, Eq, PartialEq, Debug)]
#[r#type = 0x000B01]
pub(crate) struct PingResponse {}

pub(crate) fn decode_response(_message: Message) -> PingResponse {
    PingResponse {}
}

#[cfg(test)]
mod tests {
    

    

    

    #[test]
    fn test_ping_encode_request() {

    }

    // #[test]
    // fn should_write_ping_request() {
    //     let request = PingRequest::new();

    //     let mut writeable = BytesMut::new();
    //     request.write_to(&mut writeable);

    //     let readable = &mut writeable.to_bytes();
    //     assert_eq!(readable.bytes(), []);
    // }

    // #[test]
    // fn should_read_ping_response() {
    //     let readable = &mut BytesMut::new().to_bytes();
    //     assert_eq!(PingResponse::read_from(readable), PingResponse {});
    // }
}
