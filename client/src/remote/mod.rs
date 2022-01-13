use std::convert::TryInto;

use bytes::{Buf, Bytes, BytesMut};

use crate::{
    messaging::{Request, Response},
    HazelcastClientError, TryFrom,
};

mod channel;
pub(crate) mod cluster;
mod member;
pub(crate) mod message;

// const PROTOCOL_SEQUENCE: [u8; 3] = [0x43, 0x42, 0x32];  // CB2
const PROTOCOL_SEQUENCE: [u8; 3] = [0x43, 0x50, 0x32];  // CP2

const CLIENT_TYPE: &str = "Rust";
const CLIENT_VERSION: &'static str = env!("CARGO_PKG_VERSION");
const PROTOCOL_VERSION: u8 = 1;
//
const BEGIN_MESSAGE: u16 = 0x8000;
const END_MESSAGE: u16 = 0x4000;
const IS_FINAL: u16 = 0x2000;
const UNFRAGMENTED_MESSAGE: u16 = BEGIN_MESSAGE | END_MESSAGE;
const SINGLE_FRAME: u16 = UNFRAGMENTED_MESSAGE | IS_FINAL;

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;
const LENGTH_FIELD_ADJUSTMENT: isize = -4;
const HEADER_LENGTH: usize = 22;

// #[derive(Eq, PartialEq, Debug)]
// struct Message(u64, u32, Bytes);
//
// impl Message {
//     fn id(&self) -> u64 {
//         self.0
//     }
//
//     fn r#type(&self) -> u32 {
//         self.1
//     }
//
//     fn payload(&self) -> Bytes {
//         self.2.clone()
//     }
// }
//
// impl<R: Request> From<(u64, R)> for Message {
//     fn from(request: (u64, R)) -> Self {
//         use crate::codec::Writer;
//
//         let mut frame = BytesMut::with_capacity(HEADER_LENGTH - LENGTH_FIELD_LENGTH + request.1.length());
//
//         // let data_offset: u16 = HEADER_LENGTH.try_into().expect("unable to convert");
//
//         // PROTOCOL_VERSION.write_to(&mut frame);
//         SINGLE_FRAME.write_to(&mut frame);
//         R::r#type().write_to(&mut frame);
//         request.0.write_to(&mut frame);
//         request.1.partition_id().write_to(&mut frame);
//         // data_offset.write_to(&mut frame);
//         request.1.write_to(&mut frame);
//
//         Message(request.0, R::r#type(), frame.to_bytes())
//     }
// }
//
// impl From<Bytes> for Message {
//     fn from(mut frame: Bytes) -> Self {
//         use crate::codec::Readable;
//
//         // let _version = frame.read_u8();
//         let _flags = frame.read_u16();
//         let message_type = frame.read_u32();
//         let correlation_id = frame.read_u64();
//         let _partition_id = frame.read_u8();
//
//         // let data_offset: usize = frame.read_u16().try_into().expect("unable to convert!");
//         // frame.skip(data_offset - HEADER_LENGTH);
//
//         Message(correlation_id, message_type, frame.to_bytes())
//     }
// }
//
// impl<R: Response> TryFrom<R> for Message {
//     type Error = HazelcastClientError;
//
//     fn try_from(self) -> Result<R, Self::Error> {
//         use crate::codec::Reader;
//         use crate::messaging::error::Exception;
//
//         let r#type = self.r#type();
//         let mut readable = self.payload();
//
//         if r#type == R::r#type() {
//             Ok(R::read_from(&mut readable))
//         } else {
//             assert_eq!(
//                 r#type,
//                 Exception::r#type(),
//                 "unknown messaging type: {}, expected: {}",
//                 r#type,
//                 R::r#type()
//             );
//             Err(HazelcastClientError::ServerFailure(Box::new(Exception::read_from(
//                 &mut readable,
//             ))))
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use bytes::Buf;

    use super::*;

    // #[test]
    // fn should_convert_to_message_from_request() {
    //     let id = 1;
    //     let request = SomeRequest { field: 2 };
    //
    //     let message: Message = (id, request).into();
    //     assert_eq!(message.id(), id);
    //     assert_eq!(message.r#type(), SomeRequest::r#type());
    //     assert_eq!(
    //         message.payload().bytes(),
    //         [
    //             // 1,   // version
    //             0, 0xE0, // flags
    //             0x00, 0x01, 0x1D, 0x00,// type
    //             1, 0, 0, 0, 0, 0, 0, 0, // correlation id
    //             255, 255, 255, 255, // partition id
    //             2  // payload
    //         ]
    //     );
    // }
    //
    // #[test]
    // fn should_convert_to_message_from_bytes() {
    //     let bytes = Bytes::copy_from_slice(&[
    //         // 1,   // version
    //         0, 0xE0, // flags
    //         0x00, 0x01, 0x1D, 0x00,// type
    //         1, 0, 0, 0, 0, 0, 0, 0, // correlation id
    //         0, // Backup Acks Count
    //         2  // payload
    //     ]);
    //
    //     let message: Message =  bytes.into();
    //     assert_eq!(message.id(), 1);
    //     assert_eq!(message.r#type(), 0x1D0100);
    //     assert_eq!(message.payload().bytes(), [2]);
    // }
    //
    // #[derive(Request, Eq, PartialEq, Debug)]
    // #[r#type = 1900800]
    // struct SomeRequest {
    //     field: u8,
    // }
}
