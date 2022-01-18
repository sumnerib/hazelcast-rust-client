use bytes::{Bytes, BytesMut, Buf};
use std::collections::LinkedList;
use std::convert::{Infallible, TryInto};
use std::cmp::Ordering;
use std::collections::linked_list::{Iter, IntoIter, IterMut};
use std::mem;
use crate::codec::{Readable, Writer};
use crate::messaging::{Request, Response};
use crate::{HazelcastClientError, TryFrom};
use crate::messaging::error::Exception;
use crate::codec::Reader;

pub(crate) const DEFAULT_FLAGS: u16 = 0;
pub(crate) const BEGIN_FRAGMENT_FLAG: u16 = 1 << 15;
pub(crate) const END_FRAGMENT_FLAG: u16 = 1 << 14;
pub(crate) const IS_FINAL_FLAG: u16 = 1 << 13;
pub(crate) const UNFRAGMENTED_MESSAGE: u16 = BEGIN_FRAGMENT_FLAG | END_FRAGMENT_FLAG;
pub(crate) const END_DATA_STRUCTURE_FLAG: u16 = 1 << 11;
pub(crate) const BEGIN_DATA_STRUCTURE_FLAG: u16 = 1 << 12;
pub(crate) const IS_NULL_FLAG: u16 = 1 << 10;

const FLAGS_LENGTH: usize = 2;
const FRAME_LENGTH: usize = 4;
const MESSAGE_TYPE_LENGTH: usize = 4;
const CORRELATION_ID_LENGTH: usize = 8;
pub(crate) const FIXED_FIELD_OFFSET: usize = MESSAGE_TYPE_LENGTH
    + CORRELATION_ID_LENGTH
    + mem::size_of::<u8>();

const HEADER_LENGTH: usize = 18;

#[derive(Debug)]
pub(crate) struct Message { id: u64, r#type: u32, frames: LinkedList<Frame> }

impl Message {

    pub(crate) fn new(id: u64, r#type: u32, initial_frame: Frame) -> Self {
        let mut frames = LinkedList::new();
        frames.push_back(initial_frame);
        Message {id, r#type, frames }
    }

    pub(crate) fn add(&mut self, frame: Frame) {
        self.frames.push_back(frame);
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn iter(&self) -> Iter<'_, Frame> {
        self.frames.iter()
    }

    pub(crate) fn iter_mut(&mut self) -> IterMut<'_, Frame> {
        self.frames.iter_mut()
    }
}

impl<R: Request> From<(u64, R)> for Message {
    fn from(request: (u64, R)) -> Self {
        let initial_frame= Frame::initial_frame(UNFRAGMENTED_MESSAGE,
            R::r#type(), request.0);
        R::encoder()(request.0, request.1, initial_frame)
    }
}

impl<R: Response> TryFrom<R> for Message {
    type Error = HazelcastClientError;

    fn try_from(self) -> Result<R, Self::Error> {

        if self.r#type == R::r#type() {
            Ok(R::decoder()(self))
        } else {
            assert_eq!(
                self.r#type,
                Exception::r#type(),
                "unknown messaging type: {}, expected: {}",
                self.r#type,
                R::r#type()
            );
            let payload = self.iter().next().unwrap().payload(false);
            Err(HazelcastClientError::ServerFailure(Box::new(Exception::read_from(
                payload,
            ))))
        }
    }
}

/// TODO() probably needs to be pushed down to the tokio FramedRead.decoder() layer
impl From<BytesMut> for Message {
    fn from(mut message_bytes: BytesMut) -> Self {

        // get the first frame
        let init_len: usize = message_bytes.get_u32_le().try_into().unwrap();
        let init_flags = message_bytes.get_u16_le();
        let message_type = message_bytes.get_u32_le();
        let id = message_bytes.get_u64_le();
        let mut init_frame = Frame::initial_frame(init_flags, message_type, id);
        let init_content = message_bytes.split_to(init_len - HEADER_LENGTH);
        init_frame.append_content(init_content);

        let mut message = Message::new(id, message_type, init_frame);

        // get remaining
        while message_bytes.has_remaining() {
            let len: usize = message_bytes.get_u32_le().try_into().unwrap();
            let flags = message_bytes.get_u16_le();
            let content = message_bytes.split_to(len - (FRAME_LENGTH + FLAGS_LENGTH));
            message.add(Frame::new(content, flags));
            if is_flag_set(flags, IS_FINAL_FLAG) {
                break;
            }
        }

        message
    }
}

#[derive(Eq, PartialEq, Debug)]
pub(crate) struct Frame {
    content: BytesMut,
    pub(crate) flags: u16,
    r#type: u32,
    id: u64,
    pub(crate) is_first: bool
}

impl Frame {

    /// Intended for non-initial frames
    pub(crate) fn new(content: BytesMut, flags: u16) -> Self {
        Frame { content, flags, r#type: 0, id: 0, is_first: false }
    }

    pub(crate) fn from(mut content: BytesMut, is_first: bool) -> Self {

        let flags = content.split_to(FLAGS_LENGTH).to_bytes().read_u16();
        let mut frame: Frame;
        if is_first {
            let message_type = content
                .split_to(MESSAGE_TYPE_LENGTH)
                .to_bytes().
                read_u32();
            let correlation_id = content
                .split_to(CORRELATION_ID_LENGTH)
                .to_bytes()
                .read_u64();
            frame = Self::initial_frame(flags, message_type, correlation_id);
            frame.append_content(content)
        } else {
            frame = Self::new(content, flags);
        }

        frame
    }

    pub(crate) fn append_content(&mut self, content: BytesMut) {
        self.content.extend_from_slice(content.as_ref())
    }

    pub(crate) fn initial_frame(flags: u16, message_type: u32,
                                correlation_id: u64) -> Self {

        let mut content = BytesMut::with_capacity(HEADER_LENGTH);
        message_type.write_to(&mut content);
        correlation_id.write_to(&mut content);

        Frame { content, flags, r#type: message_type, id: correlation_id, is_first: true }
    }

    pub(crate) fn is_end_frame(&self) -> bool {
        is_flag_set(self.flags, END_DATA_STRUCTURE_FLAG)
    }

    pub(crate) fn is_begin_frame(&self) -> bool {
        is_flag_set(self.flags, BEGIN_DATA_STRUCTURE_FLAG)
    }

    pub(crate) fn is_null_frame(&self) -> bool {
        is_flag_set(self.flags, IS_NULL_FLAG)
    }

    /// Returns the correlation id iff this is the first frame
    ///
    /// # Panics
    ///
    /// Panics if is_first() for this Frame returns false
    pub(crate) fn id(&self) -> u64 {
        if !self.is_first {
            panic!("Frame.id() can only be used on the 'first frame' of a message");
        }

        self.id
    }

    /// Returns the message tyep iff this is the first frame
    ///
    /// # Panics
    ///
    /// Panics if is_first() for this Frame returns false
    pub(crate) fn r#type(&self) -> u32 {
        if !self.is_first {
            panic!("Frame.r#type() can only be used on the 'first frame' of a message");
        }

        self.r#type
    }

    pub(crate) fn payload(&self, is_final: bool) -> Bytes {
        let mut payload = BytesMut::with_capacity(
            FLAGS_LENGTH + self.content.len());

        if is_final {
            (self.flags | IS_FINAL_FLAG).write_to(&mut payload);
        } else {    
            self.flags.write_to(&mut payload);
        }

        self.content.write_to(&mut payload);
        payload.to_bytes()
    }

    /// Consumes the bytes with the content field
    pub(crate) fn content(&mut self) -> Bytes {
        self.content.to_bytes()
    }
}

impl From<BytesMut> for Frame {

    fn from(content: BytesMut) -> Self {
        Frame::new(content, DEFAULT_FLAGS)
    }
}

pub(crate) fn is_flag_set(flags: u16, mask: u16) -> bool {
    (flags & mask) == mask
}

#[cfg(test)]
mod tests {
    use crate::codec::Reader;
    use super::*;

    #[test]
    fn should_convert_to_message_from_request() {
        let id = 1;
        let request = SomeRequest { field: 36 };

        let message: Message = (id, request).into();
        let mut expected = Frame::initial_frame(
            UNFRAGMENTED_MESSAGE, 0x0A0B0C0D, id);
        let partition_id : &[u8] = &[0xff, 0xff, 0xff, 0xff];
        expected.append_content(BytesMut::from(partition_id));
        expected.append_content(BytesMut::from("$"));
        assert_eq!(expected, *message.iter().next().expect("should have"));
    }

    #[test]
    fn should_convert_to_message_from_bytes() {
        let mut message_bytes: BytesMut = BytesMut::new();
        message_bytes.extend_from_slice(&[
            0x3c, 0x00, 0x00, 0x00, 0x00, 0xc0, 0x01, 0x01,     // first frame
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x49, 0x2e, 
            0xc0, 0x97, 0x9d, 0xeb, 0x37, 0x13, 0x85, 0xb8,
            0xc3, 0xd9, 0x54, 0xd6, 0x8b, 0x01, 0x0f, 0x01,
            0x00, 0x00, 0x00, 0x2e, 0x4f, 0x78, 0x21, 0x19,
            0x50, 0xcf, 0x85, 0xdd, 0x08, 0xb9, 0xcc, 0xff, 
            0x94, 0x70, 0x85, 0x00,  

            0x06, 0x00, 0x00, 0x00,0x00, 0x10,                  // Begin data structure frame    
            
            0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x45, 0x16,     // data structure init frame
            0x00, 0x00,
            
            0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x30,     // String field: "10.0.0.191"
            0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x39, 0x31,
            
            0x06, 0x00, 0x00, 0x00, 0x00, 0x08,                 // End data structure frame
            
            0x12, 0x00, 0x00, 0x00, 0x00, 0x20, 0x35, 0x2e,     // String frame with IS_FINAL marked
            0x30, 0x2d, 0x53, 0x4e, 0x41, 0x50, 0x53, 0x48, 
            0x4f, 0x54
        ]);

        let message: Message = message_bytes.into();
        let mut message_iter = message.iter();
        let first = message_iter.next().expect("should have");
        assert_eq!(0xC000, first.flags);
        message_iter.next().expect("should have");
        message_iter.next().expect("should have");
        message_iter.next().expect("should have");
        message_iter.next().expect("should have");
        let last = message_iter.next().expect("should have");
        assert_eq!(0x2000, last.flags);
    }

    #[test]
    fn should_convert_to_response_from_message() {

        let mut content: BytesMut = BytesMut::new();
        content.extend_from_slice(&[
            0x1D, 0x4D, // flags
            0x0D, 0x0C, 0x0B, 0x0A,// type
            1, 0, 0, 0, 0, 0, 0, 0, // correlation id
            0, // backup acks
            36  // payload
        ]);
        let initial = Frame::from(content, true);
        let message = Message::new(1, 0x0A0B0C0D, initial);
        let response = TryFrom::<SomeRequest>::try_from(message);
        let expected = SomeRequest { field: 36 };
        assert_eq!(expected, response.unwrap());
    }

    #[test]
    #[should_panic]
    fn test_message_to_response_panic() {

        let mut content: BytesMut = BytesMut::new();
        content.extend_from_slice(&[
            0x1D, 0x4D, // flags
            0x0f, 0x0f, 0x0f, 0x0f,  // bad type
            1, 0, 0, 0, 0, 0, 0, 0, // correlation id
            0, // backup acks
            36  // payload
        ]);
        let initial = Frame::from(content, true);
        let message = Message::new(1, 65535, initial);
        let response = TryFrom::<SomeRequest>::try_from(message);
        response.unwrap();
    }

    #[test]
    fn test_frame_payload() {
        let id = 1;
        let request = SomeRequest { field: 36 };
        let message: Message = (id, request).into();
        let frame = message.iter().next();

        assert_eq!(
            frame.expect("").payload(false).bytes(),
            [
                0, 0xC0, // flags
                0x0D, 0x0C, 0x0B, 0x0A,// type
                1, 0, 0, 0, 0, 0, 0, 0, // correlation id
                255, 255, 255, 255, // partition id
                36  // payload
            ]
        );

        let request = SomeRequest { field: 36 };
        let message: Message = (id, request).into();
        let frame = message.iter().next();
        assert_eq!(
            frame.expect("").payload(true).bytes(),
            [
                0, 0xE0, // flags
                0x0D, 0x0C, 0x0B, 0x0A,// type
                1, 0, 0, 0, 0, 0, 0, 0, // correlation id
                255, 255, 255, 255, // partition id
                36  // payload
            ]
        );
    }

    #[test]
    fn test_is_flag_set() {
        assert!(is_flag_set(0xffff, 1 << 10));
        assert!(!is_flag_set(0, 1 << 10));
        assert!(is_flag_set(0x3000, 1 << 13));
    }

    #[test]
    fn test_message_iterator() {

        let mut content = BytesMut::new();
        content.extend_from_slice(&[1, 2, 3, 4]);
        let mut message = Message::new(1, 0,Frame::new(content, DEFAULT_FLAGS));
        match message.iter().next() {
            Some(f) => assert!(true),
            None => assert!(false)
        }
    }

    #[test]
    fn test_initial_frame() {

        let mut content = BytesMut::new();
        content.extend_from_slice(&[
            0x00, 0x01, 0x1D, 0x00,// type
            1, 0, 0, 0, 0, 0, 0, 0, // correlation id
        ]);

        let expected = Frame { content, flags: 0xE000, r#type: 0x1D0100, id: 1, is_first: true };

        assert_eq!(expected, Frame::initial_frame(0xE000, 0x1D0100, 1));
    }

    #[test]
    fn test_frame_id() {
        let frame = Frame::initial_frame(DEFAULT_FLAGS, 1, 369);
        assert_eq!(369, frame.id());
    }

    #[test]
    #[should_panic]
    fn test_frame_id_panic() {
        let frame = Frame::new(BytesMut::new(), DEFAULT_FLAGS);
        frame.id();
    }

    #[test]
    fn test_frame_type() {
        let frame = Frame::initial_frame(DEFAULT_FLAGS, 1, 369);
        assert_eq!(1, frame.r#type());
    }

    #[test]
    #[should_panic]
    fn test_frame_type_panic() {
        let frame = Frame::new(BytesMut::new(), DEFAULT_FLAGS);
        frame.r#type();
    }

    #[derive(Request, Response, Eq, PartialEq, Debug)]
    #[r#type = 0x0A0B0C0D]
    struct SomeRequest {
        field: u8,
    }

    fn encode_request(id: u64, request: SomeRequest, mut initial_frame: Frame) -> Message {
        let mut fields = BytesMut::new();
        let partition_id = -1;
        partition_id.write_to(&mut fields);
        request.field.write_to(&mut fields);
        initial_frame.append_content(fields);
        Message::new(id, 43981, initial_frame)
    }

    fn decode_response(mut message: Message) -> SomeRequest {

        let mut content = message
            .iter_mut()
            .next()
            .expect("shouldn't be empty")
            .content();
        content.advance(FIXED_FIELD_OFFSET);
        SomeRequest { field: content.get_u8() }
    }

    #[test]
    fn test_frame_from() {
        let mut content: BytesMut = BytesMut::new();
        content.extend_from_slice(&[
            0x1D, 0x4D, // flags
            0x00, 0x0E, 0x0E, 0x0E,// type
            22, 0, 0, 0, 0, 0, 0, 0, // correlation id
            255, 255, 255, 255, // partition id
            36  // payload
        ]);
        let frame=  Frame::from(content, true);
        assert_eq!(0x4D1D, frame.flags);
        assert_eq!(22, frame.id());
    }
}
