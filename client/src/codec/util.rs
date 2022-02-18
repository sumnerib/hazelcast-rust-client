use std::mem;
use std::collections::linked_list::IterMut;
use crate::remote::message::{Message, Frame, IS_NULL_FLAG, BEGIN_DATA_STRUCTURE_FLAG, END_DATA_STRUCTURE_FLAG};
use bytes::{BytesMut, Bytes, Buf};
use uuid::Uuid;
use std::iter::Peekable;

use super::{Writeable, Writer, Reader, Readable};

pub(crate) const UUID_SIZE: usize = mem::size_of::<Uuid>() + 1;

pub(crate) fn encode_string(message: &mut Message, value: String) {

    let mut bytes = BytesMut::with_capacity(value.len());
    value.write_to(&mut bytes);
    let frame = bytes.into();
    message.add(frame);
}

pub(crate) fn decode_string(iter: &mut Peekable<IterMut<Frame>>) -> String {
    let content = iter.next().expect("Bad message: missing frame!!").content();
    std::str::from_utf8(&content)
        .expect("Bad message: unable to parse utf8 string!")
        .to_string()
}

pub(crate) fn encode_nullable<T>(message: &mut Message, value: Option<T>,
                                 encoder: fn(&mut Message, T)) {
    match value {
        Some(v) => encoder(message, v),
        None => message.add(Frame::new(BytesMut::new(), IS_NULL_FLAG))
    }
}

pub(crate) fn decode_nullable<T>(iter: &mut Peekable<IterMut<Frame>>,
                                 decoder: fn(&mut Peekable<IterMut<Frame>>) -> T) -> Option<T> {
    if iter.peek()?.is_null_frame() {
        None
    } else {
        Some(decoder(iter))
    }
}

pub(crate) fn encode_list<T>(message: &mut Message, list: Vec<T>,
                             encoder: fn(&mut Message, T)) {
    message.add(Frame::new(BytesMut::new(), BEGIN_DATA_STRUCTURE_FLAG));
    for val in list {
        encoder(message, val);
    }
    message.add(Frame::new(BytesMut::new(), END_DATA_STRUCTURE_FLAG))
}

pub(crate) fn encode_uuid(writeable: &mut dyn Writeable, uuid: Uuid) {
    0u8.write_to(writeable);       // uuid cannot be null
    let bytes = &mut Bytes::copy_from_slice(uuid.as_bytes());
    let msb = bytes.get_u64();
    let lsb = bytes.get_u64();
    msb.write_to(writeable);
    lsb.write_to(writeable);
}

pub(crate) fn decode_uuid(readable: &mut dyn Readable) -> Option<Uuid> {
    if bool::read_from(readable) {      // is null?
        return None
    }

    let lsb = u64::read_from(readable);
    let msb = u64::read_from(readable);
    let mut writeable = BytesMut::with_capacity(mem::size_of::<Uuid>());
    msb.write_to(&mut writeable);
    lsb.write_to(&mut writeable);
    Some(Uuid::read_from(&mut writeable.to_bytes()))
}

#[cfg(test)]
mod test {
    use std::collections::linked_list::IterMut;
    use std::collections::LinkedList;
    use std::iter::Peekable;
    use std::mem;
    use bytes::{BytesMut, Buf};
    use uuid::Uuid;
    use crate::remote::message::{Message, Frame, DEFAULT_FLAGS};
    use crate::codec::util::{encode_string, encode_nullable, encode_list, decode_nullable, decode_string};
    use crate::codec::Writer;

    use super::{encode_uuid, decode_uuid};

    #[test]
    fn test_encode_string() {
        let mut content = BytesMut::new();
        content.extend_from_slice(&[1, 2, 3, 4]);
        let mut message = Message::new(1, 0, Frame::new(content, DEFAULT_FLAGS));

        encode_string(&mut message, "this is a value".to_string());

        let content = BytesMut::from("this is a value");
        let expected = Frame::new(content, DEFAULT_FLAGS);

        let mut message_iter = message.iter();
        message_iter.next();
        assert_eq!(expected, *message_iter.next().expect("should have frame"));
    }

    #[test]
    fn test_decode_string() {
        let expected = "localhost".to_string();
        let mut list: LinkedList<Frame> = LinkedList::new();
        let mut bytes = BytesMut::with_capacity(9);
        expected.write_to(&mut bytes);
        list.push_back(bytes.into());

        let actual = decode_string(&mut list.iter_mut().peekable());
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_encode_nullable() {
        let mut message = Message::new(
            1, 0,
            Frame::initial_frame(
                DEFAULT_FLAGS,
                111,
                1
            ));
        encode_nullable(&mut message, Some("my val".to_string()), encode_string);
        encode_nullable(&mut message, None, encode_string);

        let mut message_iter = message.iter();
        message_iter.next();
        assert!(!message_iter.next().expect("should have frame").is_null_frame());
        assert!(message_iter.next().expect("should have frame").is_null_frame());
    }

    #[test]
    fn test_decode_nullable() {
        let mut message = Message::new(
            1, 0,
            Frame::initial_frame(
                DEFAULT_FLAGS,
                111,
                1
            ));
        let actual = decode_nullable(
            &mut message.iter_mut().peekable(),
            |_iter: &mut Peekable<IterMut<Frame>>| -1
        );
        assert_eq!(Some(-1), actual);

        let mut dummy: LinkedList<Frame> = LinkedList::new();
        let actual = decode_nullable(
            &mut dummy.iter_mut().peekable(),
            |_iter: &mut Peekable<IterMut<Frame>>| -1
        );
        assert_eq!(None, actual);
    }

    #[test]
    fn test_encode_list() {
        let mut message = Message::new(
            1, 0,
            Frame::initial_frame(
                DEFAULT_FLAGS,
                111,
                1
            ));
        let list = vec!["item1".to_string(), "item2".to_string(), "item3".to_string()];
        encode_list(&mut message, list, encode_string);

        let content = BytesMut::from("item2");
        let expected = Frame::new(content, DEFAULT_FLAGS);

        let mut message_iter = message.iter();
        message_iter.next(); //initial
        assert!(message_iter.next().expect("should have frame").is_begin_frame());
        message_iter.next(); // "item1"
        assert_eq!(expected, *message_iter.next().expect("should have frame"));
        message_iter.next(); // "item3"
        assert!(message_iter.next().expect("should have frame").is_end_frame());
    }

    #[test]
    fn test_encode_uuid() {
        let mut fields = BytesMut::with_capacity(1 + mem::size_of::<Uuid>());
        let uuid = Uuid::parse_str("eb66c416-4739-465b-9af3-9dc33ed8eef9").unwrap();
        encode_uuid(&mut fields, uuid);
        assert_eq!(
            [ 
                0,  // not null
                0x5b, 0x46, 0x39, 0x47, 0x16, 0xc4, 0x66, 0xeb,   //msb  EB 66 C4 16 47 39 46 5B
                0xf9, 0xee, 0xd8, 0x3e, 0xc3, 0x9d, 0xf3, 0x9a  // lsb 9A F3 9D C3 3E D8 EE F9
            ],    
            fields.to_bytes().bytes()
        );
    }

    #[test]
    fn test_decode_uuid() {
        let mut fields = BytesMut::with_capacity(1 + mem::size_of::<Uuid>());
        let expected = Uuid::parse_str("11141552-8456-4b0f-aa95-559c81a19f48").unwrap();
        encode_uuid(&mut fields, expected);
        let actual = decode_uuid(&mut fields.to_bytes());
        assert_eq!(expected, actual.expect("Should have UUID"));
    }
}