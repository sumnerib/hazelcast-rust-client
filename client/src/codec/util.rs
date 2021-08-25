use crate::remote::message::{Message, Frame, IS_NULL_FLAG, BEGIN_DATA_STRUCTURE_FLAG, END_DATA_STRUCTURE_FLAG};
use bytes::BytesMut;

pub(crate) fn encode_string(message: &mut Message, value: String) {
    use crate::codec::Writer;

    let mut bytes = BytesMut::with_capacity(value.len());
    value.write_to(&mut bytes);
    let frame = bytes.into();
    message.add(frame);
}

pub(crate) fn encode_nullable<T>(message: &mut Message, value: Option<T>,
                                 encoder: fn(&mut Message, T)) {
    match value {
        Some(v) => encoder(message, v),
        None => message.add(Frame::new(BytesMut::new(), IS_NULL_FLAG))
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

#[cfg(test)]
mod test {
    use bytes::BytesMut;
    use crate::remote::message::{Message, Frame, DEFAULT_FLAGS};
    use crate::codec::util::{encode_string, encode_nullable, encode_list};

    #[test]
    fn test_encode_string() {
        let mut content = BytesMut::new();
        content.extend_from_slice(&[1, 2, 3, 4]);
        let mut message = Message::new(Frame::new(content, DEFAULT_FLAGS));

        encode_string(&mut message, "this is a value".to_string());

        let content = BytesMut::from("this is a value");
        let expected = Frame::new(content, DEFAULT_FLAGS);

        let mut message_iter = message.iter();
        message_iter.next();
        assert_eq!(expected, *message_iter.next().expect("should have frame"));
    }

    #[test]
    fn test_encode_nullable() {
        let mut message = Message::new(
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
    fn test_encode_list() {
        let mut message = Message::new(
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
}