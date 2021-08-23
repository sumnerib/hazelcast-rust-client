use crate::remote::message::Message;
use bytes::BytesMut;

pub(crate) fn encode_string(message: &mut Message, value: String) {
    use crate::codec::Writer;

    let mut bytes = BytesMut::with_capacity(value.len());
    value.write_to(&mut bytes);
    let frame = bytes.into();
    message.add(frame);
}
#[cfg(test)]
mod test {
    use bytes::BytesMut;
    use crate::remote::message::{Message, Frame, DEFAULT_FLAGS};
    use crate::codec::util::encode_string;

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
}