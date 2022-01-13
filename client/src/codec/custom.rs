use std::collections::linked_list::IterMut;
use std::iter::Peekable;
use crate::codec::{Reader, util};
use crate::messaging::Address;
use crate::remote::message::Frame;

pub(crate) fn decode_address(iter: &mut Peekable<IterMut<Frame>>) -> Address {
    iter.next();    // begin frame
    let mut content = iter.next().expect("Bad message: missing frame!!").content();
    let port = u32::read_from(&mut content);
    let host = util::decode_string(iter);
    iter.next();    // end frame
    Address::new(host, port)
}

#[cfg(test)]
mod test {
    use std::collections::LinkedList;
    use std::mem;
    use bytes::BytesMut;
    use crate::codec::{util, Writer};
    use crate::codec::custom::decode_address;
    use crate::messaging::Address;
    use crate::remote::message::{BEGIN_DATA_STRUCTURE_FLAG, END_DATA_STRUCTURE_FLAG, Frame};

    #[test]
    fn test_decode_address() {
        let mut list = LinkedList::new();
        list.push_back(Frame::new(BytesMut::new(), BEGIN_DATA_STRUCTURE_FLAG));
        let mut bytes = BytesMut::with_capacity(mem::size_of::<u32>());
        5701.write_to(&mut bytes);
        list.push_back(bytes.into());
        let mut bytes = BytesMut::with_capacity(9);
        "localhost".to_string().write_to(&mut bytes);
        list.push_back(bytes.into());
        list.push_back(Frame::new(BytesMut::new(), END_DATA_STRUCTURE_FLAG));

        let actual = decode_address(&mut list.iter_mut().peekable());
        assert_eq!(Address::new("localhost".to_string(), 5701), actual);
    }
}
