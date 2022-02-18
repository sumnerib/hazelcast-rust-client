use std::collections::linked_list::IterMut;
use std::iter::Peekable;
use std::mem;
use bytes::BytesMut;
use uuid::Uuid;

use crate::codec::{Reader, util};
use crate::messaging::{Address, ReplicaTimestampEntry};
use crate::remote::message::{Frame, Message, DEFAULT_FLAGS};

use super::Writer;

const REPLICA_TIMESTAMP_ENTRY_SIZE: usize = mem::size_of::<Uuid>() + 1 + mem::size_of::<u64>();

pub(crate) fn decode_address(iter: &mut Peekable<IterMut<Frame>>) -> Address {
    iter.next();    // begin frame
    let mut content = iter.next().expect("Bad message: missing frame!!").content();
    let port = u32::read_from(&mut content);
    let host = util::decode_string(iter);
    iter.next();    // end frame
    Address::new(host, port)
}

pub(crate) fn encode_replica_timestamp_list(
        message: &mut Message, entries: &[ReplicaTimestampEntry]) {
    
    let mut content = BytesMut::with_capacity(entries.len() * REPLICA_TIMESTAMP_ENTRY_SIZE);
    for entry in entries {
        util::encode_uuid(&mut content, entry.key());
        entry.value().write_to(&mut content);
    }
    message.add(Frame::new(content, DEFAULT_FLAGS));
}

pub(crate) fn decode_replica_timestamp_list(iter: &mut Peekable<IterMut<Frame>>) -> Vec<ReplicaTimestampEntry> {
    let mut content = iter.next().expect("Bad message: missing frame!!").content();
    let item_count = content.len() / REPLICA_TIMESTAMP_ENTRY_SIZE;
    let mut entries: Vec<ReplicaTimestampEntry> = Vec::new();
    for _ in 0..item_count {
        let key = util::decode_uuid(&mut content).expect("Got bad key from replica timestamp!");
        let value = i64::read_from(&mut content);
        entries.push(ReplicaTimestampEntry::new(key, value));
    }

    entries
}

#[cfg(test)]
mod test {
    use std::collections::LinkedList;
    use std::mem;
    use bytes::{BytesMut, Buf};
    use uuid::Uuid;
    use crate::codec::{util, Writer, Reader};
    use crate::codec::custom::decode_address;
    use crate::messaging::{Address, ReplicaTimestampEntry};
    use crate::remote::message::{BEGIN_DATA_STRUCTURE_FLAG, END_DATA_STRUCTURE_FLAG, Frame, Message, DEFAULT_FLAGS};

    use super::{encode_replica_timestamp_list, decode_replica_timestamp_list};

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

    #[test]
    fn test_encode_replica_timestamp_list() {
        let key = Uuid::new_v4();
        let writeable = &mut BytesMut::new();
        key.write_to(writeable);
        1001_i64.write_to(writeable);
        let entry = ReplicaTimestampEntry::read_from(&mut writeable.to_bytes());

        let key1 = Uuid::new_v4();
        let writeable1 = &mut BytesMut::new();
        key1.write_to(writeable1);
        1011_i64.write_to(writeable1);
        let entry1 = ReplicaTimestampEntry::read_from(&mut writeable1.to_bytes());

        let key2 = Uuid::new_v4();
        let writeable2 = &mut BytesMut::new();
        key2.write_to(writeable2);
        1111_i64.write_to(writeable2);
        let entry2 = ReplicaTimestampEntry::read_from(&mut writeable2.to_bytes());

        let entries = &[entry, entry1, entry2];

        let mut content = BytesMut::new();
        content.extend_from_slice(&[1, 2, 3, 4]);
        let mut message = Message::new(1, 0, Frame::new(content, DEFAULT_FLAGS));

        encode_replica_timestamp_list(&mut message, entries);

        let mut iter = message.iter();
        iter.next();
        iter.next().expect("should have entries");
    }

    #[test]
    fn test_decode_replica_timestamp_list() {
        let key = Uuid::new_v4();
        let writeable = &mut BytesMut::new();
        key.write_to(writeable);
        1001_i64.write_to(writeable);
        let entry = ReplicaTimestampEntry::read_from(&mut writeable.to_bytes());

        let key1 = Uuid::new_v4();
        let writeable1 = &mut BytesMut::new();
        key1.write_to(writeable1);
        1011_i64.write_to(writeable1);
        let entry1 = ReplicaTimestampEntry::read_from(&mut writeable1.to_bytes());

        let key2 = Uuid::new_v4();
        let writeable2 = &mut BytesMut::new();
        key2.write_to(writeable2);
        1111_i64.write_to(writeable2);
        let entry2 = ReplicaTimestampEntry::read_from(&mut writeable2.to_bytes());

        let entries = &[entry, entry1, entry2];

        let mut content = BytesMut::new();
        content.extend_from_slice(&[1, 2, 3, 4]);
        let mut message = Message::new(1, 0, Frame::new(content, DEFAULT_FLAGS));

        encode_replica_timestamp_list(&mut message, entries);

        let mut iter = message.iter_mut().peekable();
        iter.next();
        let actual = decode_replica_timestamp_list(&mut iter);
        assert_eq!(entries.to_vec(), actual);
    }
}
