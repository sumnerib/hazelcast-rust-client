use bytes::{Bytes, BytesMut, Buf};
use std::collections::LinkedList;
use std::convert::Infallible;
use std::cmp::Ordering;
use std::collections::linked_list::{Iter, IntoIter};
use crate::codec::Writer;

pub(crate) const DEFAULT_FLAGS: u16 = 0;
pub(crate) const END_DATA_STRUCTURE_FLAG: u16 = 1 << 11;
pub(crate) const BEGIN_DATA_STRUCTURE_FLAG: u16 = 1 << 12;
pub(crate) const IS_NULL_FLAG: u16 = 1 << 10;
const HEADER_LENGTH: usize = 18;

pub(crate) struct Message { frames: LinkedList<Frame> }

impl Message {

    pub(crate) fn new(initial_frame: Frame) -> Self {
        let mut frames = LinkedList::new();
        frames.push_back(initial_frame);
        Message { frames }
    }

    pub(crate) fn add(&mut self, frame: Frame) {
        self.frames.push_back(frame);
    }

    pub(crate) fn iter(&self) -> Iter<'_, Frame> {
        self.frames.iter()
    }
}

#[derive(Eq, PartialEq, Debug)]
pub(crate) struct Frame {
    content: BytesMut,
    flags: u16
}

impl Frame {

    pub(crate) fn new(content: BytesMut, flags: u16) -> Self {
        Frame { content, flags}
    }

    pub(crate) fn append_content(&mut self, content: BytesMut) {
        self.content.extend_from_slice(content.as_ref())
    }

    pub(crate) fn initial_frame(flags: u16, request_type: u32,
                            correlation_id: u64) -> Self {

        let mut content = BytesMut::with_capacity(HEADER_LENGTH);
        request_type.write_to(&mut content);
        correlation_id.write_to(&mut content);

        Self::new(content, flags)
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
}

impl From<BytesMut> for Frame {

    fn from(content: BytesMut) -> Self {
        Frame::new(content, DEFAULT_FLAGS)
    }
}

fn is_flag_set(flags: u16, mask: u16) -> bool {
    return (flags & mask) == mask
}

#[cfg(test)]
mod tests {
    
    use super::*;

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
        let mut message = Message::new(Frame::new(content, DEFAULT_FLAGS));
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

        let expected = Frame::new(content, 0xE000);

        assert_eq!(expected, Frame::initial_frame(0xE000, 0x1D0100, 1));
    }
}
