use std::{
    error::Error,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes, BytesMut};
use tokio::{
    net::tcp::{ReadHalf, WriteHalf},
    prelude::*,
    stream::Stream,
    sync::{mpsc, oneshot},
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::remote::{LENGTH_FIELD_ADJUSTMENT, LENGTH_FIELD_LENGTH, LENGTH_FIELD_OFFSET, PROTOCOL_SEQUENCE};
use crate::remote::message::{Frame, IS_FINAL_FLAG, Message, is_flag_set};

const NO_CORRELATION: u64 = 0;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
// type Responder = oneshot::Sender<Message>;
type Responder = mpsc::UnboundedSender<Frame>;

enum Event {
    Egress((Message, Responder)),
    Ingress(BytesMut),
}

pub(in crate::remote) struct Channel {
    egress: mpsc::UnboundedSender<(Message, Responder)>,
}

impl Channel {
    pub(in crate::remote) async fn connect(address: &SocketAddr) -> Result<Self> {
        use std::collections::HashMap;
        use tokio::{net::TcpStream, stream::StreamExt};

        let mut stream = TcpStream::connect(address).await?;
        stream.write_all(&PROTOCOL_SEQUENCE).await?;

        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let (reader, writer) = stream.split();
            let mut writer = Writer::new(writer);
            let mut events = Events::new(receiver, reader);

            let mut correlations = HashMap::with_capacity(1024);
            let mut active_correlation = NO_CORRELATION;
            while let Some(event) = events.next().await {
                match event {
                    Ok(Event::Egress((message, responder))) => {
                        let mut frames = message.iter().peekable();
                        while let Some(frame) = frames.next() {
                            let is_final = match frames.peek() {
                                Some(_) => false,
                                None => true,
                            };
                            writer.write(frame.payload(is_final)).await?;
                        }
                        correlations.insert(message.id(), responder);
                    }
                    Ok(Event::Ingress(mut frame_bytes)) => {
                        let frame = Frame::from(
                            frame_bytes,
                            active_correlation == NO_CORRELATION
                        );
                        if frame.is_first {
                            active_correlation = frame.id();
                            match correlations
                                .get(&active_correlation)
                                .expect("missing correlation!")
                                .send(frame) { _ => {} }
                        } else {
                            if is_flag_set(frame.flags, IS_FINAL_FLAG) {
                                match correlations
                                    .remove(&active_correlation)
                                    .expect("missing correlation!")
                                    .send(frame) { _ => {} }
                                active_correlation = NO_CORRELATION;
                            } else {
                                match correlations
                                    .get(&active_correlation)
                                    .expect("missing correlation!")
                                    .send(frame) { _ => {} }
                            }
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        });

        Ok(Channel { egress: sender })
    }

    pub(in crate::remote) async fn send(&self, message: Message) -> Result<Message> {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        self.egress.send((message, sender))?;
        let first_frame = receiver.recv().await.unwrap();
        let mut message = Message::new(
            first_frame.id(),
            first_frame.r#type(),
            first_frame
        );
        while let Some(frame) = receiver.recv().await {
            message.add(frame);
        }
        Ok(message)
    }
}

struct Writer<'a> {
    writer: FramedWrite<WriteHalf<'a>, LengthDelimitedCodec>,
}

impl<'a> Writer<'a> {
    fn new(writer: WriteHalf<'a>) -> Self {
        let writer = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .length_adjustment(LENGTH_FIELD_ADJUSTMENT)
            .little_endian()
            .new_write(writer);

        Writer { writer }
    }

    async fn write(&mut self, frame: Bytes) -> Result<()> {
        use futures::SinkExt;

        Ok(self.writer.send(frame).await?)
    }
}

struct Events<'a> {
    egress: mpsc::UnboundedReceiver<(Message, Responder)>,
    ingress: FramedRead<ReadHalf<'a>, LengthDelimitedCodec>,
}

impl<'a> Events<'a> {
    fn new(messages: mpsc::UnboundedReceiver<(Message, Responder)>, reader: ReadHalf<'a>) -> Self {
        let reader = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .length_adjustment(LENGTH_FIELD_ADJUSTMENT)
            .little_endian()
            .new_read(reader);

        Events {
            egress: messages,
            ingress: reader,
        }
    }
}

impl Stream for Events<'_> {
    type Item = Result<Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.egress).poll_next(cx) {
            Poll::Ready(Some(payload)) => return Poll::Ready(Some(Ok(Event::Egress(payload)))),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => {}
        }

        Poll::Ready(match futures::ready!(Pin::new(&mut self.ingress).poll_next(cx)) {
            Some(Ok(frame)) => Some(Ok(Event::Ingress(frame))),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        })
    }
}
