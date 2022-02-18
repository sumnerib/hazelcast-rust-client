use std::{
    error::Error,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use tokio::{
    net::tcp::{ReadHalf, WriteHalf},
    prelude::*,
    stream::Stream,
    sync::{mpsc, oneshot},
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec, BytesCodec};

use crate::remote::{LENGTH_FIELD_ADJUSTMENT, LENGTH_FIELD_LENGTH, LENGTH_FIELD_OFFSET, PROTOCOL_SEQUENCE};
use crate::remote::message::{Message};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
type Responder = oneshot::Sender<Message>;

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
            while let Some(event) = events.next().await {
                match event {
                    Ok(Event::Egress((message, responder))) => {
                        let mut frames = message.iter().peekable();
                        while let Some(frame) = frames.next() {
                            let is_final = frames.peek().is_none();
                            writer.write(frame.payload(is_final)).await?;
                        }
                        correlations.insert(message.id(), responder);
                    }
                    Ok(Event::Ingress(frame_bytes)) => {
                        let message: Message = frame_bytes.into();
                        match correlations
                            .remove(&message.id())
                            .expect("missing correlation!")
                            .send(message) { _ => {} }  // TODO() handle Err
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        });

        Ok(Channel { egress: sender })
    }

    pub(in crate::remote) async fn send(&self, message: Message) -> Result<Message> {
        let (sender, receiver) = oneshot::channel();
        self.egress.send((message, sender))?;
        Ok(receiver.await?)
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
    ingress: FramedRead<ReadHalf<'a>, BytesCodec>,
}

impl<'a> Events<'a> {
    fn new(messages: mpsc::UnboundedReceiver<(Message, Responder)>, reader: ReadHalf<'a>) -> Self {
        let reader = FramedRead::new(reader, BytesCodec::new());

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
