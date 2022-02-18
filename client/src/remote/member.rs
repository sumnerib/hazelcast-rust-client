use std::{
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
};

use derive_more::Display;
use log::debug;
use crate::remote::message::Message;
use crate::remote::message::{Frame, DEFAULT_FLAGS};
use uuid::Uuid;

use crate::{
    messaging::{Address, Request, Response},
    remote::{channel::Channel, CLIENT_TYPE, CLIENT_VERSION, PROTOCOL_VERSION},
    HazelcastClientError::{AuthenticationFailure, CommunicationFailure},
    {Result, TryFrom},
};

#[derive(Display)]
#[display(fmt = "{} - {:?}", address, member_uuid)]
pub(crate) struct Member {
    member_uuid: Uuid,
    cluster_uuid: Uuid,
    address: Address,

    sender: Sender,
}

impl Member {
    pub(in crate::remote) async fn connect(endpoint: &SocketAddr) -> Result<Self> {
        use crate::messaging::authentication::{AuthenticationRequest, AuthenticationResponse, AuthenticationStatus};

        let channel = match Channel::connect(endpoint).await {
            Ok(channel) => channel,
            Err(e) => return Err(CommunicationFailure(e)),
        };
        let sender = Sender::new(channel);

        let request = AuthenticationRequest::new(
            PROTOCOL_VERSION, "dev".to_string(),
            None, None,
            CLIENT_TYPE.to_string(), CLIENT_VERSION.to_string(),
            "hz_client".to_string(), vec![]
        );
        let response: AuthenticationResponse = sender.send(request).await?;
        debug!("Authenticated with member with uuid: {}", response.member_uuid);
        match AuthenticationResponse::status(&response) {
            AuthenticationStatus::Authenticated => Ok(Member {
                member_uuid: response.member_uuid,
                cluster_uuid: response.cluster_id,
                address: response.address().as_ref().expect("missing address!").clone(),
                sender,
            }),
            status => Err(AuthenticationFailure(status.to_string())),
        }
    }

    pub(in crate::remote) async fn send<RQ: Request, RS: Response>(&self, request: RQ) -> Result<RS> {
        self.sender.send(request).await
    }

    pub(in crate::remote) fn address(&self) -> &Address {
        &self.address
    }

    pub(crate) fn member_uuid(&self) -> &Uuid {
        &self.member_uuid
    }
}

impl Eq for Member {}

impl PartialEq for Member {
    fn eq(&self, other: &Self) -> bool {
        self.member_uuid.eq(&other.member_uuid)
    }
}

struct Sender {
    sequencer: AtomicUsize,
    channel: Channel,
}

impl Sender {
    fn new(channel: Channel) -> Self {
        Sender {
            sequencer: AtomicUsize::new(0),
            channel,
        }
    }

    async fn send<RQ: Request, RS: Response>(&self, request: RQ) -> Result<RS> {
        use std::convert::TryInto;

        let id: u64 = self
            .sequencer
            .fetch_add(1, Ordering::SeqCst)
            .try_into()
            .expect("unable to convert!");
        let message = (id, request).into();

        match self.channel.send(message).await {
            Ok(message) => TryFrom::<RS>::try_from(message),
            Err(e) => Err(CommunicationFailure(e)),
        }
    }
}
