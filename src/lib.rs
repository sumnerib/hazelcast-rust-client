use std::error::Error;

use crate::protocol::pn_counter::PnCounter;
use crate::remote::connection::Connection;

mod bytes;
mod codec;
mod message;
mod protocol;
mod remote;

pub(crate) type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

pub trait TryFrom<T> {
    type Error;

    fn try_from(self) -> std::result::Result<T, Self::Error>;
}

pub struct HazelcastClient {
    connection: Connection,
}

impl HazelcastClient {
    pub async fn new(address: &str, username: &str, password: &str) -> Result<Self> {
        let connection = Connection::create(address, username, password).await?;

        Ok(HazelcastClient { connection })
    }

    pub fn pn_counter(&mut self, name: &str) -> PnCounter {
        PnCounter::new(name, &mut self.connection)
    }
}

#[cfg(test)]
mod tests {
    use tokio;

    use crate::HazelcastClient;
    use crate::Result;

    #[tokio::test]
    async fn run() -> Result<()> {
        let mut client = HazelcastClient::new("127.0.0.1:5701", "dev", "dev-pass").await?;

        let mut counter = client.pn_counter("my-counter");
        let value = counter.get().await?;
        println!("counter value: {}", value);

        Ok(())
    }
}
