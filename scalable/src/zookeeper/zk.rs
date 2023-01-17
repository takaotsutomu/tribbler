use std::net::SocketAddr;

use failure::{self, ResultExt};
use tokio::prelude::*;

use super::proto::Packetizer;

pub struct Zookeeper<S> {
    connection: Packetizer<S>,
}

impl<S> Zookeeper<S> {
    pub fn connect(
        addr: &SocketAddr
    ) -> impl Future<Item = Zookeeper<tokio::net::TcpStream>, Error = failure::Error> {
        tokio::net::TcpStram::connect(addr).and_then(|stream| Self::handshake(stream))
    }

    fn handshake(
        stream: S,
    ) -> impl Future<Item = Self, Error = failure::Error> {
        let request = proto::Request::Connect {
            protocol_version: 0,
            last_zxid_seen: 0,
            timeout: 0,
            session_id: 0,
            passwd: vec![],
            read_only: 0,
        };
        Packetizer::new(stream)
            .send(request) // this is sink to be changed 
            .and_then(|zk| zk.into_future())
            .and_then(|(response, zk)| {
                if response.is_none() {
                    todo!();
                }

                Zookeeper {
                    connection: zk,
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let zk = tokio::run(Zookeeper::connect());
    }
}
