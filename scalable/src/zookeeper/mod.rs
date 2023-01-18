use std::{net::SocketAddr, future::Future};

use failure::{self, ResultExt};
use tokio::prelude::*;

mod proto;

use super::proto::{Enqueuer, Packetizer, Response};

pub struct ZooKeeper<S> {
    connection: Enqueuer,
}

impl<S> ZooKeeper<S> {
    pub fn connect(
        addr: &SocketAddr
    ) -> impl Future<Item = Self, Error = failure::Error> {
        tokio::net::TcpStram::connect(addr)
            .mapp_err(failure::Error::from)
            .and_then(|stream| Self::handshake(stream))
    }

    fn handshake<S>(
        stream: S,
    ) -> impl Future<Item = Self, Error = failure::Error>
    where 
        S: Send + 'static + AsyncRead + AsyncWrite,
    {
        let request = proto::Request::Connect {
            protocol_version: 0,
            last_zxid_seen: 0,
            timeout: 0,
            session_id: 0,
            passwd: vec![],
            read_only: false,
        };
        eprintln!("about to handshake");

        let mut enqueuer = Packetizer::new(stream);
        enqueuer
            .enqueue(request)
            .map(move |response| {
                eprintln!("{:?}", response);
                ZooKeeper { 
                    connection: enqueuer,
                }
            })
    }

    pub fn create(&self,
        path: &str,
        data: &[u8]
    ) -> impl Future<Item = String, Error = failure::Error> {
        self.connection.enqueue(Request::Create {
            path: path.to_string(),
            data: Vec::from(data),
            acl: vec![],
            flags: 0,
        }).map(|r| {
            if let Response::Create { path } = r {
                path
            } else {
                unreachable!("got a non-create response to a create request: {:?}", r);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut rt = tokio::runtime::new().unwrap();
        let zk = rt
            .block_on(ZooKeeper::connect(&"127.0.0.1:2171".parse().unwrap()))
            .and_then(|zk| {
                zk.create("/foo", &[0x42])
                    .inspect(|path| eprintln!("created {}", path))
            })
            .unwrap();
        drop(zk);
        rt.shutdown_on_idle();
    }
}