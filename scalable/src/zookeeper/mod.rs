use std::{net::SocketAddr, future::Future, borrow::Cow};

use failure::{self, ResultExt};
use tokio::prelude::*;

mod proto;
mod types;
mod error;

use proto::{Enqueuer, Packetizer, Request, Response, ZkError};
use types::{Acl, CreateMode, KeeperState, Stat, WatchedEvent, WatchedEventType};

#[derive(Clone)]
pub struct ZooKeeper<S> {
    connection: Enqueuer,
}

impl<S> ZooKeeper<S> {
    pub fn connect(
        addr: &SocketAddr,
    ) -> impl Future<Item = (Self, impl Stream<Item = WatchedEvent, Error = ()>), Error = failure::Error>
    {
        let (tx, rx) = futures::sync::mpsc::unbounded();
        tokio::net::TcpStream::connect(addr)
            .map_err(failure::Error::from)
            .and_then(move |stream| Self::handshake(stream, tx))
            .map(move |zk| (zk, rx))
    }

    fn handshake<S>(
        stream: S,
        default_watcher: futures::sync::mpsc::UnboundedSender<WatchedEvent>,
    ) -> impl Future<Item = Self, Error = failure::Error>
    where
        S: Send + 'static + AsyncRead + AsyncWrite,
    {
        let request = Request::Connect {
            protocol_version: 0,
            last_zxid_seen: 0,
            timeout: 0,
            session_id: 0,
            passwd: vec![],
            read_only: false,
        };
        eprintln!("about to handshake");

        let enqueuer = Packetizer::new(stream, default_watcher);
        enqueuer
            .enqueue(request)
            .map(move |response| {
                eprintln!("{:?}", response);
                ZooKeeper {
                    connection: enqueuer,
                }
            })
    }

    pub fn create<D, A>(
        self,
        path: &str,
        data: D,
        acl: A,
        mode: CreateMode,
    ) -> impl Future<Item = (Self, Result<String, error::Create>), Error = failure::Error>
    where
        D: Into<Cow<'static, [u8]>>,
        A: Into<Cow<'static, [Acl]>>,
    {
        self.connection
            .enqueue(Request::Create {
                path: path.to_string(),
                data: data.into(),
                acl: acl.into(),
                mode,
            })
            .and_then(move |r| match r {
                Ok(Response::String(s)) => Ok(Ok(s)),
                Ok(_) => unreachable!("got non-string response to create"),
                Err(ZkError::NoNode) => Ok(Err(error::Create::NoNode)),
                Err(ZkError::NodeExists) => Ok(Err(error::Create::NodeExists)),
                Err(ZkError::InvalidACL) => Ok(Err(error::Create::InvalidAcl)),
                Err(ZkError::NoChildrenForEphemerals) => {
                    Ok(Err(error::Create::NoChildrenForEphemerals))
                }
                Err(e) => Err(format_err!("create call failed: {:?}", e)),
            })
            .map(move |r| (self, r))
    }

    pub fn exists(
        self,
        path: &str,
        watch: bool,
    ) -> impl Future<Item = (Self, Option<Stat>), Error = failure::Error> {
        self.connection
            .enqueue(Request::Exists {
                path: path.to_string(),
                watch: if watch { 1 } else { 0 },
            })
            .and_then(|r| match r {
                Ok(Response::Exists { stat }) => Ok(Some(stat)),
                Err(ZkError::NoNode) => Ok(None),
                Err(e) => bail!("exists call failed: {:?}", e),
                _ => {
                    unreachable!("got a non-create response to a create request: {:?}", r);
                }
            })
            .map(move |r| (self, r))
    }

    pub fn delete(
        self,
        path: &str,
        version: Option<i32>,
    ) -> impl Future<Item = (Self, Result<(), error::Delete>), Error = failure::Error> {
        let version = version.unwrap_or(-1);
        self.connection
            .enqueue(Request::Delete {
                path: path.to_string(),
                version: version,
            })
            .and_then(move |r| match r {
                Ok(Response::Empty) => Ok(Ok(())),
                Ok(_) => unreachable!("got non-empty response to delete"),
                Err(ZkError::NoNode) => Ok(Err(error::Delete::NoNode)),
                Err(ZkError::NotEmpty) => Ok(Err(error::Delete::NotEmpty)),
                Err(ZkError::BadVersion) => {
                    Ok(Err(error::Delete::BadVersion { expected: version }))
                }
                Err(e) => Err(format_err!("delete call failed: {:?}", e)),
            })
            .map(move |r| (self, r))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let (zk, w): (ZooKeeper, _) =
            rt.block_on(
                ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap()).and_then(|(zk, w)| {
                    zk.exists("/foo", true)
                        .inspect(|(_, stat)| assert_eq!(stat, &None))
                        .and_then(|(zk, _)| {
                            zk.create(
                                "/foo",
                                &b"Hello world"[..],
                                Acl::open_unsafe(),
                                CreateMode::Persistent,
                            )
                        })
                        .inspect(|(_, ref path)| {
                            assert_eq!(path.as_ref().map(String::as_str), Ok("/foo"))
                        })
                        .and_then(|(zk, _)| zk.exists("/foo", true))
                        .inspect(|(_, stat)| {
                            assert_eq!(stat.unwrap().data_length as usize, b"Hello world".len())
                        })
                        .and_then(|(zk, _)| zk.delete("/foo", None))
                        .inspect(|(_, res)| assert_eq!(res, &Ok(())))
                        .and_then(|(zk, _)| zk.exists("/foo", true))
                        .inspect(|(_, stat)| assert_eq!(stat, &None))
                        .and_then(move |(zk, _)| {
                            w.into_future()
                                .map(move |x| (zk, x))
                                .map_err(|e| format_err!("stream error: {:?}", e.0))
                        })
                        .inspect(|(_, (event, _))| {
                            assert_eq!(
                                event,
                                &Some(WatchedEvent {
                                    event_type: WatchedEventType::NodeCreated,
                                    keeper_state: KeeperState::SyncConnected,
                                    path: String::from("/foo"),
                                })
                            );
                        })
                        .and_then(|(zk, (_, w))| {
                            w.into_future()
                                .map(move |x| (zk, x))
                                .map_err(|e| format_err!("stream error: {:?}", e.0))
                        })
                        .inspect(|(_, (event, _))| {
                            assert_eq!(
                                event,
                                &Some(WatchedEvent {
                                    event_type: WatchedEventType::NodeDeleted,
                                    keeper_state: KeeperState::SyncConnected,
                                    path: String::from("/foo"),
                                })
                            );
                        })
                        .map(|(zk, (_, w))| (zk, w))
                }),
            ).unwrap();

        eprintln!("got through all futures");
        drop(zk); // make Packetizer idle
        rt.shutdown_on_idle().wait().unwrap();
        assert_eq!(w.wait().count(), 0);
    }
}