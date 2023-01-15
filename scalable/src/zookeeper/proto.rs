use std::io::{self, Write};

use byteorder::{WriteBytesExt, BigEndian};
use failure;
use tokio::io::*;

#[repr(i32)]
enum OpCode {
    Notification = 0,
    Create = 1,
    Delete = 2,
    Exists = 3,
    GetData = 4,
    SetData = 5,
    GetACL = 6,
    SetACL = 7,
    GetChildren = 8,
    Sync = 9,
    Ping = 11,
    GetChildren2 = 12,
    Check = 13,
    Multi = 14,
    Create2 = 15,
    Reconfig = 16,
    CheckWatches = 17,
    RemoveWatches = 18,
    CreateContainer = 19,
    DeleteContainer = 20,
    CreateTTL = 21,
    MultiRead = 22,
    Auth = 100,
    SetWatches = 101,
    Sasl = 102,
    GetEphemerals = 103,
    GetAllChildrenNumber = 104,
    SetWatches2 = 105,
    AddWatch = 106,
    WhoAmI = 107,
    CreateSession = -10,
    CloseSession = -11,
    Error = -1,
}

pub(crate) struct Packetizer<S> {
    stream: S,

    // Bytes that has not yet be set.
    outbox: Vec<u8>,

    // Prefix of outbox that has been sent.
    outstart: usize,

    // Bytes that has not yet be deserialized.
    inbox: Vec<u8>,

    // Prefix of inbox that has been sent.
    instart: usize,

    // Connection ID for the wrapped stream.
    xid: i32,
}

impl<S> Packetizer<S> {
    pub(crate) fn new(stream: S) -> Packetizer<S> {
        Packetizer {
            stream,
            outbox: Vec::new(),
            outstart, 0,
            inbox: Vec::new(),
            instart: 0,
            xid: 0,
        }
    }
}

fn wrap<S>(stream: S) -> Packetizer
where
    S: AsyncRead + AsyncWrite,
{
    Packetizer { stream }
}

pub(crate) enum Request {
    Connect {
        protocol_version: i32,
        last_zxid_seen: i64,
        timeout: i32,
        session_id: i64,
        passwd: Vec<u8>,
        read_only: bool,
    },
}

impl Request {
    fn serialize_into(&self, buffer: &mut Vec<u8>) -> Result<(), io::Error> {
        match *self {
            Request::Connect { 
                protocol_version,
                last_zxid_seen,
                timeout,
                session_id,
                passwd,
                read_only,
            } => {
                writer.write_i32::<BigEndian>(protocol_version)?;
                writer.write_i64::<BigEndian>(last_zxid_seen)?;
                writer.write_i32::<BigEndian>(timeout)?;
                writer.write_i64::<BigEndian>(session_id)?;
                writer.write_i32::<BigEndian>(passwd.len() as i32)?;
                writer.write_all(passwd)?;
                writer.write_u8(read_only as u8);
                Ok(())
            }
    }
}

impl<S> Sink for Packetizer
where
    S: AsyncWrite,
{
    type SinkItem = Request;
    type SinkError = failure::Error;
  
    fn start_send(
        &mut self,
        item: Self::SinkItem,
    ) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        let lengthi = self.outbox.len();
        // dummy length
        self.outbox.push(0);
        self.outbox.push(0);
        self.outbox.push(0);
        self.outbox.push(0);
        
        if let Request::Connect {}
        // xid
        self
            .outbox
            .write_i32::<BigEndian>(0)
            .expect("Vec::write should never fail");

        // type and payload
        item.serialize_into(&mut self.outbox).expect("Vec::write should never fail");
        self
            .outbox
            .write_i32::<BigEndian>(self.xid)
            .expect("Vec::write should never fail");
        

        // Set true length
        let written = self.outbox.len() - lengthi - 4;
        let length = &mut self.outbox[lengthi..lengthi + 4];
        length
            .write_i32::<BigEndian>(written)
            .expect("Vec::write should never fail");
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        let n = try_ready!(self.stream.write(&self.outbox[self.outstart..]));
        self.outstart += n;
        if self.outstart == self.outbox.length() {
            self.outbox.clear();
            self.outstart = 0;
        } else {
            return Ok(Async::NotReady())
        }
        self.stream.poll_flush()
    }
}

impl<S> Stream for Packetizer<S> where S: AsyncRead {
    type Item = Responese;
    type Error = failure::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>, Selft::Error> {
        while self.inbox.len() - self.instart < 4 {
            let n = 
        }
    }
}