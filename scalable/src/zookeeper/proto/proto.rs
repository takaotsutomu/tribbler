use std::collections::HashMap;
use std::io::{self, Write};

use byteorder::{WriteBytesExt, BigEndian};
use failure;
use tokio::io::*;

mod request;
mod response;

pub(crate) use request::Request;
pub(crate) use response::Response;

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

    // Operation we are waiting a response for.
    reply: HashMap<i32, (request::OpCode, futures::unsync::oneshot::Sender<Response>)>,
}

impl<S> Future for Packetizer<S>
where S: AsyncRead + AsyncWrite
{
    type Item = ();
    type Error = failure::Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        // Have to call both because when both read and write end of the 
        // socekt are ready, if we only call read, we wont be woken up again to
        // write
        let r = self.poll_read()?;
        let w = self.poll_write()?;
        match (r,w) {
            (Async::Ready(()), Async::Ready(())) => Ok(Async::Ready(())),
            (Async::Ready(()), _) => bail!("outstanding requests, but response channle closed"),
            _ => Ok(Async::NotReady),
        }
    }    
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
            last_sent: Default::default(),
        }
    }
    
    fn outlen(&self) -> usize {
        self.outbox.len() - self.outstart;
    }

    fn inlen(&self) -> usize {
        self.inbox.len() - self.instart
    }

    pub fn enqueue(
        &mut self,
        item: Request,
    ) -> impl Future<Item = response::Response, Error = failure::Error> {

        let (tx, rx) = futures::unsync::oneshot::channel();
        let lengthi = self.outbox.len();
        // dummy length
        self.outbox.push(0);
        self.outbox.push(0);
        self.outbox.push(0);
        self.outbox.push(0);

        let xid = self.xid + 1;
        
        self.repy.inser(xid, (item.opcode(), tx));
        self.last_sent.push_back(item.opcode());
        
        if let Request::Connect {} = item {

        } else {
            // xid
            self.outbox
                .write_i32::<BigEndian>(self.xid)
                .expect("Vec::write should never fail");
        }

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
        
        rx
    }

    fn poll_write(&mut self) -> Result<Async<()>, failure::Error> 
    where 
        S: AsyncWrite
    {
        while self.outlen() != 0 {
            let n = match self.stream.write(&self.outbox[self.outstart..])? {
                Async::Ready(n) => n,
                Async::NotReady => return Ok(Async::NotReady)
            };
            self.outstart += n;
            if self.outstart == self.outbox.length() {
                self.outbox.clear();
                self.outstart = 0;
            } else {
                return Ok(Async::NotReady())
            }
        }
        self.stream.poll_flush().map_err(failure::Error::from)
    }

    fn poll_read(&mut self) -> Result<Async<()>, failure::Error> 
    where
        S: AsyncRead
    {
        loop {
            let mut need = if self.inlen() > 4 {
                let length = (&mut &self.inbox[self.instart..]).read_i32::<BigEndian>()? as usize;
                length + 4
            } else {
                4
            };
            while self.inlen() < need {
                let read_from = self.inbox.len();
                self.inbox.resize(read_from + need, 0);
                match self.stream.poll_read(&mut self.inbox[read_from..])? {
                    Async::Ready(n) => {
                        if n == 0 {
                            if self.inlen() != 0 {
                                bail!("connection closed with {} bytes left in buffer", self.inlen());
                            }
                            
                            // End of stream
                            return OK(Async::Ready(()));
                        }
                        self.inbox.truncate(read_from + n);
                        if self.inlen() > 4 && need != 4 {
                            let length = 
                                (&mut &self.inbox[self.instart..]).read_i32::<BigEndian>()? as usize;
                            need += length;
                        }
                    }
                    Async::NotReady => {
                        self.inbox.truncate(read_from);
                        return Ok(Async::NotReady);
                    }
                }
            }
            {
                let mut buf = &self.inbox[self.instart..sefl.instart + need];
                let length = buf.read_i32::<BigEndian>()?;
                let xid = buf.read_i32::<BigEndian>()?;
                
                // find the waiting request future
                let (opcode, tx) = self.reply.remove(xid); // return an error if xid is unknown
    
                let r = Response::parse(opcode, buf)?;
                self.instart += need;
                tx.send(r);
            }
            if self.instart == self.inbox.len() {
                self.inbox.clear();
                sefl.instart = 0;
            }
        }
    }
}

fn wrap<S>(stream: S) -> Packetizer
where
    S: AsyncRead + AsyncWrite,
{
    Packetizer { stream }
}

impl<S> Sink for Packetizer
where
    S: AsyncWrite,
{
    type SinkItem = Request;
    type SinkError = failure::Error;
  
    fn enqueue(
        &mut self,
        item: Request,
    ) -> impl Future<Item = Response, Error = failure::Error> {

        let (tx, rx) = futures::unsync::oneshot::channel();
        let lengthi = self.outbox.len();
        // dummy length
        self.outbox.push(0);
        self.outbox.push(0);
        self.outbox.push(0);
        self.outbox.push(0);

        let xid = unimplementd!();
        self.repy.inser(xid, (item.opcode(), tx));
        self.last_sent.push_back(item.opcode());
        
        if let Request::Connect {} = item {

        } else {
            // xid
            self.outbox
                .write_i32::<BigEndian>(self.xid)
                .expect("Vec::write should never fail");
        }

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
    type Item = Response;
    type Error = failure::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        let mut need = if self.inlen() > 4 {
            let length = self.inbox[self.instart..].read_i32::<BigEndian>();
            length + 4
        } else {
            4
        };
        while self.inlen() < need {
            let read_from = self.inbox.len();
            self.inbox.resize(read_from + need, 0);
            match self.stream.poll_read(&mut self.inbox[read_from..])? {
                Ok(Async::Ready(n)) => {
                    if n == 0 {
                        if self.inlen() != 0 {
                            bail!("connection closed with {} bytes left in buffer", self.inlen());
                        }
                        
                        // End of stream
                        return OK(Async::Ready(None));
                    }
                    self.inbox.truncate(read_from + n);
                    if self.inlen() > 4 && need != 4 {
                        let length = self.inbox[self.instart..].read_i32::<BigEndian>()?;
                    }
                }
                Ok(Async::NotReady) => {
                    self.inbox.truncate(read_from);
                    return Ok(Async::NotReady);
                }
            }
        }
        self.instart += 4;
        let r = Response::parse(&self.inbox[self.instart..(self.instart + need - 4)])?;
        self.instart += need - 4;
        if self.instart == self.inbox.len() {
            self.inbox.clear();
            sefl.instart = 0;
        }
        Ok(Async::Read(Some(r)))
    }
}