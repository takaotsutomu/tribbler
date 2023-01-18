use std::collections::HashMap;

use byteorder::{WriteBytesExt, BigEndian, ReadBytesExt};
use failure;
use futures::{sync::{mpsc, oneshot}, future::Either}; // future sync allows multiple threads to communicate with the Zookeerp at the same time
use tokio::prelude::*;
use tokio;

mod request;
mod response;

pub(crate) use request::Request;
pub(crate) use response::Response;

pub(crate) struct Enqueuer(mpsc::UnboundedSender<(Request, oneshot::Sender<Response>)>);

impl Enqueuer {
    // TODO: maybe:
    // fn enqueue<Req, Res>(&slef, req; Req) 0> impl Furutre(Item = Res) where Req: Returns
    pub(crate) fn enqueue(&self, req: Resuest) -> impl Future<Item = Response, Error = failure::Error> {
        let(tx, rx) = oneshot::channel();
        match self.0
            .unbounded_send((req, tx))
            {
                Ok(()) => {
                    Either::A(rx.map_err(|e| format_err!("failed to enqueue new request: {:?}", e)))
                }
                Err(e) => {
                    Either::B(Err(format_err!("failed to enqueue new request: {:?}", e)).into_future())
                }
            }
    }
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

    // Operation we are waiting a response for.
    reply: HashMap<i32, (request::OpCode, oneshot::Sender<Response>)>,

    // Next xid to issue.
    xid: i32,

    // Incomming requests.
    rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Response>)>,

    exiting: bool,
}

impl<S> Packetizer<S> {
    pub(crate) fn new(stream: S) -> Enqueuer
    where 
        S: Send + 'static + AsyncRead + AsyncWrite,
    {
        // TODO: document that it calls tokio::spawn
        let (tx, rx) = mpsc::unbounded();

        tokio::spawn(Packetizer {
            stream,
            outbox: Vec::new(),
            outstart: 0,
            inbox: Vec::new(),
            instart: 0,
            reply: Default::default(),
            xid: 0,
            rx,
            exiting: false,
        }).map_err(|e| {
            // TODO: expose this error to the user somehow
            eprintln!("packetizer exiting: {:?}", e);
            drop(e)
        });

        Enqueuer(tx)
    }
}

impl<S> Packetizer<S> {
    fn outlen(&self) -> usize {
        self.outbox.len() - self.outstart
    }

    fn inlen(&self) -> usize {
        self.inbox.len() - self.instart
    }

    fn poll_enqueue(&mut self) -> Result<Async<()>, ()> {
        loop {
            let (item, tx)  = match try_ready!(self.rx.poll()) {
                Ok((item, tx)) => (item, tx),
                None => return Err(())
            }; // returns error if there are no more requests coming
            let (tx, rx) = futures::unsync::oneshot::channel();
            let lengthi = self.outbox.len();
            // dummy length
            self.outbox.push(0);
            self.outbox.push(0);
            self.outbox.push(0);
            self.outbox.push(0);

            let xid = self.xid;
            self.xid += 1;
            self.reply.insert(xid, (item.opcode(), tx));
            self.last_sent.push_back(item.opcode());
            
            if let Request::Connect {..} = item {
            } else {
                // xid
                self.outbox
                    .write_i32::<BigEndian>(self.xid)
                    .expect("Vec::write should never fail");
            }

            // type and payload
            item.serialize_into(&mut self.outbox)
                .expect("Vec::write should never fail");

            // Set true length
            let written = self.outbox.len() - lengthi - 4;
            let mut length = &self.outbox[lengthi..lengthi + 4];
            length
                .write_i32::<BigEndian>(written as i32)
                .expect("Vec::write should never fail");
        }
    }

    fn poll_write(&mut self) -> Result<Async<()>, failure::Error> 
    where 
        S: AsyncWrite
    {
        while self.outlen() != 0 {
            let n = try_read!(self.stream.write(&self.outbox[self.outstart..]));
            self.outstart += n;
            if self.outstart == self.outbox.outlen() {
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
                        if self.inlen() >= 4 && need == 4 {
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
                let mut buf = &self.inbox[self.instart + 4..self.instart + need];
                let xid = if self.first {
                    0
                } else {
                    let xid = buf.read_i32::<BigEndian>()?;
                    let _zxid = buf.read_i64::<BigEndian>()?;
                    let _err = buf.read_i32::<BigEndian>()?;

                };
                self.first = false;
                
                // find the waiting request future
                let (opcode, tx) = self.reply.remove(&xid); // return an error if xid is unknown
    
                let r = Response::parse(opcode, buf)?;
                self.instart += need;
                tx.send(r).is_ok(); // If the receiver doesn't care, we don't either
            }

            if self.instart == self.inbox.len() {
                self.inbox.clear();
                sefl.instart = 0;
            }
        }
    }
}

impl<S> Future for Packetizer<S>
where S: AsyncRead + AsyncWrite
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        eprintln!("packetizer polled");
        if !self.exiting {
            let e = match self.poll_enqueue() {
                Ok(_) => {},
                Err(()) => {
                    // no more requests will be enqueued
                    unimplemented!();
                },
            };
        }
        
        // Have to call both because when both read and write end of the 
        // socekt are ready, if we only call read, we wont be woken up again to
        // write
        let r = self.poll_read()?;
        let w = self.poll_write()?;
        match (r,w) {
            (Async::Ready(()), Async::Ready(())) if self.exiting => {
                eprintln!("packetizer polled"); 
                Ok(Async::Ready(()))
            },
            (Async::Ready(()), Async::Ready(())) => Ok(Async::NotReady(())),
            (_, Async::Ready(())) if self.exiting => {
                Ok(Async::NotReady)
            }
            (Async::Ready(()), _) => bail!("outstanding requests, but response channle closed"),
            
            _ => Ok(Async::NotReady),
        }
    }    
}
