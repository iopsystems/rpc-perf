use std::io::{Error, ErrorKind};
use crate::*;
use net::Events;
use crate::workload::WorkItem;
use async_channel::Receiver;
use net::event::Source;
use std::net::SocketAddr;
use std::collections::VecDeque;
use session::Session;
use net::TcpStream;
use net::Token;
use core::time::Duration;
use protocol_common::{Compose, Parse};
use session::ClientSession;
use slab::Slab;
use net::Poll;
use std::io::Result;

fn map_err(e: std::io::Error) -> Result<()> {
    match e.kind() {
        ErrorKind::WouldBlock => Ok(()),
        _ => Err(e),
    }
}

fn map_result(result: Result<usize>) -> Result<()> {
    match result {
        Ok(0) => Err(Error::new(ErrorKind::Other, "client hangup")),
        Ok(_) => Ok(()),
        Err(e) => map_err(e),
    }
}

pub struct Client<Parser, Request, Response> {
	free_queue: VecDeque<Token>,
	nevent: usize,
	parser: Parser,
	poll: Poll,
	sessions: Slab<ClientSession<Parser, Request, Response>>,
	timeout: Duration,
	work_queue: Receiver<WorkItem>,
	// waker: Arc<Waker>,
}

pub struct ClientBuilder<Parser, Request, Response> {
	free_queue: VecDeque<Token>,
	nevent: usize,
	parser: Parser,
	poll: Poll,
	sessions: Slab<ClientSession<Parser, Request, Response>>,
	timeout: Duration,
	// waker: Arc<Waker>,
}

impl<Parser, Request, Response> ClientBuilder<Parser, Request, Response>
where
    Parser: Clone + Parse<Response>,
    Request: Compose,
{
	pub fn new(parser: Parser, endpoints: &[&SocketAddr]) -> Result<Self> {
		let poll = Poll::new()?;

		// let waker = Arc::new(Waker::from(
        //     ::net::Waker::new(poll.registry(), WAKER_TOKEN).unwrap(),
        // ));

        let nevent = 16384;
        let timeout = Duration::from_millis(1);

        let mut sessions = Slab::new();
        let mut free_queue = VecDeque::new();

        for endpoint in endpoints {
        	// let endpoint = endpoint.to_socket_addrs();
            let stream = TcpStream::connect(**endpoint)?;
            let mut session = ClientSession::new(Session::from(stream), parser.clone());
            let s = sessions.vacant_entry();
            let interest = session.interest();
            session
                .register(poll.registry(), Token(s.key()), interest)
                .expect("failed to register");
            free_queue.push_back(Token(s.key()));
            s.insert(session);
        }

        Ok(Self {
            free_queue,
            nevent,
            parser,
            poll,
            sessions,
            timeout,
            // waker,
        })
	}

	pub fn build(
        self,
        work_queue: Receiver<WorkItem>,
        // signal_queue: Queues<(), Signal>,
    ) -> Client<Parser, Request, Response> {
        Client {
            // backlog: VecDeque::new(),
            work_queue,
            free_queue: self.free_queue,
            nevent: self.nevent,
            parser: self.parser,
            // pending: HashMap::new(),
            poll: self.poll,
            sessions: self.sessions,
            // signal_queue,
            timeout: self.timeout,
            // waker: self.waker,
        }
    }
}

impl<Parser, Request, Response> Client<Parser, Request, Response>
where
    Parser: Parse<Response> + Clone,
    Request: Compose,
{
	fn close(&mut self, token: Token) {
        if self.sessions.contains(token.0) {
            let mut session = self.sessions.remove(token.0);
            let _ = session.flush();
        }
    }

    /// Handle up to one response for a session
    fn read(&mut self, token: Token) -> Result<()> {
        let session = self
            .sessions
            .get_mut(token.0)
            .ok_or_else(|| Error::new(ErrorKind::Other, "non-existant session"))?;

        // fill the session
        map_result(session.fill())?;

        // process up to one request
        match session.receive() {
            Ok((_request, _response)) => {
            	self.free_queue.push_back(token);
                // if let Some(fe_token) = self.pending.remove(&token) {
                //     self.free_queue.push_back(token);
                //     // self.data_queue
                //     //     .try_send_to(0, (request, response, fe_token))
                //     //     .map_err(|_| Error::new(ErrorKind::Other, "data queue is full"))
                // } else {
                //     panic!("corrupted state");
                // }
                Ok(())
            }
            Err(e) => map_err(e),
        }
    }

    fn write(&mut self, token: Token) -> Result<()> {
        let session = self
            .sessions
            .get_mut(token.0)
            .ok_or_else(|| Error::new(ErrorKind::Other, "non-existant session"))?;

        match session.flush() {
            Ok(_) => Ok(()),
            Err(e) => map_err(e),
        }
    }

	pub fn run(&mut self) {
        // these are buffers which are re-used in each loop iteration to receive
        // events and queue messages
        let mut events = Events::with_capacity(self.nevent);
        // let mut messages = Vec::with_capacity(16384);

        while RUNNING.load(Ordering::Relaxed) {
        	if self.poll.poll(&mut events, Some(self.timeout)).is_err() {
                error!("Error polling");
            }

            // let timestamp = Instant::now();

            // process all events
            for event in events.iter() {
                let token = event.token();

                if event.is_error() {
                    // BACKEND_EVENT_ERROR.increment();

                    self.close(token);
                    continue;
                }

                if event.is_writable() {
                    // BACKEND_EVENT_WRITE.increment();

                    if self.write(token).is_err() {
                        self.close(token);
                        continue;
                    }
                }

                if event.is_readable() {
                    // BACKEND_EVENT_READ.increment();

                    if self.read(token).is_err() {
                        self.close(token);
                        continue;
                    }
                }
            }
        }

        

    }
}

