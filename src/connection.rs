use cmd::{Cmd, IntoArgs};
use codec::RedisCodec;
use futures::{Async, Future, Poll, Sink, StartSend, Stream};
use futures::IntoFuture;
use futures::future::{Either, lazy};
use futures::stream;
use futures::sync::{mpsc, oneshot};
use nodrop::NoDrop;
use spin::Mutex;
use std::cell::RefCell;
use std::io;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_core::io::Framed;
use tokio_core::io::Io;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Remote;
use url::Url;
use value::Value;

pub struct ConnectionInfo {
    pub addr: SocketAddr,
    pub password: Option<String>,
    pub db: u64,
    pub max_connections: usize,
}

impl ConnectionInfo {
    pub fn from_url(url: &Url) -> Option<ConnectionInfo> {
        let addr = match url.to_socket_addrs().ok().and_then(|mut addrs| addrs.next()) {
            Some(addr) => addr,
            None => return None,
        };

        let password = url.password().map(|s| s.to_string());
        let db = url.path()[1..].parse().unwrap_or(0);

        let mut max = 16;
        for (k, v) in url.query_pairs() {
            if k == "max_connections" {
                if let Ok(maxc) = v.parse() {
                    max = maxc;
                    break;
                }
            }
        }

        Some(ConnectionInfo {
            addr: addr,
            password: password,
            db: db,
            max_connections: max,
        })
    }
}

#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
}

impl Pool {
    pub fn new(info: ConnectionInfo) -> Pool {
        let (tx, rx) = mpsc::channel(info.max_connections + 1);
        Pool {
            inner: Arc::new(PoolInner {
                info: info,
                total_created: AtomicUsize::new(0),
                available: QueueReceiver::new(rx),
                return_pile: Mutex::new(tx),
            }),
        }
    }

    pub fn pop(&self, remote: Remote) -> Box<Future<Item = Connection, Error = io::Error>> {
        let pool = self.clone();
        Box::new(lazy(move || pool.do_pop(&remote)))
    }

    fn do_pop(&self, remote: &Remote) -> Box<Future<Item = Connection, Error = io::Error>> {
        match self.inner.available.queue.lock().poll() {
            Ok(Async::Ready(Some(raw_conn))) => {
                let socket = RedisStream::new(raw_conn, self.clone());
                let conn = Connection::build(self.clone(), socket);
                return Box::new(Ok(conn).into_future());
            }
            Ok(Async::Ready(None)) => panic!("Redis Pool left in a bad state"),
            Ok(Async::NotReady) => {
                // We have to construct a new conn or wait for the pool
            }
            Err(_) => panic!("Redis Pool left in a bad state"),
        }

        let count = self.inner.total_created.fetch_add(1, Ordering::SeqCst);
        if count >= self.inner.info.max_connections {
            self.inner.total_created.fetch_sub(1, Ordering::SeqCst);
            let queue = self.inner.available.clone();
            let pool = self.clone();
            let future = queue.into_future()
                .map(move |(raw_conn, _)| {
                    let raw_conn = raw_conn.expect("Redis Pool left in a bad state");
                    let socket = RedisStream::new(raw_conn, pool.clone());
                    Connection::build(pool.clone(), socket)
                })
                .map_err(panic_state);
            return Box::new(future);
        }

        let future = Connection::connect(self.clone(), remote);
        Box::new(future)
    }
}

fn panic_state<T>(_: T) -> io::Error {
    panic!("Redis Pool left in a bad state")
}

struct PoolInner {
    info: ConnectionInfo,
    total_created: AtomicUsize,
    available: QueueReceiver,
    return_pile: Mutex<mpsc::Sender<RawConn>>,
}

pub struct Connection {
    inner: NoDrop<(Pool, RedisStream)>,
}

impl Connection {
    fn connect(pool: Pool, remote: &Remote)
               -> impl Future<Item = Connection, Error = io::Error> + 'static {
        let passwd = pool.inner.info.password.clone();
        let db = pool.inner.info.db;
        let (tx, rx) = oneshot::channel();
        let remote_pool = pool.clone();
        remote.spawn(move |handle| {
            TcpStream::connect(&remote_pool.inner.info.addr, handle)
                .and_then(move |stream| {
                    let socket = stream.framed(RedisCodec);
                    // Authenticate if we need to
                    if let Some(passwd) = passwd {
                        let future = socket.send(Cmd::new("AUTH", &passwd))
                            .and_then(|socket| socket.into_future().map_err(|(err, _)| err))
                            .and_then(|(value, socket)| match value {
                                Some(Value::Okay) => Ok(socket),
                                _ => fail!(ConnectionRefused, "Invalid redis password"),
                            });
                        Either::A(future)
                    } else {
                        Either::B(Ok(socket).into_future())
                    }
                })
                .and_then(move |socket| {
                    // Try to select the database!
                    if db != 0 {
                        let future = socket.send(Cmd::new("SELECT", &db))
                            .and_then(|socket| socket.into_future().map_err(|(err, _)| err))
                            .and_then(|(value, socket)| match value {
                                Some(Value::Okay) => Ok(socket),
                                _ => fail!(ConnectionRefused, "Invalid redis database"),
                            });
                        Either::A(future)
                    } else {
                        Either::B(Ok(socket).into_future())
                    }
                })
                .then(|result| {
                    tx.complete(result);
                    Ok(())
                })
        });
        rx.then(|result| match result {
            Ok(Ok(socket)) => {
                let socket = RefStream::new(socket, pool.clone());
                Ok(Connection::build(pool, socket))
            }
            Ok(Err(err)) => Err(err),
            Err(_) => fail!(Other, "Connection cancelled"),
        })
    }

    pub fn build(pool: Pool, socket: RedisStream) -> Self {
        Connection { inner: NoDrop::new((pool, socket)) }
    }

    /// Execute a single redis command and get its result value
    pub fn exec_cmd(self, cmd: Cmd) -> impl Future<Item = (Connection, Value), Error = io::Error> {
        let (pool, socket) = self.take();
        socket.send(cmd)
            .and_then(move |socket| socket.into_future().map_err(|(err, _)| err))
            .and_then(move |(value, socket)| match value {
                Some(value) => Ok((Connection::build(pool, socket), value)),
                None => fail!(ConnectionAborted, "Connection ended"),
            })
    }

    pub fn exec<T: IntoArgs>(self, cmd: &str, args: T)
                             -> impl Future<Item = (Connection, Value), Error = io::Error> {
        self.exec_cmd(Cmd::new(cmd, args))
    }

    /// Execute a series of redis commands and get back the results all at once
    pub fn exec_all(self, cmds: Vec<Cmd>)
                    -> impl Future<Item = (Connection, Vec<Value>), Error = io::Error> {
        let socket = self.clone_socket();
        let count = cmds.len() as u64;
        stream::iter(cmds.into_iter().map(Ok))
            .forward(socket)
            .and_then(move |(_, socket)| socket.take(count).collect())
            .map(move |results| (self, results))
    }

    fn take(mut self) -> (Pool, RedisStream) {
        unsafe {
            let pool = ::std::ptr::read(&mut self.inner.0);
            let socket = ::std::ptr::read(&mut self.inner.1);
            ::std::mem::forget(self);
            (pool, socket)
        }
    }

    /// This is okay because the insides are NoDrop
    fn ref_take(&mut self) -> (Pool, RedisStream) {
        unsafe {
            let pool = ::std::ptr::read(&mut self.inner.0);
            let socket = ::std::ptr::read(&mut self.inner.1);
            ::std::mem::forget(self);
            (pool, socket)
        }
    }

    fn clone_socket(&self) -> RedisStream {
        self.inner.1.clone()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let (pool, socket) = self.ref_take();
        if let Ok(socket) = Rc::try_unwrap(socket.extract()) {
            let socket = socket.into_inner();
            pool.inner
                .return_pile
                .lock()
                .start_send(socket)
                .expect("Bug in the redis Pool");
        } else {
            error!("There's still additional references to the \
                    Connection stream after it's being dropped");
        }
    }
}

pub type RawConn = Framed<TcpStream, RedisCodec>;
pub type RedisStream = RefStream<RawConn>;

pub struct RefStream<S> {
    stream: Rc<RefCell<S>>,
    pool: Option<Pool>,
}

impl<S> RefStream<S> {
    pub fn new(stream: S, pool: Pool) -> Self {
        RefStream {
            stream: Rc::new(RefCell::new(stream)),
            pool: Some(pool),
        }
    }

    fn extract(mut self) -> Rc<RefCell<S>> {
        unsafe {
            let s = ::std::ptr::read(&mut self.stream);
            ::std::mem::forget(self);
            s
        }
    }
}

impl<S> Drop for RefStream<S> {
    fn drop(&mut self) {
        if let Some(ref pool) = self.pool {
            pool.inner.total_created.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl<S> Clone for RefStream<S> {
    fn clone(&self) -> Self {
        RefStream {
            stream: self.stream.clone(),
            pool: None,
        }
    }
}

impl<S: Stream> Stream for RefStream<S> {
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.stream.borrow_mut().poll()
    }
}

impl<S: Sink> Sink for RefStream<S> {
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    fn start_send(&mut self, item: S::SinkItem) -> StartSend<S::SinkItem, S::SinkError> {
        self.stream.borrow_mut().start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), S::SinkError> {
        self.stream.borrow_mut().poll_complete()
    }
}

#[derive(Clone)]
struct QueueReceiver {
    queue: Arc<Mutex<mpsc::Receiver<RawConn>>>,
}

impl QueueReceiver {
    fn new(rx: mpsc::Receiver<RawConn>) -> Self {
        QueueReceiver { queue: Arc::new(Mutex::new(rx)) }
    }
}

impl Stream for QueueReceiver {
    type Item = RawConn;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.queue.lock().poll()
    }
}
