extern crate tk_redis;
extern crate tokio_core;
extern crate futures;

use futures::future::Future;
use futures::sync::oneshot;
use std::str::FromStr;
use std::thread;

#[test]
fn connect_to_redis() {
    let info = tk_redis::ConnectionInfo {
        addr: FromStr::from_str("127.0.0.1:6379").unwrap(),
        password: None,
        db: 0,
        max_connections: 8,
    };

    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        let mut core = tokio_core::reactor::Core::new().unwrap();
        tx.complete(core.remote());
        loop {
            core.turn(None);
        }
    });
    let util_remote = rx.wait().unwrap();

    let pool = tk_redis::Pool::new(info);
    let task = pool.pop(util_remote.clone())
        .and_then(|redis| {
            // Delete any existing key
            redis.exec("DEL", "test/key")
        })
        .and_then(|(redis, result)| {
            assert!(result.is_int());
            // Set our key to a value
            redis.exec("SET", ("test/key", "test-value"))
        })
        .and_then(|(redis, result)| {
            assert!(result.is_okay());
            // Get the value back out
            redis.exec("GET", "test/key")
        })
        .map(|(_, result)| {
            // Make sure we got it
            assert_eq!(result.as_str(), Some("test-value"))
        })
        .map_err(|_| ());

    let mut core = tokio_core::reactor::Core::new().unwrap();
    core.run(task).expect("Failed to connect and send commands");
}
