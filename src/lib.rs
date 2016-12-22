#![feature(type_ascription)]
#![feature(conservative_impl_trait)]
#![feature(slice_patterns)]

#[macro_use]
extern crate log;
extern crate futures;
extern crate tokio_core;
extern crate either;
extern crate spin;
extern crate nodrop;

#[doc(inline)]
pub use cmd::{Arg, Arguments, Cmd, IntoArg, IntoArgs};
#[doc(inline)]
pub use connection::{Connection, ConnectionInfo, Pool};
#[doc(inline)]
pub use value::Value;

#[macro_use]
mod macros;
#[doc(hidden)]
pub mod cmd;
#[doc(hidden)]
pub mod codec;
#[doc(hidden)]
pub mod connection;
#[doc(hidden)]
pub mod value;
