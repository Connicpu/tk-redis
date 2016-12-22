use cmd::Cmd;
use std::io;
use tokio_core::io::{Codec, EasyBuf};
use value::Value;

pub struct RedisCodec;

impl Codec for RedisCodec {
    type In = Value;
    type Out = Cmd;

    fn decode(&mut self, buf: &mut EasyBuf) -> io::Result<Option<Value>> {
        if let Some((value, len)) = Value::parse(buf.as_slice())? {
            buf.drain_to(len);
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn encode(&mut self, cmd: Cmd, buf: &mut Vec<u8>) -> io::Result<()> {
        buf.reserve(cmd.encode_len());
        cmd.encode(buf);
        Ok(())
    }
}
