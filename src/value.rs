use std::borrow::Cow;
use std::io;
use std::str::{self, FromStr};

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Nil,
    Int(i64),
    Data(Vec<u8>),
    Bulk(Vec<Value>),
    Status(String),
    Okay,
}

impl Value {
    pub fn is_okay(&self) -> bool {
        *self == Value::Okay
    }

    pub fn is_int(&self) -> bool {
        match *self {
            Value::Int(_) => true,
            _ => false,
        }
    }

    pub fn is_data(&self) -> bool {
        match *self {
            Value::Data(_) => true,
            _ => false,
        }
    }

    pub fn is_bulk(&self) -> bool {
        match *self {
            Value::Bulk(_) => true,
            _ => false,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match *self {
            Value::Int(i) => Some(i),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match *self {
            Value::Data(ref data) => str::from_utf8(data).ok(),
            _ => None,
        }
    }

    pub fn as_str_lossy(&self) -> Option<Cow<str>> {
        match *self {
            Value::Data(ref data) => Some(String::from_utf8_lossy(data)),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self.as_str() {
            Some(s) => f64::from_str(s).ok(),
            None => self.as_i64().map(|i| i as f64),
        }
    }

    pub fn into_string(self) -> Result<String, Value> {
        match self {
            Value::Data(data) => String::from_utf8(data).map_err(|e| Value::Data(e.into_bytes())),
            value => Err(value),
        }
    }

    pub fn parse(buf: &[u8]) -> io::Result<Option<(Value, usize)>> {
        if buf.len() < 1 {
            return Ok(None);
        }

        match buf[0] {
            b'+' => Value::parse_status(buf),
            b':' => Value::parse_int(buf),
            b'$' => Value::parse_data(buf),
            b'*' => Value::parse_bulk(buf),
            b'-' => Value::parse_error(buf),
            _ => fail!(InvalidData, "Expected redis control code"),
        }
    }

    fn parse_status(buf: &[u8]) -> io::Result<Option<(Value, usize)>> {
        if buf.starts_with(b"+OK\r\n") {
            Ok(Some((Value::Okay, 5)))
        } else {
            let newline = match buf.iter().position(|&b| b == b'\r') {
                Some(newline) => newline,
                None => return Ok(None),
            };

            // Make sure the newline exists in the buffer
            let tot_len = newline + 2;
            if buf.len() < tot_len {
                return Ok(None);
            }

            let status = String::from_utf8_lossy(&buf[1..newline]).into_owned();
            Ok(Some((Value::Status(status), tot_len)))
        }
    }

    fn parse_int(buf: &[u8]) -> io::Result<Option<(Value, usize)>> {
        let newline = match buf.iter().position(|&b| b == b'\r') {
            Some(newline) => newline,
            None => return Ok(None),
        };

        // Make sure the newline exists in the buffer
        let tot_len = newline + 2;
        if buf.len() < tot_len {
            return Ok(None);
        }

        let data = match str::from_utf8(&buf[1..newline]) {
            Ok(data) => data,
            Err(_) => fail!(InvalidData, "Invalid int value"),
        };

        match i64::from_str(data) {
            Ok(value) => Ok(Some((Value::Int(value), tot_len))),
            Err(_) => fail!(InvalidData, "Invalid int value"),
        }
    }

    fn parse_data(buf: &[u8]) -> io::Result<Option<(Value, usize)>> {
        let (len, start) = match Value::parse_int(buf) {
            Ok(Some((Value::Int(len), start))) => {
                if len < 0 {
                    return Ok(Some((Value::Nil, start)));
                }
                (len as usize, start)
            }
            Ok(_) => return Ok(None),
            Err(err) => return Err(err),
        };

        // Check the briefcase, we got the good stuff?
        let tot_len = start + len + 2;
        if buf.len() < tot_len {
            return Ok(None);
        }

        Ok(Some((Value::Data(buf[start..start + len].into()), tot_len)))
    }

    fn parse_bulk(buf: &[u8]) -> io::Result<Option<(Value, usize)>> {
        let (count, mut start) = match Value::parse_int(buf) {
            Ok(Some((Value::Int(count), start))) => (count, start),
            Ok(_) => return Ok(None),
            Err(err) => return Err(err),
        };

        if count < 0 {
            return Ok(Some((Value::Nil, start)));
        }

        let mut values = Vec::with_capacity(count as usize);
        for _ in 0..count {
            match Value::parse(&buf[start..])? {
                Some((value, len)) => {
                    values.push(value);
                    start += len;
                }
                None => return Ok(None),
            }
        }

        Ok(Some((Value::Bulk(values), start)))
    }

    fn parse_error(buf: &[u8]) -> io::Result<Option<(Value, usize)>> {
        let newline = match buf.iter().position(|&b| b == b'\r') {
            Some(newline) => newline,
            None => return Ok(None),
        };

        // Make sure the newline exists in the buffer
        let tot_len = newline + 2;
        if buf.len() < tot_len {
            return Ok(None);
        }

        let data = match str::from_utf8(&buf[1..newline]) {
            Ok(data) => data,
            Err(_) => fail!(InvalidData, "Invalid UTF-8"),
        };

        let mut pieces = data.splitn(2, ' ');
        match pieces.next().unwrap() {
            "ERR" => fail!(Other, "Redis Response Error"),
            "EXECABORT" => fail!(Interrupted, "Execution Aborted"),
            "LOADING" => fail!(Other, "Busy Loading"),
            "NOSCRIPT" => fail!(NotFound, "Script not found"),
            err => fail!(Other, format!("Extension error: {}: {:?}", err, pieces.next())),
        }
    }
}

#[cfg(test)]
mod tests {
    use value::*;
    use value::Value::*;

    macro_rules! test_parse{
        ($data:expr, $result:expr) => {
            assert_eq!(Value::parse($data).unwrap(), Some(($result, $data.len())))
        }
    }

    #[test]
    fn test_parse_status() {
        test_parse!(b"+OK\r\n", Okay);
        test_parse!(b"+ThisIsFine\r\n", Status("ThisIsFine".into()));
    }

    #[test]
    fn test_parse_int() {
        test_parse!(b":-1\r\n", Int(-1));
        test_parse!(b":0\r\n", Int(0));
        test_parse!(b":1\r\n", Int(1));
        test_parse!(b":123456789\r\n", Int(123_456_789));
    }

    #[test]
    fn test_parse_data() {
        test_parse!(b"$0\r\n\r\n", Data(vec![]));
        test_parse!(b"$5\r\nHello\r\n", Data("Hello".into()));
        test_parse!(b"$6\r\nWorld!\r\n", Data("World!".into()));
        test_parse!(b"$1\r\n\0\r\n", Data(vec![0]));
    }

    #[test]
    fn test_parse_bulk() {
        test_parse!(b"*-1\r\n", Nil);
        test_parse!(b"*0\r\n", Bulk(vec![]));
        test_parse!(b"*1\r\n:1\r\n", Bulk(vec![Value::Int(1)]));
        test_parse!(b"*1\r\n+OK\r\n", Bulk(vec![Value::Okay]));
        test_parse!(b"*1\r\n*-1\r\n", Bulk(vec![Value::Nil]));
        test_parse!(b"*2\r\n$5\r\nHello\r\n*-1\r\n", Bulk(vec![Data("Hello".into()), Nil]));
    }
}
