macro_rules! fail {
    ($ek:ident, $msg:expr) => {
        return Err(::std::io::Error::new(::std::io::ErrorKind::$ek, $msg))
    }
}
