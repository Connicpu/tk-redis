use std::borrow::Cow;
use std::io::Write;
use std::ops::Deref;

#[derive(Clone)]
pub struct Arg(Cow<'static, [u8]>);

pub enum Arguments {
    NoArgs,
    One([Arg; 1]),
    Two([Arg; 2]),
    Three([Arg; 3]),
    More(Vec<Arg>),
}

impl Clone for Arguments {
    fn clone(&self) -> Arguments {
        match *self {
            Arguments::NoArgs => Arguments::NoArgs,
            Arguments::One([ref arg0]) => Arguments::One([arg0.clone()]),
            Arguments::Two([ref arg0, ref arg1]) => Arguments::Two([arg0.clone(), arg1.clone()]),
            Arguments::Three([ref arg0, ref arg1, ref arg2]) => {
                Arguments::Three([arg0.clone(), arg1.clone(), arg2.clone()])
            }
            Arguments::More(ref args) => Arguments::More(args.clone()),
        }
    }
}

pub struct Cmd {
    pub command: Arg,
    pub args: Arguments,
}

impl Cmd {
    pub fn new<A: IntoArgs>(cmd: &str, args: A) -> Cmd {
        Cmd {
            command: cmd.into_arg(),
            args: args.into_args(),
        }
    }

    pub fn encode_len(&self) -> usize {
        let header_len = 1 + count_digits(self.arg_count()) + 2;
        let cmd_len = bulk_len(self.command.len());
        let arg_len = self.args.as_ref().iter().map(Arg::encode_len).sum(): usize;
        header_len + cmd_len + arg_len
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        // These are unwrapped because `write!` to a Vec cannot fail.
        write!(buf, "*{}\r\n", self.arg_count()).unwrap();
        self.command.encode(buf);
        for arg in self.args.iter() {
            arg.encode(buf);
        }
    }

    pub fn arg_count(&self) -> usize {
        // 1 for the command itself
        1 + self.args.len()
    }
}

const EMPTY_ARG: Arg = Arg(Cow::Borrowed(b""));
impl Arg {
    pub fn take(&mut self) -> Arg {
        ::std::mem::replace(self, EMPTY_ARG)
    }

    fn encode(&self, buf: &mut Vec<u8>) {
        // These are unwrapped because `write!` to a Vec cannot fail.
        write!(buf, "${}\r\n", self.len()).unwrap();
        buf.extend(self.as_ref());
        buf.extend(b"\r\n");
    }

    fn encode_len(&self) -> usize {
        bulk_len(self.len())
    }
}

impl AsRef<[u8]> for Arg {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for Arg {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Arguments {
    fn take(&mut self) -> Arguments {
        ::std::mem::replace(self, Arguments::NoArgs)
    }

    pub fn push(&mut self, arg: Arg) {
        use self::Arguments::*;
        *self = match self.take() {
            NoArgs => One([arg]),
            One(mut args) => Two([args[0].take(), arg]),
            Two(mut args) => Three([args[0].take(), args[1].take(), arg]),
            Three(mut args) => More(vec![args[0].take(), args[1].take(), args[2].take(), arg]),
            More(mut args) => {
                args.push(arg);
                More(args)
            }
        };
    }
}

static EMPTY_ARGS: [Arg; 0] = [];
impl AsRef<[Arg]> for Arguments {
    fn as_ref(&self) -> &[Arg] {
        match *self {
            Arguments::NoArgs => &EMPTY_ARGS,
            Arguments::One(ref a) => a,
            Arguments::Two(ref a) => a,
            Arguments::Three(ref a) => a,
            Arguments::More(ref a) => a,
        }
    }
}

impl Deref for Arguments {
    type Target = [Arg];
    fn deref(&self) -> &[Arg] {
        self.as_ref()
    }
}

pub trait IntoArg {
    fn into_arg(self) -> Arg;
}

impl IntoArg for Arg {
    fn into_arg(self) -> Arg {
        self
    }
}

impl<'a, T: IntoArg + Clone + 'a> IntoArg for &'a T {
    fn into_arg(self) -> Arg {
        T::into_arg(self.clone())
    }
}

impl<'a> IntoArg for &'a str {
    fn into_arg(self) -> Arg {
        Arg(self.to_string().into_bytes().into())
    }
}

impl IntoArg for String {
    fn into_arg(self) -> Arg {
        Arg(self.into_bytes().into())
    }
}

impl IntoArg for i64 {
    fn into_arg(self) -> Arg {
        Arg(self.to_string().into_bytes().into())
    }
}

impl IntoArg for u64 {
    fn into_arg(self) -> Arg {
        Arg(self.to_string().into_bytes().into())
    }
}

impl IntoArg for i32 {
    fn into_arg(self) -> Arg {
        Arg(self.to_string().into_bytes().into())
    }
}

impl IntoArg for u32 {
    fn into_arg(self) -> Arg {
        Arg(self.to_string().into_bytes().into())
    }
}

impl IntoArg for i16 {
    fn into_arg(self) -> Arg {
        Arg(self.to_string().into_bytes().into())
    }
}

impl IntoArg for u16 {
    fn into_arg(self) -> Arg {
        Arg(self.to_string().into_bytes().into())
    }
}

impl IntoArg for i8 {
    fn into_arg(self) -> Arg {
        Arg(self.to_string().into_bytes().into())
    }
}

impl IntoArg for u8 {
    fn into_arg(self) -> Arg {
        Arg(self.to_string().into_bytes().into())
    }
}

impl IntoArg for isize {
    fn into_arg(self) -> Arg {
        Arg(self.to_string().into_bytes().into())
    }
}

impl IntoArg for usize {
    fn into_arg(self) -> Arg {
        Arg(self.to_string().into_bytes().into())
    }
}

impl IntoArg for f32 {
    fn into_arg(self) -> Arg {
        Arg(self.to_string().into_bytes().into())
    }
}

impl IntoArg for f64 {
    fn into_arg(self) -> Arg {
        Arg(self.to_string().into_bytes().into())
    }
}

impl IntoArg for bool {
    fn into_arg(self) -> Arg {
        Arg(Cow::Borrowed(if self { b"true" } else { b"false" }))
    }
}

pub trait IntoArgs {
    fn into_args(self) -> Arguments;
}

impl<'a, T: IntoArg + Clone + 'a> IntoArgs for &'a [T] {
    fn into_args(self) -> Arguments {
        match self.len() {
            0 => Arguments::NoArgs,
            1 => Arguments::One([IntoArg::into_arg(self[0].clone())]),
            2 => {
                Arguments::Two([IntoArg::into_arg(self[0].clone()),
                                IntoArg::into_arg(self[1].clone())])
            }
            3 => {
                Arguments::Three([IntoArg::into_arg(self[0].clone()),
                                  IntoArg::into_arg(self[1].clone()),
                                  IntoArg::into_arg(self[2].clone())])
            }
            _ => Arguments::More(self.iter().cloned().map(IntoArg::into_arg).collect()),
        }
    }
}

impl IntoArgs for Arguments {
    fn into_args(self) -> Arguments {
        self.clone()
    }
}

impl IntoArgs for () {
    fn into_args(self) -> Arguments {
        Arguments::NoArgs
    }
}

impl<T: IntoArg> IntoArgs for T {
    fn into_args(self) -> Arguments {
        Arguments::One([self.into_arg()])
    }
}

impl<T1, T2> IntoArgs for (T1, T2)
    where T1: IntoArg,
          T2: IntoArg
{
    fn into_args(self) -> Arguments {
        Arguments::Two([self.0.into_arg(), self.1.into_arg()])
    }
}

impl<T1, T2, T3> IntoArgs for (T1, T2, T3)
    where T1: IntoArg,
          T2: IntoArg,
          T3: IntoArg
{
    fn into_args(self) -> Arguments {
        Arguments::Three([self.0.into_arg(), self.1.into_arg(), self.2.into_arg()])
    }
}

impl<T1, T2, T3, T4> IntoArgs for (T1, T2, T3, T4)
    where T1: IntoArg,
          T2: IntoArg,
          T3: IntoArg,
          T4: IntoArg
{
    fn into_args(self) -> Arguments {
        Arguments::More(vec![self.0.into_arg(),
                             self.1.into_arg(),
                             self.2.into_arg(),
                             self.3.into_arg()])
    }
}

impl<T: IntoArg> IntoArgs for Vec<T> {
    fn into_args(self) -> Arguments {
        Arguments::More(self.into_iter().map(IntoArg::into_arg).collect())
    }
}

fn count_digits(mut v: usize) -> usize {
    let mut result = 1;
    loop {
        if v < 10 {
            return result;
        }
        if v < 100 {
            return result + 1;
        }
        if v < 1000 {
            return result + 2;
        }
        if v < 10000 {
            return result + 3;
        }

        v /= 10000;
        result += 4;
    }
}

#[inline]
fn bulk_len(len: usize) -> usize {
    return 1 + count_digits(len) + 2 + len + 2;
}
