use std::{
    alloc::{AllocError, LayoutError},
    convert::Infallible,
    fmt::Display,
};

use anyhow::anyhow;

pub enum QueueState<T> {
    Full(T),
    Busy(T),
    Available,
}

pub enum BlockState {
    Available,
    NoEntry,
    NotAvailable,
}

#[derive(Debug, Clone, Copy)]
pub enum ErrorKind {
    BlockDone,
    Empty,
    Memory,
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    inner: anyhow::Error,
}

impl Error {
    pub fn new(kind: ErrorKind, inner: anyhow::Error) -> Self {
        Self { kind, inner }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn into_inner(self) -> anyhow::Error {
        self.inner
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<AllocError> for Error {
    fn from(value: AllocError) -> Self {
        Error::new(ErrorKind::Memory, anyhow!(value))
    }
}

impl From<LayoutError> for Error {
    fn from(value: LayoutError) -> Self {
        Error::new(ErrorKind::Memory, anyhow!(value))
    }
}
pub(crate) trait ErrorContext<T, E> {
    type Output;

    /// Wrap the error value with additional context.
    fn context<C>(self, context: C) -> Self::Output
    where
        C: Display + Send + Sync + 'static;

    /// Wrap the error value with additional context that is evaluated lazily
    /// only once an error does occur.
    fn with_context<C, F>(self, f: F) -> Self::Output
    where
        C: Display + Send + Sync + 'static,
        F: FnOnce() -> C;
}

impl<T> ErrorContext<T, Infallible> for Option<T> {
    type Output = Result<T>;

    fn context<C>(self, context: C) -> Self::Output
    where
        C: Display + Send + Sync + 'static,
    {
        self.ok_or_else(|| Error::new(ErrorKind::Empty, anyhow!(context.to_string())))
    }

    fn with_context<C, F>(self, context: F) -> Self::Output
    where
        C: Display + Send + Sync + 'static,
        F: FnOnce() -> C,
    {
        self.ok_or_else(|| Error {
            kind: ErrorKind::Empty,
            inner: anyhow!(context().to_string()),
        })
    }
}
