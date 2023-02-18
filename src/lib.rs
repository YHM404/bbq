#![feature(allocator_api)]
#![feature(ptr_as_uninit)]

mod bbq_impl;
mod bbq_trait;
mod error;
pub use bbq_impl::Bbq;
pub use bbq_trait::BlockingQueue;
pub use error::{Error, ErrorKind, Result};
