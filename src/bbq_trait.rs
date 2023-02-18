use crate::Result;

pub trait BlockingQueue {
    type Item;

    fn push(&self, item: Self::Item) -> Result<()>;

    fn pop(&self) -> Result<Self::Item>;
}
