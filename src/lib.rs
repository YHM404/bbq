#![feature(allocator_api)]
mod error;
pub use error::Result;
use error::{BlockState, ErrorContext, QueueState};
use std::{
    alloc::{Allocator, Global, Layout},
    ops::Deref,
    ptr::{self, NonNull},
    sync::{atomic::AtomicUsize, Arc},
    thread::sleep,
    time::Duration,
};

const VSN_BIT_LEN: usize = 16;
const OFFSET_BIT_LEN: usize = usize::BITS as usize - VSN_BIT_LEN;

#[derive(Debug)]
struct Block<T> {
    entries: NonNull<T>,
    /// Indicating which location has been allocated by the producers.
    allocated_cursor: Arc<Cursor>,
    /// Indicating which location has been committed by the producers.
    committed_cursor: Arc<Cursor>,
    /// Indicating which location has been reserved by the producers.
    reserved_cursor: Arc<Cursor>,
    /// Indicating which location has been consumed by the producers.
    consumed_cursor: Arc<Cursor>,
    size: usize,
}

impl<T> Block<T> {
    fn init(size: usize) -> Result<Self> {
        let entries_layout = Layout::array::<T>(size)?;
        let entries = Global.allocate_zeroed(entries_layout)?.cast::<T>();

        Ok(Self {
            entries,
            allocated_cursor: Cursor::init_arc(0),
            committed_cursor: Cursor::init_arc(0),
            reserved_cursor: Cursor::init_arc(0),
            consumed_cursor: Cursor::init_arc(size),
            size,
        })
    }

    fn allocate_entry(&self) -> Result<Option<&mut T>> {
        if Cursor::offset(self.allocated_cursor.as_raw()) >= self.size {
            return Ok(None);
        }

        let old = self
            .allocated_cursor
            .inner
            .fetch_add(1 << OFFSET_BIT_LEN, std::sync::atomic::Ordering::SeqCst);
        let old_cursor = Cursor::offset(old);
        if old_cursor >= self.size {
            return Ok(None);
        }

        let entry_ref = unsafe {
            self.entries
                .as_ptr()
                .add(old_cursor)
                .as_mut()
                .with_context(|| "entry ptr is null")?
        };
        Ok(Some(entry_ref))
    }
}

impl<T> Drop for Block<T> {
    fn drop(&mut self) {
        unsafe {
            ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
                self.entries.as_ptr(),
                self.size,
            ));
        }
    }
}

/// The cursor inside is a usize, indicating version + offset,
/// with the version being stored in the last two bits.
#[derive(Debug)]
struct Cursor {
    inner: AtomicUsize,
}

impl Cursor {
    fn init_arc(offset: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: AtomicUsize::new(offset),
        })
    }

    fn new_raw(offset: usize, vsn: usize) -> usize {
        (vsn << OFFSET_BIT_LEN) | offset
    }

    fn fetch_max(&self, raw_val: usize) -> usize {
        self.inner
            .fetch_max(raw_val, std::sync::atomic::Ordering::SeqCst)
    }

    fn as_raw(&self) -> usize {
        self.inner.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[inline]
    fn vsn(raw: usize) -> usize {
        raw >> OFFSET_BIT_LEN
    }

    #[inline]
    fn offset(raw: usize) -> usize {
        raw << VSN_BIT_LEN >> VSN_BIT_LEN
    }
}

#[derive(Debug, Clone)]
pub struct Bbq<T> {
    inner: Arc<BBQInner<T>>,
}

impl<T> Deref for Bbq<T> {
    type Target = BBQInner<T>;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

#[derive(Debug)]
pub struct BBQInner<T> {
    blocks: NonNull<Block<T>>,
    phead_idx: AtomicUsize,
    _chead_idx: AtomicUsize,
    blocks_num: usize,
}

impl<T> Drop for BBQInner<T> {
    fn drop(&mut self) {
        unsafe {
            ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
                self.blocks.as_ptr(),
                self.blocks_num,
            ));
        }
    }
}

impl<T> Bbq<T> {
    pub fn new(block_size: usize, blocks_num: usize) -> Result<Self> {
        let blocks_layout = Layout::array::<Block<T>>(blocks_num)?;
        let blocks = Global.allocate_zeroed(blocks_layout)?.cast::<Block<T>>();

        (0..blocks_num)
            .map(|offset| unsafe {
                blocks.as_ptr().add(offset).write(Block::init(block_size)?);
                Ok(())
            })
            .collect::<Result<_>>()?;

        Ok(Self {
            inner: Arc::new(BBQInner {
                blocks,
                phead_idx: AtomicUsize::new(0),
                _chead_idx: AtomicUsize::new(0),
                blocks_num,
            }),
        })
    }

    fn get_p_head_block(&self) -> Result<(usize, &Block<T>)> {
        let phead_block_idx = self.phead_idx.load(std::sync::atomic::Ordering::SeqCst);
        let phead_block_ref = unsafe {
            self.blocks
                .as_ptr()
                .add(self.phead_idx.load(std::sync::atomic::Ordering::SeqCst) % self.blocks_num)
                .as_ref()
                .context("ptr is null.")?
        };
        Ok((phead_block_idx, phead_block_ref))
    }

    pub fn enqueue(&self, item: T) -> Result<QueueState<T>> {
        loop {
            let (phead_block_idx, phead_block) = self.get_p_head_block()?;
            if let Some(entry_mut_ref) = phead_block.allocate_entry()? {
                *entry_mut_ref = item;
                return Ok(QueueState::Available);
            } else {
                match self.advance_phead(phead_block_idx)? {
                    BlockState::NoEntry => return Ok(QueueState::Busy(item)),
                    BlockState::NotAvailable => return Ok(QueueState::Full(item)),
                    BlockState::Available => continue,
                }
            }
        }
    }

    fn advance_phead(&self, phead_idx: usize) -> Result<BlockState> {
        let phead_block = unsafe {
            self.blocks
                .as_ptr()
                .add((phead_idx + 1) % self.blocks_num)
                .as_ref()
                .context("ptr is null.")
        }?;

        let phead_vsn = Cursor::vsn(phead_block.committed_cursor.as_raw());
        let consumed_raw = phead_block.consumed_cursor.as_raw();
        let consumed_vsn = Cursor::vsn(consumed_raw);
        let consumed_offset = Cursor::offset(consumed_raw);

        if consumed_vsn < phead_vsn
            || (consumed_vsn == phead_vsn && consumed_offset != phead_block.size)
        {
            let reserved_raw = phead_block.reserved_cursor.as_raw();
            let reserved_offset = Cursor::offset(reserved_raw);
            if reserved_offset == consumed_offset {
                return Ok(BlockState::NoEntry);
            } else {
                return Ok(BlockState::NotAvailable);
            }
        }

        phead_block
            .committed_cursor
            .fetch_max(Cursor::new_raw(0, phead_vsn + 1));
        phead_block
            .allocated_cursor
            .fetch_max(Cursor::new_raw(0, phead_vsn + 1));
        self.phead_idx
            .fetch_max(phead_idx + 1, std::sync::atomic::Ordering::SeqCst);

        Ok(BlockState::Available)
    }
}

pub trait BlockingQueue {
    type Item;

    fn push(&self, item: Self::Item) -> Result<()>;

    fn pop(&self) -> Self::Item;
}

const SLEEP_MILLES: u64 = 1000000000;

impl<T> BlockingQueue for Bbq<T> {
    type Item = T;

    fn push(&self, item: Self::Item) -> Result<()> {
        let mut item = item;
        loop {
            match self.enqueue(item)? {
                QueueState::Full(it) => item = it,
                QueueState::Busy(it) => item = it,
                QueueState::Available => return Ok(()),
            }
            // yield thread, stop wasting cpu
            sleep(Duration::from_millis(SLEEP_MILLES));
        }
    }

    fn pop(&self) -> Self::Item {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Bbq, BlockingQueue, Cursor};

    #[test]
    fn test_cursor() {
        let cursor = Cursor::new_raw(4, 11);
        assert_eq!(4, Cursor::offset(cursor));
        assert_eq!(11, Cursor::vsn(cursor));

        let cursor = Cursor::init_arc(0);

        let old = cursor.fetch_max(Cursor::new_raw(1, 0));
        assert_eq!(0, Cursor::offset(old));
        assert_eq!(0, Cursor::vsn(old));

        let old = cursor.fetch_max(Cursor::new_raw(0, 1));
        assert_eq!(1, Cursor::offset(old));
        assert_eq!(0, Cursor::vsn(old));
    }

    #[test]
    fn test_push() {
        let bbq = Bbq::<u64>::new(1, 5).unwrap();
        bbq.push(11).unwrap();
        bbq.push(12).unwrap();
    }
}
