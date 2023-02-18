use std::{
    alloc::{Allocator, Global, Layout},
    fmt::Debug,
    ops::Deref,
    ptr::{self, NonNull},
    sync::{atomic::AtomicUsize, Arc},
    thread::sleep,
    time::Duration,
};

use crate::{bbq_trait::BlockingQueue, error::ErrorContext, Result};

const VSN_BIT_LEN: usize = 32;
const OFFSET_BIT_LEN: usize = usize::BITS as usize - VSN_BIT_LEN;

enum EnqueueState<T> {
    Full(T),
    Busy(T),
    Available,
}

enum DequeueState<T> {
    Empty,
    Busy,
    Ok(T),
}

enum PBlockState {
    Available,
    NoEntry,
    NotAvailable,
}

enum CBlockState<T> {
    BlockDone,
    Consumed(T),
    NoEntry,
    NotAvailable,
}

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

impl<T> Debug for Block<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("allocated_cursor", &self.allocated_cursor)
            .field("committed_cursor", &self.committed_cursor)
            .field("reserved_cursor", &self.reserved_cursor)
            .field("consumed_cursor", &self.consumed_cursor)
            .field("size", &self.size)
            .finish()
    }
}

impl<T> Block<T> {
    fn init(size: usize, cursors_offset: usize) -> Result<Self> {
        let entries_layout = Layout::array::<T>(size)?;
        let entries = Global.allocate_zeroed(entries_layout)?.cast::<T>();

        Ok(Self {
            entries,
            allocated_cursor: Cursor::init_arc(cursors_offset, 0),
            committed_cursor: Cursor::init_arc(cursors_offset, 0),
            reserved_cursor: Cursor::init_arc(cursors_offset, 0),
            consumed_cursor: Cursor::init_arc(cursors_offset, 0),
            size,
        })
    }

    fn allocate_entry(&self) -> Result<Option<&mut T>> {
        if Cursor::offset(self.allocated_cursor.as_raw()) >= self.size {
            return Ok(None);
        }

        let old_allocated_cursor_raw = self
            .allocated_cursor
            .inner
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let old_allocated_cursor_offset = Cursor::offset(old_allocated_cursor_raw);
        if old_allocated_cursor_offset >= self.size {
            return Ok(None);
        }

        let entry_ref = unsafe {
            self.entries
                .as_ptr()
                .add(old_allocated_cursor_offset)
                .as_mut()
                .with_context(|| "entry ptr is null")?
        };
        Ok(Some(entry_ref))
    }

    /// Try to reserved an entry in this Block.
    ///
    /// return the offset of reserved if it success.
    fn try_consume_entry(&self) -> Result<CBlockState<T>> {
        loop {
            let reserved_raw = self.reserved_cursor.as_raw();
            // all spaces had been reserved by others consumer.
            if Cursor::offset(reserved_raw) >= self.size {
                return Ok(CBlockState::BlockDone);
            }

            let committed_raw = self.committed_cursor.as_raw();
            // means that there have no space to reserved.
            if Cursor::offset(reserved_raw) == Cursor::offset(committed_raw) {
                return Ok(CBlockState::NoEntry);
            }

            // means that there still have producers are allocating at this block.
            // all consumer actions must execute after there have no executing actions of producer on this block.
            if Cursor::offset(committed_raw) != self.size {
                let allocated_raw = self.allocated_cursor.as_raw();
                if Cursor::offset(allocated_raw) != Cursor::offset(committed_raw) {
                    return Ok(CBlockState::NotAvailable);
                }
            }

            // return the reserved index when it success reserved an entry.
            if self.reserved_cursor.fetch_max(reserved_raw + 1) == reserved_raw {
                let entry = unsafe { self.consume_entry_unchecked(Cursor::offset(reserved_raw))? };
                self.consumed_cursor.fetch_add_offset(1);
                return Ok(CBlockState::Consumed(entry));
            }
        }
    }

    unsafe fn consume_entry_unchecked(&self, entry_offset: usize) -> Result<T> {
        let layout = Layout::new::<T>();
        let entry_ptr = Global.allocate(layout)?.cast::<T>();

        self.entries
            .as_ptr()
            .add(entry_offset)
            .copy_to(entry_ptr.as_ptr(), 1);
        let entry = entry_ptr.as_uninit_mut().assume_init_read();

        Ok(entry)
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
struct Cursor {
    inner: AtomicUsize,
}

impl Cursor {
    fn init_arc(offset: usize, vsn: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: AtomicUsize::new(Self::new_raw(offset, vsn)),
        })
    }

    fn new_raw(offset: usize, vsn: usize) -> usize {
        (vsn << OFFSET_BIT_LEN) | offset
    }

    fn fetch_max(&self, raw_val: usize) -> usize {
        self.inner
            .fetch_max(raw_val, std::sync::atomic::Ordering::SeqCst)
    }

    fn fetch_add_offset(&self, count: usize) -> usize {
        let raw = self
            .inner
            .fetch_add(count, std::sync::atomic::Ordering::SeqCst);
        Cursor::offset(raw)
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

impl Debug for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let raw = self.as_raw();
        f.write_str("{ \n")?;
        f.write_fmt(format_args!("    offset: {}\n", Cursor::offset(raw)))?;
        f.write_fmt(format_args!("    vsn: {}\n", Cursor::vsn(raw)))?;
        f.write_str("}")
    }
}

/// This is a concurrent queue that supports multiple producers and multiple consumers.
///
/// ## Example
/// ```
/// use crate::bbq::Bbq;
/// use crate::bbq::BlockingQueue;
///
/// fn main() {
///     let queue = Bbq::new(100, 100).unwrap();
///
///     // Create four producer threads
///     for i in 0..4 {
///         let q = queue.clone();
///         std::thread::spawn(move || {
///             q.push(i);
///         });
///     }
///
///     // Create four consumer threads
///     for _ in 0..4 {
///         let q = queue.clone();
///         std::thread::spawn(move || {
///             println!("{}", q.pop().unwrap());
///         });
///     }
/// }
#[derive(Debug, Clone)]
pub struct Bbq<T> {
    inner: Arc<BbqInner<T>>,
}

unsafe impl<T> Send for Bbq<T> where T: Send {}
unsafe impl<T> Sync for Bbq<T> where T: Sync {}

impl<T> Deref for Bbq<T> {
    type Target = BbqInner<T>;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl<T> Debug for BbqInner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let blocks = (0..self.blocks_num)
            .map(|i| unsafe { self.blocks.as_ptr().add(i).as_ref() })
            .collect::<Vec<Option<&Block<T>>>>();
        f.debug_struct("BBQInner")
            .field("blocks", &blocks)
            .field("phead_idx", &self.phead_idx)
            .field("chead_idx", &self.chead_idx)
            .field("blocks_num", &self.blocks_num)
            .finish()
    }
}

pub struct BbqInner<T> {
    blocks: NonNull<Block<T>>,
    phead_idx: AtomicUsize,
    chead_idx: AtomicUsize,
    blocks_num: usize,
}

impl<T> Drop for BbqInner<T> {
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

        unsafe {
            blocks.as_ptr().add(0).write(Block::init(block_size, 0)?);
            (1..blocks_num).try_for_each(|offset| {
                blocks
                    .as_ptr()
                    .add(offset)
                    .write(Block::init(block_size, block_size)?);
                Result::Ok(())
            })?;
        }

        Ok(Self {
            inner: Arc::new(BbqInner {
                blocks,
                phead_idx: AtomicUsize::new(0),
                chead_idx: AtomicUsize::new(0),
                blocks_num,
            }),
        })
    }

    fn get_phead_and_block(&self) -> Result<(usize, &Block<T>)> {
        let phead_block_idx = self.phead_idx.load(std::sync::atomic::Ordering::SeqCst);

        let phead_block_ref = unsafe { self.get_block_by_idx(phead_block_idx % self.blocks_num)? };
        Ok((phead_block_idx, phead_block_ref))
    }

    fn get_chead_and_block(&self) -> Result<(usize, &Block<T>)> {
        let chead_block_idx = self.chead_idx.load(std::sync::atomic::Ordering::SeqCst);
        let chead_block_ref = unsafe { self.get_block_by_idx(chead_block_idx % self.blocks_num)? };
        Ok((chead_block_idx, chead_block_ref))
    }

    unsafe fn get_block_by_idx(&self, block_idx: usize) -> Result<&Block<T>> {
        self.blocks
            .as_ptr()
            .add(block_idx)
            .as_ref()
            .context("block ptr is null.")
    }

    fn enqueue(&self, item: T) -> Result<EnqueueState<T>> {
        loop {
            let (phead_block_idx, phead_block) = self.get_phead_and_block()?;
            if let Some(entry_mut_ref) = phead_block.allocate_entry()? {
                *entry_mut_ref = item;
                phead_block.committed_cursor.fetch_add_offset(1);
                return Ok(EnqueueState::Available);
            } else {
                match self.advance_phead(phead_block_idx)? {
                    PBlockState::NoEntry => return Ok(EnqueueState::Busy(item)),
                    PBlockState::NotAvailable => return Ok(EnqueueState::Full(item)),
                    PBlockState::Available => continue,
                }
            }
        }
    }

    fn dequeue(&self) -> Result<DequeueState<T>> {
        loop {
            let (chead_idx, chead_block) = self.get_chead_and_block()?;
            match chead_block.try_consume_entry()? {
                CBlockState::Consumed(entry) => return Ok(DequeueState::Ok(entry)),
                CBlockState::NoEntry => return Ok(DequeueState::Empty),
                CBlockState::NotAvailable => return Ok(DequeueState::Busy),
                CBlockState::BlockDone => {
                    if !self.advance_chead(chead_idx)? {
                        return Ok(DequeueState::Empty);
                    }
                }
            }
        }
    }

    fn advance_phead(&self, phead_idx: usize) -> Result<PBlockState> {
        let phead_block = unsafe { self.get_block_by_idx(phead_idx % self.blocks_num) }?;
        let phead_next_block = unsafe { self.get_block_by_idx((phead_idx + 1) % self.blocks_num) }?;

        let phead_vsn = Cursor::vsn(phead_block.committed_cursor.as_raw());
        let nblk_consumed_raw = phead_next_block.consumed_cursor.as_raw();
        let nblk_consumed_offset = Cursor::offset(nblk_consumed_raw);

        if nblk_consumed_offset == phead_next_block.size {
            phead_next_block
                .committed_cursor
                .fetch_max(Cursor::new_raw(0, phead_vsn + 1));
            phead_next_block
                .allocated_cursor
                .fetch_max(Cursor::new_raw(0, phead_vsn + 1));
            self.phead_idx
                .fetch_max(phead_idx + 1, std::sync::atomic::Ordering::SeqCst);
            return Ok(PBlockState::Available);
        } else {
            let nblk_reserved_raw = phead_next_block.reserved_cursor.as_raw();
            let nblk_reserved_offset = Cursor::offset(nblk_reserved_raw);
            if nblk_reserved_offset == nblk_consumed_offset {
                return Ok(PBlockState::NoEntry);
            } else {
                return Ok(PBlockState::NotAvailable);
            }
        }
    }

    fn advance_chead(&self, chead_idx: usize) -> Result<bool> {
        let chead_block = unsafe { self.get_block_by_idx(chead_idx % self.blocks_num) }?;
        let chead_next_block = unsafe { self.get_block_by_idx((chead_idx + 1) % self.blocks_num) }?;

        let chead_vsn = Cursor::vsn(chead_block.consumed_cursor.as_raw());
        let nblk_committed_vsn = Cursor::vsn(chead_next_block.committed_cursor.as_raw());

        // producer haven't produce on next block.
        // The logic is still right if cancel this check, but maybe causing more competition with producers?
        if nblk_committed_vsn < chead_vsn + 1 {
            return Ok(false);
        }

        chead_next_block
            .consumed_cursor
            .fetch_max(Cursor::new_raw(0, chead_vsn + 1));
        chead_next_block
            .reserved_cursor
            .fetch_max(Cursor::new_raw(0, chead_vsn + 1));
        self.chead_idx
            .fetch_max(chead_idx + 1, std::sync::atomic::Ordering::SeqCst);

        Ok(true)
    }
}

const SLEEP_MILLES: u64 = 10;

impl<T> BlockingQueue for Bbq<T> {
    type Item = T;

    /// Blocking until send this item successful.
    fn push(&self, item: Self::Item) -> Result<()> {
        let mut item = item;
        loop {
            match self.enqueue(item)? {
                EnqueueState::Full(it) => item = it,
                EnqueueState::Busy(it) => item = it,
                EnqueueState::Available => return Ok(()),
            }
            // yield thread, stop wasting cpu
            sleep(Duration::from_millis(SLEEP_MILLES));
        }
    }

    /// Blocking until get a item from this queue.
    fn pop(&self) -> Result<Self::Item> {
        loop {
            if let DequeueState::Ok(item) = self.dequeue()? {
                return Ok(item);
            }
            // yield thread, stop wasting cpu
            sleep(Duration::from_millis(SLEEP_MILLES));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use crate::{bbq_impl::Cursor, Bbq, BlockingQueue};

    #[test]
    fn test_cursor() {
        let cursor = Cursor::new_raw(4, 11);
        assert_eq!(4, Cursor::offset(cursor));
        assert_eq!(11, Cursor::vsn(cursor));

        let cursor = Cursor::init_arc(0, 0);

        let old = cursor.fetch_max(Cursor::new_raw(1, 0));
        assert_eq!(0, Cursor::offset(old));
        assert_eq!(0, Cursor::vsn(old));

        let old = cursor.fetch_max(Cursor::new_raw(0, 1));
        assert_eq!(1, Cursor::offset(old));
        assert_eq!(0, Cursor::vsn(old));
    }

    #[test]
    fn test_push_and_pop() {
        let bbq = Bbq::<u64>::new(2, 3).unwrap();
        bbq.push(11).unwrap();
        bbq.push(12).unwrap();
        bbq.push(13).unwrap();
        bbq.push(14).unwrap();

        bbq.pop().unwrap();
        bbq.pop().unwrap();
        bbq.pop().unwrap();

        bbq.push(15).unwrap();

        bbq.push(16).unwrap();
        bbq.pop().unwrap();

        bbq.push(17).unwrap();
        bbq.push(18).unwrap();
    }

    #[test]
    fn test_push_pop_concurrent() {
        let bbq_1 = Bbq::<u64>::new(1000, 1000).unwrap();
        let bbq_2 = bbq_1.clone();
        let bbq_3 = bbq_1.clone();
        let bbq_4 = bbq_1.clone();
        let bbq_5 = bbq_1.clone();
        let bbq_6 = bbq_1.clone();

        let handle_1 = thread::spawn(move || {
            for i in 0..200_000 {
                bbq_1.push(i).unwrap();
            }
        });

        let handle_2 = thread::spawn(move || {
            for i in 0..200_000 {
                bbq_2.push(i).unwrap();
            }
        });

        let handle_3 = thread::spawn(move || {
            for i in 0..200_000 {
                bbq_3.push(i).unwrap();
            }
        });

        let handle_4 = thread::spawn(move || {
            for _ in 0..200_000 {
                bbq_4.pop().unwrap();
            }
        });

        let handle_5 = thread::spawn(move || {
            for _ in 0..200_000 {
                bbq_5.pop().unwrap();
            }
        });

        let handle_6 = thread::spawn(move || {
            for _ in 0..200_000 {
                bbq_6.pop().unwrap();
            }
        });

        handle_1.join().unwrap();
        handle_2.join().unwrap();
        handle_3.join().unwrap();
        handle_4.join().unwrap();
        handle_5.join().unwrap();
        handle_6.join().unwrap();
    }
}
