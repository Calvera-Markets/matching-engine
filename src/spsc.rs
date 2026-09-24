//! Single-producer single-consumer ring, power-of-two capacity.
//! Same role as `rigtorp::SPSCQueuePow2` in ordertruques.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

#[repr(align(64))]
struct CachePadded<T>(T);

pub struct Spsc<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    /// Next slot the consumer will read.
    head: CachePadded<AtomicUsize>,
    /// Next slot the producer will write.
    tail: CachePadded<AtomicUsize>,
}

unsafe impl<T: Send> Send for Spsc<T> {}
unsafe impl<T: Send> Sync for Spsc<T> {}

impl<T> Spsc<T> {
    pub fn new(cap: usize) -> Self {
        assert!(cap.is_power_of_two() && cap > 0);
        let mut buf = Vec::with_capacity(cap);
        for _ in 0..cap {
            buf.push(UnsafeCell::new(MaybeUninit::uninit()));
        }
        Self {
            buf: buf.into_boxed_slice(),
            mask: cap - 1,
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
        }
    }

    pub fn cap(&self) -> usize {
        self.mask + 1
    }

    pub fn push(&self, mut val: T) {
        loop {
            match self.try_push(val) {
                Ok(()) => return,
                Err(back) => {
                    val = back;
                    crate::pause();
                }
            }
        }
    }

    pub fn try_push(&self, val: T) -> Result<(), T> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Acquire);
        if tail.wrapping_sub(head) > self.mask {
            return Err(val);
        }
        unsafe {
            (*self.buf[tail & self.mask].get()).write(val);
        }
        self.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    pub fn pop(&self) -> Option<T> {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let val = unsafe { (*self.buf[head & self.mask].get()).assume_init_read() };
        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        Some(val)
    }
}

impl<T> Drop for Spsc<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn reports_capacity_and_rejects_a_full_ring() {
        let q = Spsc::new(2);
        assert_eq!(q.cap(), 2);
        assert!(q.try_push(1).is_ok());
        assert!(q.try_push(2).is_ok());
        assert_eq!(q.try_push(3).err(), Some(3));
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn push_waits_until_a_slot_frees() {
        let q = Arc::new(Spsc::new(1));
        q.push(1);
        let writer = Arc::clone(&q);
        let handle = thread::spawn(move || writer.push(2));
        thread::sleep(Duration::from_millis(50));
        assert_eq!(q.pop(), Some(1));
        handle.join().unwrap();
        assert_eq!(q.pop(), Some(2));
    }

    #[test]
    fn drop_drains_what_is_left() {
        let q = Spsc::new(2);
        q.push(1);
        drop(q);
    }

    #[test]
    fn capacity_must_be_a_power_of_two() {
        assert!(std::panic::catch_unwind(|| Spsc::<u8>::new(3)).is_err());
    }
}
