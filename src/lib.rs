//! Matching engine shell around `calvera-books`.

pub mod codec;
pub mod egress;
pub mod engine;
pub mod ingress;
pub mod itch;
pub mod itch_pub;
pub mod ouch;
pub mod spsc;
pub mod types;
pub mod wal;

pub use engine::MatchingEngine;
pub use spsc::Spsc;
pub use types::{Command, CommandType, Event, EventType};

#[inline(always)]
pub fn pause() {
    std::hint::spin_loop();
}

pub fn pin_to_cpu(cpu: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        let _ = libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set);
    }
    let _ = cpu;
}

pub fn lock_memory() {
    let _ = unsafe { libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn spsc_push_pop() {
        let q = Spsc::new(8);
        q.push(1u32);
        q.push(2);
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn engine_add_then_cancel() {
        let dir = std::env::temp_dir().join(format!("me-wal-{}.wal", std::process::id()));
        let cmds = Arc::new(Spsc::new(16));
        let ouch = Arc::new(Spsc::new(16));
        let itch = Arc::new(Spsc::new(16));
        let mut eng = MatchingEngine::with_wal_size(
            cmds.clone(),
            ouch.clone(),
            itch.clone(),
            &dir,
            1024,
            64 * 1024,
        )
        .unwrap();

        let mut cl = [b' '; 14];
        cl[0] = b'A';
        cmds.push(Command {
            ty: CommandType::Add,
            side: calvera_books::Side::Bid,
            price: calvera_books::Price(100),
            quantity: 10,
            client_fd: 7,
            user_ref: 1,
            cl_ord_id: cl,
            time_in_force: 0,
            display: 0,
            capacity: 0,
        });
        cmds.push(Command::poison());
        let run = std::sync::atomic::AtomicBool::new(true);
        eng.run(&run);

        let ev = ouch.pop().expect("accepted");
        assert_eq!(ev.ty, EventType::OrderAccepted);
        assert_eq!(ev.order.quantity, 10);

        let _ = std::fs::remove_file(dir);
    }
}
