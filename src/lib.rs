//! Matching engine shell around `calvera-books`.

pub mod codec;
pub mod egress;
pub mod engine;
pub mod ingress;
pub mod md_pub;
pub mod spsc;
pub mod types;
pub mod wal;

#[cfg(feature = "itch")]
pub use codec::itch;
#[cfg(feature = "ouch")]
pub use codec::ouch;
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

    #[test]
    fn fill_acks_maker_and_taker_once_on_tape() {
        let dir = std::env::temp_dir().join(format!("me-wal-{}-fill.wal", std::process::id()));
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

        let mut bid = Command::blank(CommandType::Add);
        bid.side = calvera_books::Side::Bid;
        bid.price = calvera_books::Price(100);
        bid.quantity = 10;
        bid.client_fd = 7;
        bid.user_ref = 1;
        let mut ask = Command::blank(CommandType::Add);
        ask.side = calvera_books::Side::Ask;
        ask.price = calvera_books::Price(100);
        ask.quantity = 10;
        ask.client_fd = 8;
        ask.user_ref = 2;

        cmds.push(bid);
        cmds.push(ask);
        cmds.push(Command::poison());
        eng.run(&std::sync::atomic::AtomicBool::new(true));

        let mut private = Vec::new();
        while let Some(ev) = ouch.pop() {
            private.push(ev);
        }
        let mut public = Vec::new();
        while let Some(ev) = itch.pop() {
            public.push(ev);
        }

        let private_trades: Vec<_> = private
            .iter()
            .filter(|e| e.ty == EventType::TradeExecuted)
            .collect();
        assert_eq!(private_trades.len(), 2);
        let mut fds: Vec<i32> = private_trades.iter().map(|e| e.client_fd).collect();
        fds.sort();
        assert_eq!(fds, [7, 8]);

        let public_trades: Vec<_> = public
            .iter()
            .filter(|e| e.ty == EventType::TradeExecuted)
            .collect();
        assert_eq!(public_trades.len(), 1);
        assert_eq!(public_trades[0].trade.quantity, 10);

        let _ = std::fs::remove_file(dir);
    }
}
