pub mod engine;
pub mod ingress;
pub mod itch;
pub mod itch_pub;
pub mod ouch;
pub mod spsc;
pub mod types;
pub mod wal;

#[inline(always)]
pub fn pause() {
    std::hint::spin_loop();
}
