//! Reject reason codes carried in `EventReject::reason`.

/// The `(client_fd, user_ref)` key is already live.
pub const DUPLICATE_KEY: u16 = 0;
/// The book has no free slot for the remainder.
pub const SLAB_FULL: u16 = 1;
/// The book reported an internal inconsistency.
pub const BOOK_FAULT: u16 = 2;
/// The journal has no spare and no reserve room; the command was not applied.
pub const JOURNAL_FULL: u16 = 3;
/// The journal failed for another reason; the command was not applied.
pub const JOURNAL_FAULT: u16 = 4;
/// The journal is on its emergency reserve and takes cancels only; the
/// command was not applied.
pub const JOURNAL_CANCEL_ONLY: u16 = 5;
/// Codes at or above this value come from the balance check.
pub const CHECK_BASE: u16 = 0x0100;

/// Reject code for a balance-check refusal.
pub const fn check(code: u8) -> u16 {
    CHECK_BASE + code as u16
}

/// The balance-check code inside `reason`, if the reject came from the check.
pub const fn check_code(reason: u16) -> Option<u8> {
    if reason >= CHECK_BASE && reason - CHECK_BASE <= u8::MAX as u16 {
        Some((reason - CHECK_BASE) as u8)
    } else {
        None
    }
}
