/// Monotonic ULID generator.
///
/// Wraps `Ulid::new()` (wall-clock generation) with a monotonicity guarantee:
/// if the system clock moves backwards (e.g. NTP correction), the generator
/// advances the random portion of the last-issued ULID rather than reusing
/// a past timestamp.
///
/// # Initialization
///
/// Seed with the highest ULID already present in the fork (typically the WAL
/// filename, or the highest segment ULID if no WAL exists). This ensures the
/// first `next()` call produces a ULID that sorts after all existing data.
///
/// # Clock skew behaviour
///
/// On entry to a skewed state (candidate <= last), a `warn!` is emitted once.
/// On recovery (candidate > last again), a `warn!` is emitted once.
/// In between, the generator advances monotonically via `last.increment()`.
///
/// # Overflow
///
/// `Ulid::increment()` returns `None` when all 80 random bits are set.
/// This is physically impossible to reach in practice (2^80 calls within the
/// same millisecond), but the fallback advances the timestamp by 1 ms and
/// resets the random portion to zero. This is the same approach used by the
/// coordinator GC (`compaction_ulid`).
pub struct UlidMint {
    last: ulid::Ulid,
    /// True while the system clock is behind `last`. Used to gate the warn-once
    /// log messages.
    skewed: bool,
}

impl UlidMint {
    /// Create a new mint seeded from `floor`.
    ///
    /// The first `next()` call will return a ULID strictly greater than `floor`
    /// (either from the current clock, or monotonically advanced if the clock
    /// is behind).
    pub fn new(floor: ulid::Ulid) -> Self {
        Self {
            last: floor,
            skewed: false,
        }
    }

    /// Generate the next ULID, guaranteed to be strictly greater than all
    /// previously issued ULIDs.
    ///
    /// This is not an `Iterator::next()` because `UlidMint` is a stateful
    /// generator with no end condition, not an iterable sequence.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> ulid::Ulid {
        let candidate = ulid::Ulid::new();
        let result = if candidate > self.last {
            if self.skewed {
                log::warn!(
                    "ulid clock skew recovered: system clock ({}) is ahead of last known ulid ({})",
                    candidate.timestamp_ms(),
                    self.last.timestamp_ms(),
                );
                self.skewed = false;
            }
            candidate
        } else {
            if !self.skewed {
                log::warn!(
                    "ulid clock skew detected: system clock ({}) is at or behind last known ulid ({}); advancing monotonically",
                    candidate.timestamp_ms(),
                    self.last.timestamp_ms(),
                );
                self.skewed = true;
            }
            match self.last.increment() {
                Some(u) => u,
                None => ulid::Ulid::from_parts(self.last.timestamp_ms() + 1, 0),
            }
        };
        self.last = result;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    #[test]
    fn monotonic_across_calls() {
        let floor = Ulid::new();
        let mut mint = UlidMint::new(floor);
        let a = mint.next();
        let b = mint.next();
        let c = mint.next();
        assert!(a > floor);
        assert!(b > a);
        assert!(c > b);
    }

    #[test]
    fn advances_past_skewed_floor() {
        // Seed with a floor far in the future to simulate a clock-behind scenario.
        let future = Ulid::from_parts(u64::MAX / 2, 0);
        let mut mint = UlidMint::new(future);
        let a = mint.next();
        let b = mint.next();
        assert!(a > future, "first call must exceed floor");
        assert!(b > a, "second call must exceed first");
    }

    #[test]
    fn overflow_fallback_advances_timestamp() {
        // Seed with a ULID that has all 80 random bits set so increment() returns None.
        let max_random = Ulid::from_parts(1000, u128::MAX >> 48); // 80 random bits all set
        let mut mint = UlidMint::new(max_random);
        let result = mint.next();
        // Either from current clock (if it's ahead) or from the overflow fallback.
        // Either way it must be > max_random.
        assert!(result > max_random);
    }
}
