//! Absolute-deadline pacing for a publisher.
//!
//! Deadlines are computed from the run start rather than from the previous
//! send, so one late wake-up does not push every later message back. A run
//! with no `--rate` has no deadlines at all.

use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub struct Schedule {
    start: Instant,
    interval: Option<Duration>,
}

impl Schedule {
    /// `per_second` is this publisher's share of the run's offered load. A
    /// `None`, zero or negative rate means send as fast as the connection
    /// allows.
    pub fn new(start: Instant, per_second: Option<f64>) -> Schedule {
        let interval = match per_second {
            Some(rate) if rate > 0.0 => Some(Duration::from_secs_f64(1.0 / rate)),
            _ => None,
        };
        Schedule { start, interval }
    }

    /// When message `index` should be sent, or `None` for an unthrottled run.
    pub fn deadline(&self, index: u64) -> Option<Instant> {
        self.interval.map(|i| self.start + i.mul_f64(index as f64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn unthrottled_runs_have_no_deadlines() {
        let schedule = Schedule::new(Instant::now(), None);
        assert_eq!(schedule.deadline(0), None);
        assert_eq!(schedule.deadline(1_000), None);
    }

    #[test]
    fn deadlines_are_absolute_multiples_of_the_interval() {
        let start = Instant::now();
        // 1000 messages per second: one every millisecond.
        let schedule = Schedule::new(start, Some(1000.0));
        assert_eq!(schedule.deadline(0), Some(start));
        assert_eq!(schedule.deadline(1), Some(start + Duration::from_millis(1)));
        assert_eq!(
            schedule.deadline(500),
            Some(start + Duration::from_millis(500))
        );
    }

    #[test]
    fn a_late_message_does_not_shift_the_ones_after_it() {
        // The property that matters: deadline(n) depends only on n, never on
        // when the previous send actually happened.
        let start = Instant::now();
        let schedule = Schedule::new(start, Some(100.0));
        let tenth = schedule.deadline(10).expect("a paced run has deadlines");
        assert_eq!(tenth, start + Duration::from_millis(100));
        assert_eq!(schedule.deadline(10), Some(tenth));
    }

    #[test]
    fn a_rate_of_zero_or_less_is_treated_as_unthrottled() {
        let start = Instant::now();
        assert_eq!(Schedule::new(start, Some(0.0)).deadline(1), None);
        assert_eq!(Schedule::new(start, Some(-1.0)).deadline(1), None);
    }
}
