//! 5-field crontab parser + next-fire calculator.
//!
//! Exposed as `jl_cron_next_after(expr, from_unix) -> next_unix` via
//! `attach_joblite_functions`. One implementation for every binding —
//! the Python `CronSchedule` class collapses to a marker holding the
//! expression string.
//!
//! Semantics match standard Unix cron (and the previous Python
//! implementation):
//!
//!   * Fields: minute (0-59), hour (0-23), day-of-month (1-31),
//!     month (1-12), day-of-week (0-6, Sunday=0).
//!   * Each field: `*`, `N`, `N-M`, `*/K`, `N-M/K`, or a comma list
//!     of those.
//!   * `next_after(dt)` returns the first boundary STRICTLY AFTER `dt`
//!     at minute precision.
//!   * Calendar arithmetic runs in the SYSTEM LOCAL TIME ZONE — same
//!     as standard cron. Set `TZ=UTC` in the scheduler's environment
//!     if you want UTC boundaries.
//!
//! Implementation strategy: naive minute-by-minute scan, capped at
//! ~5 years of iterations. Simple and good enough — a scheduler tick
//! calls this once per registered task per boundary, not in a hot
//! loop.

use chrono::{Datelike, Duration, Local, NaiveDateTime, TimeZone, Timelike, Weekday};

use std::collections::BTreeSet;

/// Parsed cron expression. Each field is the set of integer values
/// that satisfy that field.
#[derive(Debug, Clone)]
pub struct CronSchedule {
    minutes: BTreeSet<u32>,
    hours: BTreeSet<u32>,
    days: BTreeSet<u32>,
    months: BTreeSet<u32>,
    dows: BTreeSet<u32>,
}

impl CronSchedule {
    pub fn parse(expr: &str) -> Result<Self, String> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(format!(
                "crontab requires 5 fields (minute hour dom month dow); got {}: {:?}",
                parts.len(),
                expr
            ));
        }
        Ok(Self {
            minutes: parse_field(parts[0], 0, 59)?,
            hours: parse_field(parts[1], 0, 23)?,
            days: parse_field(parts[2], 1, 31)?,
            months: parse_field(parts[3], 1, 12)?,
            dows: parse_field(parts[4], 0, 6)?,
        })
    }

    fn matches(&self, dt: &NaiveDateTime) -> bool {
        // chrono Weekday: Mon=0..Sun=6. Cron dow: Sun=0..Sat=6.
        let cron_dow = match dt.weekday() {
            Weekday::Sun => 0,
            Weekday::Mon => 1,
            Weekday::Tue => 2,
            Weekday::Wed => 3,
            Weekday::Thu => 4,
            Weekday::Fri => 5,
            Weekday::Sat => 6,
        };
        self.minutes.contains(&dt.minute())
            && self.hours.contains(&dt.hour())
            && self.days.contains(&(dt.day() as u32))
            && self.months.contains(&(dt.month() as u32))
            && self.dows.contains(&cron_dow)
    }
}

fn parse_field(field: &str, lo: u32, hi: u32) -> Result<BTreeSet<u32>, String> {
    let mut out = BTreeSet::new();
    for part in field.split(',') {
        let (range_part, step) = match part.split_once('/') {
            Some((r, s)) => {
                let step: u32 = s
                    .parse()
                    .map_err(|_| format!("cron step must be a positive integer: {:?}", part))?;
                if step == 0 {
                    return Err(format!("cron step must be positive: {:?}", part));
                }
                (r, step)
            }
            None => (part, 1u32),
        };
        let (start, end) = if range_part == "*" {
            (lo, hi)
        } else if let Some((a, b)) = range_part.split_once('-') {
            let a: u32 = a
                .parse()
                .map_err(|_| format!("cron field {:?} not an integer", part))?;
            let b: u32 = b
                .parse()
                .map_err(|_| format!("cron field {:?} not an integer", part))?;
            (a, b)
        } else {
            let v: u32 = range_part
                .parse()
                .map_err(|_| format!("cron field {:?} not an integer", part))?;
            (v, v)
        };
        if start < lo || end > hi || start > end {
            return Err(format!(
                "cron field {:?} out of range [{},{}] or inverted",
                part, lo, hi
            ));
        }
        let mut v = start;
        while v <= end {
            out.insert(v);
            v = v.saturating_add(step);
            if step == 0 {
                break;
            }
        }
    }
    Ok(out)
}

/// Return the unix timestamp of the next boundary strictly after
/// `from_unix`, at minute precision, in the system local time zone.
pub fn next_after_unix(expr: &str, from_unix: i64) -> Result<i64, String> {
    let sched = CronSchedule::parse(expr)?;
    // Treat `from_unix` as a UTC timestamp, convert to local time,
    // truncate to minute, then increment minute-by-minute until a
    // match. Cap at ~5 years of minutes (degenerate schedules raise).
    let local = match Local.timestamp_opt(from_unix, 0) {
        chrono::LocalResult::Single(t) => t,
        chrono::LocalResult::Ambiguous(t, _) => t,
        chrono::LocalResult::None => {
            return Err(format!("invalid timestamp: {}", from_unix));
        }
    };
    let start_naive = local.naive_local().with_second(0).and_then(|d| d.with_nanosecond(0));
    let Some(start_naive) = start_naive else {
        return Err("failed to truncate to minute".to_string());
    };
    let mut cand = start_naive + Duration::minutes(1);
    let cap = 5 * 366 * 24 * 60;
    for _ in 0..cap {
        if sched.matches(&cand) {
            // Convert back to unix. Local DST ambiguity: take the
            // earlier occurrence (matches chrono's default for
            // Ambiguous); nonexistent (spring-forward gap) → skip
            // forward.
            match Local.from_local_datetime(&cand) {
                chrono::LocalResult::Single(t) => return Ok(t.timestamp()),
                chrono::LocalResult::Ambiguous(t, _) => return Ok(t.timestamp()),
                chrono::LocalResult::None => {}
            }
        }
        cand += Duration::minutes(1);
    }
    Err(format!(
        "no cron match found within 5 years after unix_ts={}: {:?}",
        from_unix, expr
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> i64 {
        Local
            .with_ymd_and_hms(y, mo, d, h, mi, 0)
            .single()
            .unwrap()
            .timestamp()
    }

    #[test]
    fn star_every_minute() {
        // `* * * * *` — next minute boundary.
        let from = ts(2026, 4, 19, 12, 30);
        let got = next_after_unix("* * * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 31));
    }

    #[test]
    fn every_five_minutes() {
        // 12:30 -> 12:35 on `*/5 * * * *`.
        let from = ts(2026, 4, 19, 12, 30);
        let got = next_after_unix("*/5 * * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 35));
    }

    #[test]
    fn nightly_3am() {
        // `0 3 * * *` at 10am today → 3am tomorrow.
        let from = ts(2026, 4, 19, 10, 0);
        let got = next_after_unix("0 3 * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 20, 3, 0));
    }

    #[test]
    fn strictly_after() {
        // On the boundary itself → returns the NEXT boundary.
        let from = ts(2026, 4, 19, 3, 0);
        let got = next_after_unix("0 3 * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 20, 3, 0));
    }

    #[test]
    fn range_with_step() {
        // `0-30/10 * * * *` at 12:05 → 12:10.
        let from = ts(2026, 4, 19, 12, 5);
        let got = next_after_unix("0-30/10 * * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 10));
    }

    #[test]
    fn comma_list() {
        // `0,30 * * * *` at 12:10 → 12:30.
        let from = ts(2026, 4, 19, 12, 10);
        let got = next_after_unix("0,30 * * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 30));
    }

    #[test]
    fn dow_filter() {
        // `0 12 * * 1` (Mondays at noon). 2026-04-19 is a Sunday;
        // next Monday is 2026-04-20.
        let from = ts(2026, 4, 19, 0, 0);
        let got = next_after_unix("0 12 * * 1", from).unwrap();
        assert_eq!(got, ts(2026, 4, 20, 12, 0));
    }

    #[test]
    fn field_count_error() {
        assert!(next_after_unix("* * * *", 0).is_err());
        assert!(next_after_unix("* * * * * *", 0).is_err());
    }

    #[test]
    fn out_of_range_error() {
        assert!(next_after_unix("60 * * * *", 0).is_err());
        assert!(next_after_unix("* 24 * * *", 0).is_err());
        assert!(next_after_unix("* * 0 * *", 0).is_err());
    }

    #[test]
    fn inverted_range_error() {
        assert!(next_after_unix("5-3 * * * *", 0).is_err());
    }

    #[test]
    fn zero_step_error() {
        assert!(next_after_unix("*/0 * * * *", 0).is_err());
    }
}
