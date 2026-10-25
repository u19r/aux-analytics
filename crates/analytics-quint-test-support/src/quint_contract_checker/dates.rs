use std::time::{SystemTime, UNIX_EPOCH};

use crate::constants::PHASES;

#[derive(Debug, Clone)]
pub(super) enum Expiry {
    Date { value: String, day: i64 },
    Phase { value: String, index: usize },
}

pub(super) fn phase_index(phase: &str) -> Option<usize> {
    PHASES.iter().position(|candidate| *candidate == phase)
}

pub(crate) fn parse_iso_date(value: &str) -> Option<i64> {
    let valid_shape = value.len() == 10
        && value.as_bytes().iter().enumerate().all(|(index, byte)| {
            if [4, 7].contains(&index) {
                *byte == b'-'
            } else {
                byte.is_ascii_digit()
            }
        });
    if !valid_shape {
        return None;
    }
    let bytes = value.as_bytes();
    let year = parse_ascii_digits(bytes.get(0..4)?)?;
    let month = parse_ascii_digits(bytes.get(5..7)?)?;
    let day = parse_ascii_digits(bytes.get(8..10)?)?;
    if year < 1 || !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    Some(days_from_civil(year, month, day))
}

fn parse_ascii_digits(bytes: &[u8]) -> Option<i64> {
    bytes.iter().try_fold(0_i64, |value, byte| {
        byte.is_ascii_digit()
            .then_some(value * 10 + i64::from(byte - b'0'))
    })
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let adjusted_year = year - i64::from(month <= 2);
    let era = (if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    }) / 400;
    let year_of_era = adjusted_year - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

pub(super) fn unix_today() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| (duration.as_secs() / 86_400) as i64)
}

#[cfg(test)]
pub(crate) fn utc_date_with_offset(offset_days: i64) -> String {
    let (year, month, day) = civil_from_days(unix_today() + offset_days);
    format!("{year:04}-{month:02}-{day:02}")
}

#[cfg(test)]
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = (if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    }) / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_part = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_part + 2) / 5 + 1;
    let month = month_part + if month_part < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year, month, day)
}
