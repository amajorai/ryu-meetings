//! Deterministic redaction. Runs on the transcript text BEFORE any model call and
//! again on the generated notes before they are stored. The sealed transcript keeps
//! the original text for the owner; only what leaves for the model, and what the
//! notes keep, is redacted.
//!
//! Patterns, applied in order: IBANs (mod-97 checked), payment card numbers
//! (13–19 digits, Luhn-checked), US SSNs, email addresses, phone numbers, then any
//! remaining run of 9+ digits. A candidate that fails its checksum is left to the
//! later, more generic patterns.

use std::sync::OnceLock;

use regex::{Captures, Regex};

use crate::MeetingNotes;

const CARD: &str = "[card number]";
const IBAN: &str = "[iban]";
const SSN: &str = "[ssn]";
const EMAIL: &str = "[email]";
const PHONE: &str = "[phone]";
const NUMBER: &str = "[number]";

struct Patterns {
    card: Regex,
    iban: Regex,
    ssn: Regex,
    email: Regex,
    phone: Regex,
    digits: Regex,
    iso_date: Regex,
}

fn patterns() -> &'static Patterns {
    static PATTERNS: OnceLock<Patterns> = OnceLock::new();
    PATTERNS.get_or_init(|| Patterns {
        card: Regex::new(r"\b(?:\d[ -]?){12,18}\d\b").expect("card pattern"),
        iban: Regex::new(r"\b[A-Z]{2}\d{2}(?: ?[A-Z0-9]){11,30}\b").expect("iban pattern"),
        ssn: Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").expect("ssn pattern"),
        email: Regex::new(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b").expect("email pattern"),
        phone: Regex::new(
            r"(?:\+\d{1,3}[ .-]?)?(?:\(\d{2,4}\)[ .-]?)?\d{2,4}(?:[ .-]\d{2,4}){2,4}",
        )
        .expect("phone pattern"),
        digits: Regex::new(r"\d{9,}").expect("digit-run pattern"),
        iso_date: Regex::new(r"^\d{4}-\d{2}-\d{2}$").expect("date pattern"),
    })
}

fn digits_of(text: &str) -> Vec<u32> {
    text.chars().filter_map(|c| c.to_digit(10)).collect()
}

/// The Luhn checksum used by payment cards.
pub fn luhn_valid(number: &str) -> bool {
    let digits = digits_of(number);
    if digits.len() < 13 {
        return false;
    }
    let sum: u32 = digits
        .iter()
        .rev()
        .enumerate()
        .map(|(i, &d)| {
            if i % 2 == 1 {
                let doubled = d * 2;
                if doubled > 9 {
                    doubled - 9
                } else {
                    doubled
                }
            } else {
                d
            }
        })
        .sum();
    sum.is_multiple_of(10)
}

/// The ISO 13616 mod-97 check.
pub fn iban_valid(candidate: &str) -> bool {
    let compact: String = candidate.chars().filter(|c| !c.is_whitespace()).collect();
    if !(15..=34).contains(&compact.len()) {
        return false;
    }
    let (head, tail) = compact.split_at(4);
    let mut remainder: u64 = 0;
    for ch in tail.chars().chain(head.chars()) {
        let value = match ch {
            '0'..='9' => u64::from(ch as u8 - b'0'),
            'A'..='Z' => u64::from(ch as u8 - b'A') + 10,
            _ => return false,
        };
        remainder = if value >= 10 {
            (remainder * 100 + value) % 97
        } else {
            (remainder * 10 + value) % 97
        };
    }
    remainder == 1
}

/// Redact one string.
pub fn redact_text(text: &str) -> String {
    let p = patterns();
    let out = p.iban.replace_all(text, |caps: &Captures<'_>| {
        let found = &caps[0];
        if iban_valid(found) {
            IBAN.to_string()
        } else {
            found.to_string()
        }
    });
    let out = p.card.replace_all(&out, |caps: &Captures<'_>| {
        let found = &caps[0];
        if luhn_valid(found) {
            CARD.to_string()
        } else {
            found.to_string()
        }
    });
    let out = p.ssn.replace_all(&out, SSN);
    let out = p.email.replace_all(&out, EMAIL);
    let out = p.phone.replace_all(&out, |caps: &Captures<'_>| {
        let found = &caps[0];
        if digits_of(found).len() >= 7 && !p.iso_date.is_match(found) {
            PHONE.to_string()
        } else {
            found.to_string()
        }
    });
    p.digits.replace_all(&out, NUMBER).into_owned()
}

/// Redact every free-text field of generated notes in place.
pub fn redact_notes(notes: &mut MeetingNotes) {
    let fix = |items: &mut Vec<String>| {
        for item in items.iter_mut() {
            *item = redact_text(item);
        }
    };
    notes.summary = redact_text(&notes.summary);
    notes.title = notes.title.as_deref().map(redact_text);
    fix(&mut notes.key_points);
    fix(&mut notes.action_items);
    fix(&mut notes.decisions);
    fix(&mut notes.topics);
    for item in &mut notes.action_items_detail {
        item.text = redact_text(&item.text);
        item.due = item.due.as_deref().map(redact_text);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn card_numbers_are_redacted_only_when_luhn_valid() {
        assert_eq!(
            redact_text("card 4111 1111 1111 1111 ok"),
            "card [card number] ok"
        );
        assert!(luhn_valid("4111111111111111"));
        assert!(!luhn_valid("4111111111111112"));
    }

    #[test]
    fn iban_ssn_email_phone_and_digit_runs_are_redacted() {
        assert_eq!(
            redact_text("iban GB82 WEST 1234 5698 7654 32."),
            "iban [iban]."
        );
        assert!(iban_valid("DE89370400440532013000"));
        assert!(!iban_valid("DE89370400440532013001"));
        assert_eq!(redact_text("ssn 123-45-6789"), "ssn [ssn]");
        assert_eq!(redact_text("mail a.b@example.com now"), "mail [email] now");
        assert_eq!(redact_text("call +1 415 555 0100"), "call [phone]");
        assert_eq!(redact_text("ref 1234567890123"), "ref [number]");
    }

    #[test]
    fn ordinary_speech_and_small_numbers_are_kept() {
        let text = "We met at 10 on May 3, due 2026-11-20, and agreed on 25 percent for 2027.";
        assert_eq!(redact_text(text), text);
    }

    #[test]
    fn notes_fields_are_redacted() {
        let mut notes = MeetingNotes {
            summary: "Pay with 4111111111111111".into(),
            key_points: vec!["email jo@example.com".into()],
            action_items_detail: vec![crate::notes::ActionItem {
                text: "call 415-555-0100".into(),
                owner: None,
                due: None,
            }],
            ..MeetingNotes::default()
        };
        redact_notes(&mut notes);
        assert_eq!(notes.summary, "Pay with [card number]");
        assert_eq!(notes.key_points[0], "email [email]");
        assert_eq!(notes.action_items_detail[0].text, "call [phone]");
    }
}
