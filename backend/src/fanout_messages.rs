//! Inbox, toast and push copy as localizable messages.
//!
//! Core renders a notify hint's `i18n` text per recipient (the inbox row and
//! toast in the node's `ui-language`, each push in the locale its device
//! registered with); the English rendering still goes out as the plain
//! `title`/`body`, which an older Core delivers as is.
//!
//! Each `*_MESSAGE` is `format!` with one named argument per placeholder
//! (`name = arg("name")`) so the i18n extractor catalogs the ICU source;
//! `tools/generate-core-i18n.mjs` copies the translations into Core's catalog.
//! Every message is a whole sentence. Core's renderer has no plural selector,
//! so counts are written as `label: {count}`.
#![allow(clippy::useless_format)]

use ryu_app_events::{NotifyHint, NotifyMessage, NotifyTemplate as Template};

pub(crate) const NOTES_READY_MESSAGE: Template = |_| format!("Meeting notes ready");
pub(crate) const NOTES_FILED_MESSAGE: Template = |_| format!("Filed to your Spaces.");

/// The Inbox notice raised when a meeting's notes are filed to a Space. Core
/// keeps notifications in plaintext (`notify.db`) and the meeting title is
/// sealed, so the text names no meeting: the Companion opens it by id.
pub(crate) fn notes_ready_notify_hint() -> NotifyHint {
    NotifyHint::localized(
        NotifyMessage::new(NOTES_READY_MESSAGE),
        vec![NotifyMessage::new(NOTES_FILED_MESSAGE)],
    )
    .with_level("success")
}

pub(crate) const RECAP_READY_MESSAGE: Template = |_| format!("Your recap is ready");
pub(crate) const RECAP_READY_BODY_MESSAGE: Template = |_| format!("Open Ryu to read it.");
pub(crate) const DIGEST_READY_MESSAGE: Template = |_| format!("Your daily recap is ready");
pub(crate) const DIGEST_COUNT_MESSAGE: Template = |arg| {
    format!(
        "Open Ryu to read today's recaps. Conversations recapped: {count}.",
        count = arg("count")
    )
};

/// The notice raised when a recap's notes are ready. Generic text only (no title
/// or content: Core's inbox and pushes are not sealed), addressed to the recap's
/// owner. Core delivers a hint with another member's `target_user_id` only when
/// this app holds the reviewed `notifications:send-to-user` grant; without it the
/// notice is dropped, never redirected to the node's active user.
pub(crate) fn recap_ready_notify_hint(target_user_id: String) -> NotifyHint {
    let mut hint = NotifyHint::localized(
        NotifyMessage::new(RECAP_READY_MESSAGE),
        vec![NotifyMessage::new(RECAP_READY_BODY_MESSAGE)],
    )
    .with_level("success");
    hint.target_user_id = Some(target_user_id);
    hint
}

/// The evening digest notice: a count, never titles.
pub(crate) fn digest_ready_notify_hint(target_user_id: String, count: i64) -> NotifyHint {
    let mut hint = NotifyHint::localized(
        NotifyMessage::new(DIGEST_READY_MESSAGE),
        vec![NotifyMessage::new(DIGEST_COUNT_MESSAGE).count("count", count)],
    );
    hint.target_user_id = Some(target_user_id);
    hint
}
