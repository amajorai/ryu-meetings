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
