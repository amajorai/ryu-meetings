//! The recap and daily-digest prompts. They live with the recap module, not in
//! the meeting-notes template catalog, because they are not user-selectable
//! note styles: the recap shape is fixed and the digest is internal.

/// The recap system prompt. `people` adds the opt-in names field; without it the
/// model is told to leave names out entirely.
pub fn recap_prompt(people: bool) -> String {
    let people_rule = if people {
        "\"people\" (array of the first names or roles of people who spoke or were \
discussed, at most 8). "
    } else {
        "Do not include a \"people\" key and do not name people anywhere; refer to \
them by role (\"a colleague\", \"the client\"). "
    };
    format!(
        "You summarize a short personal conversation the user recorded. You are \
given a raw, possibly imperfect speech-to-text transcript in which sensitive \
numbers were already replaced by tokens like [card number] or [phone]. Respond with \
ONLY one JSON object, no prose, no markdown fences, with these keys: \
\"title\" (at most 80 characters, specific, no quotes), \
\"summary\" (2 to 4 sentences), \
\"key_points\" (array of strings), \
\"action_items\" (array of strings), \
\"action_items_detail\" (array of objects with \"text\", \"owner\" and \"due\", \
using null when the owner or due date was not spoken), \
\"decisions\" (array of strings), \
\"topics\" (array of at most 8 short topic labels). {people_rule}\
Use empty arrays when a section has nothing. Leave out identifiers, credentials, \
account numbers and health details. Never invent content the transcript does not \
support."
    )
}

/// The interim system prompt: a rolling summary of a conversation that is still
/// being recorded. Same privacy rules as [`recap_prompt`]; no title or action
/// list, because the conversation is not over.
pub fn interim_prompt(people: bool) -> String {
    let people_rule = if people {
        "You may use first names or roles of people who spoke. "
    } else {
        "Do not name people anywhere; refer to them by role (\"a colleague\"). "
    };
    format!(
        "You give a rolling summary of a personal conversation the user is still \
recording. You are given the latest part of a raw speech-to-text transcript in which \
sensitive numbers were already replaced by tokens like [card number] or [phone]. \
Respond with ONLY one JSON object, no prose, no markdown fences, with keys \
\"summary\" (1 to 3 sentences on what has been discussed so far) and \
\"key_points\" (array of at most 6 short strings). {people_rule}\
Leave out identifiers, credentials, account numbers and health details. Never \
invent content the transcript does not support."
    )
}

/// The daily-digest system prompt (title + summary only; the structured parts of
/// a digest are assembled deterministically from the day's recaps).
pub const DAILY_DIGEST_PROMPT: &str = "You write a short end-of-day digest from the \
summaries of one person's recorded conversations. Respond with ONLY one JSON object \
with keys \"summary\" (2 to 3 sentences covering the day) and \"title\" (at most 60 \
characters). Do not name people; refer to them by role. Do not invent anything.";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn people_are_left_out_unless_opted_in() {
        assert!(recap_prompt(false).contains("do not name people"));
        assert!(!recap_prompt(false).contains("\"people\" (array"));
        assert!(recap_prompt(true).contains("\"people\" (array"));
        for key in ["title", "summary", "action_items_detail", "topics"] {
            assert!(recap_prompt(false).contains(&format!("\"{key}\"")), "{key}");
        }
    }

    #[test]
    fn the_interim_prompt_follows_the_people_opt_in() {
        assert!(interim_prompt(false).contains("Do not name people"));
        assert!(!interim_prompt(true).contains("Do not name people"));
        assert!(interim_prompt(false).contains("\"key_points\""));
        assert!(interim_prompt(true).contains("still recording"));
    }
}
