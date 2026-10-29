//! Meeting-notes templates — named prompt presets over the **fixed**
//! [`super::notes::MeetingNotes`] schema.
//!
//! A template only steers *what the model emphasizes* (a standup vs. a sales call
//! vs. an interview want different notes); it does **not** change the output shape.
//! Every template still produces the same four fields (summary / key_points /
//! action_items / decisions), so the Space markdown renderer and the desktop notes
//! card never have to know which template ran. That keeps templates a pure prompt
//! concern instead of a schema change that would ripple through the whole stack.
//!
//! The prompt handed to the model is `BASE_INSTRUCTION` (the invariant JSON
//! contract) + the template's `guidance`. A user's fully custom prompt
//! (`meeting-notes-prompt`) still overrides everything, for full control.
//!
//! # Why each template carries browse metadata
//!
//! The catalog is not just a `<select>` any more: the Meetings app registers a
//! **Store tab** (`contributes.store_tabs` in its manifest) that browses these as
//! cards, so each entry needs what a card renders — a description, a category to
//! group by, a glyph, and search tags. Installing a card means selecting it: the
//! Store's declarative install action writes `meeting-notes-template`, which is the
//! same preference the notes pipeline already reads (`api::resolve_notes_prompt`).
//! No new persistence, and the Settings picker and the Store tab stay two views of
//! one list.
//!
//! **Ids are load-bearing.** `meeting-notes-template` stores an id, so renaming one
//! silently reverts that user to the default. The five original ids (`default`,
//! `standup`, `sales`, `one_on_one`, `interview`) are frozen for that reason — new
//! coverage arrives as new ids, never as a rename.

/// The invariant part of the system prompt: the JSON contract every template
/// shares. Templates append focus guidance after this.
pub const BASE_INSTRUCTION: &str = "You are an expert meeting-notes assistant. \
You are given a raw, possibly imperfect speech-to-text transcript of a meeting \
(or ordered partial summaries of a long one). Respond with ONLY a single JSON \
object, no prose, no markdown fences, with exactly these keys: \
\"summary\" (a short paragraph), \
\"key_points\" (array of strings), \
\"action_items\" (array of strings, each ideally naming an owner if one is clear), \
\"decisions\" (array of strings). \
Use empty arrays when a section has nothing. Do not invent content that is not \
supported by the transcript.";

/// One notes template.
pub struct NotesTemplate {
    pub id: &'static str,
    pub name: &'static str,
    /// One-line description — the Store card's supporting text.
    pub description: &'static str,
    /// Grouping key for the Store tab's card sections. One of `general`,
    /// `recurring`, `revenue`, `hiring`, `delivery`, `research`, `leadership`.
    pub category: &'static str,
    /// Icon id resolved by the shell's Icon primitive (Iconify `prefix:name`).
    pub icon: &'static str,
    /// Search tags.
    pub tags: &'static [&'static str],
    /// Focus guidance appended to [`BASE_INSTRUCTION`].
    pub guidance: &'static str,
}

/// The built-in templates. `default` is first and is the fallback.
pub const TEMPLATES: &[NotesTemplate] = &[
    NotesTemplate {
        id: "default",
        name: "General meeting",
        description: "Balanced notes for any conversation that doesn't fit a shape.",
        category: "general",
        icon: "lucide:notebook-pen",
        tags: &["general", "default"],
        guidance: "Write concise, useful general-purpose notes that a participant \
would want the day after.",
    },
    // ── Recurring team rituals ────────────────────────────────────────────────
    NotesTemplate {
        id: "standup",
        name: "Daily standup",
        description: "Per-person progress, today's commitments, and flagged blockers.",
        category: "recurring",
        icon: "lucide:repeat",
        tags: &["standup", "scrum", "team", "daily"],
        guidance: "This is a team standup. For key_points, capture per-person \
progress (what shipped / what's in flight). For action_items, capture today's \
commitments and anyone's stated blockers (prefix blockers with 'BLOCKER:'). Keep \
the summary to two sentences.",
    },
    NotesTemplate {
        id: "one_on_one",
        name: "1:1",
        description: "Discreet manager/report notes: growth, feedback, workload, follow-ups.",
        category: "recurring",
        icon: "lucide:user",
        tags: &["1:1", "manager", "report", "feedback"],
        guidance: "This is a manager/report 1:1. Keep it discreet and factual. \
key_points cover topics discussed (growth, feedback, workload, morale). \
action_items are follow-ups for either person. decisions are anything agreed \
(scope changes, goals, next-step timing).",
    },
    NotesTemplate {
        id: "retro",
        name: "Retrospective",
        description: "What went well, what didn't, and the experiments the team committed to.",
        category: "recurring",
        icon: "lucide:refresh-cw",
        tags: &["retro", "retrospective", "agile", "team"],
        guidance: "This is a team retrospective. In key_points, separate what went \
well from what went badly, and keep the team's own wording for each — a retro's \
value is in the phrasing people chose. action_items are the experiments or process \
changes the team committed to, with an owner where one was named. decisions are \
practices the team agreed to start, stop or keep. Do not editorialise about \
individuals or assign blame that was not voiced.",
    },
    NotesTemplate {
        id: "sprint_planning",
        name: "Sprint planning",
        description: "Scope taken in, what was deferred, and the estimates and risks raised.",
        category: "recurring",
        icon: "lucide:calendar-days",
        tags: &["planning", "sprint", "agile", "scope"],
        guidance: "This is a sprint/iteration planning session. key_points capture \
the work pulled into the sprint, anything explicitly deferred or descoped, and any \
estimate or capacity concern raised. action_items are pre-work someone owes before \
the work can start (specs, access, decisions). decisions are the committed scope \
and any priority calls. Record stated risks even when they were left unresolved.",
    },
    NotesTemplate {
        id: "all_hands",
        name: "All-hands",
        description: "Company updates, metrics quoted, and the questions asked from the floor.",
        category: "recurring",
        icon: "lucide:megaphone",
        tags: &["all-hands", "company", "announcement", "q&a"],
        guidance: "This is a company all-hands. The summary should read as an update \
someone who missed it can act on. key_points cover announcements, metrics quoted \
(keep the numbers exactly as stated), and org or roadmap changes. Capture the Q&A \
separately in key_points as 'Q: ... A: ...' pairs. action_items are anything \
leadership committed to follow up on. Do not smooth over an answer that was a \
non-answer.",
    },
    // ── Revenue conversations ─────────────────────────────────────────────────
    NotesTemplate {
        id: "sales",
        name: "Sales call",
        description: "Pain, budget/authority/need/timeline signals, objections, and competitors.",
        category: "revenue",
        icon: "lucide:trending-up",
        tags: &["sales", "prospect", "pipeline", "bant"],
        guidance: "This is a sales/customer call. Emphasize the prospect's pain \
points, budget/authority/need/timeline signals, objections raised, and \
competitors mentioned in key_points. action_items are the seller's follow-ups \
(demos, proposals, intros) with due timing when stated. decisions are any \
commitments the prospect made.",
    },
    NotesTemplate {
        id: "sales_demo",
        name: "Product demo",
        description: "Which features landed, what was asked for, and the blocking gaps.",
        category: "revenue",
        icon: "lucide:presentation",
        tags: &["demo", "sales", "product", "evaluation"],
        guidance: "This is a product demo. key_points record which capabilities were \
shown and how the audience reacted to each — enthusiasm, silence and confusion are \
all signal. Call out every feature request and every gap named as blocking, \
labelled 'GAP:'. action_items are follow-ups the seller owes (trials, docs, \
security review, pricing). decisions are next steps the buyer agreed to. Do not \
report a feature as landing well unless someone said so.",
    },
    NotesTemplate {
        id: "customer_call",
        name: "Customer check-in",
        description: "Health of an existing account: usage, friction, escalations, expansion.",
        category: "revenue",
        icon: "lucide:phone-call",
        tags: &["customer", "success", "qbr", "account"],
        guidance: "This is a check-in or QBR with an EXISTING customer, not a \
prospect. key_points cover how they are actually using the product, friction and \
open escalations, adoption or usage numbers quoted, and any expansion or \
contraction signal. action_items are commitments made to the customer, each with an \
owner. decisions are anything agreed about the account (plan changes, timelines, \
escalation paths). Flag churn risk explicitly with 'RISK:' when the customer voices \
dissatisfaction.",
    },
    NotesTemplate {
        id: "renewal",
        name: "Renewal / negotiation",
        description: "Terms discussed, pricing positions, blockers, and each side's commitments.",
        category: "revenue",
        icon: "lucide:scale",
        tags: &["renewal", "negotiation", "contract", "pricing"],
        guidance: "This is a renewal or contract negotiation. Record positions \
precisely: quote figures, dates and terms exactly as stated, and attribute each to \
the side that said it. key_points cover the terms under discussion, each side's \
stated position, and any blocker to signing (legal, security, procurement). \
action_items are what each side owes before the next conversation. decisions are \
only what was explicitly agreed — never infer agreement from an absence of \
objection.",
    },
    // ── Hiring ────────────────────────────────────────────────────────────────
    NotesTemplate {
        id: "interview",
        name: "Interview",
        description: "Candidate experience, strengths, concerns, and the next step in the loop.",
        category: "hiring",
        icon: "lucide:user-check",
        tags: &["interview", "hiring", "candidate", "screen"],
        guidance: "This is a candidate interview. key_points summarize the \
candidate's relevant experience, strengths, and any concerns surfaced. \
action_items are next steps in the process. decisions capture any stated \
lean (advance / hold / reject) without inventing a verdict that wasn't voiced.",
    },
    NotesTemplate {
        id: "interview_technical",
        name: "Technical interview",
        description: "How the candidate approached the problem, not just whether they solved it.",
        category: "hiring",
        icon: "lucide:code",
        tags: &["interview", "technical", "hiring", "engineering"],
        guidance: "This is a technical interview. key_points describe the problem \
posed, the candidate's APPROACH (how they decomposed it, what they asked, where \
they got stuck, how they recovered), and the depth shown on follow-up questions — \
process matters more than the final answer. Note communication and collaboration \
signals separately. action_items are the loop's next steps. decisions capture a \
stated hire/no-hire lean only if the interviewer actually voiced one. Never infer \
a score.",
    },
    NotesTemplate {
        id: "interview_debrief",
        name: "Hiring debrief",
        description: "Each interviewer's read, where the panel disagreed, and the decision.",
        category: "hiring",
        icon: "lucide:users",
        tags: &["debrief", "hiring", "panel", "decision"],
        guidance: "This is a hiring debrief with multiple interviewers. key_points \
capture each interviewer's read attributed to them by name, and — importantly — \
where the panel DISAGREED and on what evidence. action_items are follow-ups \
(references, an extra round, a take-home). decisions are the panel's outcome and \
any conditions attached to it. Keep evaluative language to what was actually said; \
this record can be reviewed later.",
    },
    // ── Delivery ──────────────────────────────────────────────────────────────
    NotesTemplate {
        id: "kickoff",
        name: "Project kickoff",
        description: "Goals, scope boundaries, owners, milestones, and the risks named up front.",
        category: "delivery",
        icon: "lucide:rocket",
        tags: &["kickoff", "project", "scope", "milestones"],
        guidance: "This is a project kickoff. key_points capture the goal in the \
team's own words, what is explicitly IN and OUT of scope, named owners per \
workstream, milestone dates, and dependencies on other teams. action_items are \
setup tasks with owners. decisions are the agreed scope, timeline and ownership. \
Record every risk raised, even ones waved off — an unlogged early risk is the one \
that bites.",
    },
    NotesTemplate {
        id: "status_review",
        name: "Status review",
        description: "Progress against plan, what slipped and why, and the calls made.",
        category: "delivery",
        icon: "lucide:clipboard-check",
        tags: &["status", "review", "progress", "delivery"],
        guidance: "This is a project status review. key_points state progress \
against the plan per workstream, what slipped and the stated reason, and any \
changed dates. Separate reported facts from projections. action_items are \
unblocking steps with owners and timing. decisions are scope, resourcing or date \
changes that were actually agreed in the room.",
    },
    NotesTemplate {
        id: "incident_postmortem",
        name: "Incident postmortem",
        description: "Blameless timeline, contributing factors, and the remediations owned.",
        category: "delivery",
        icon: "lucide:siren",
        tags: &["incident", "postmortem", "outage", "reliability"],
        guidance: "This is a blameless incident postmortem. Build key_points as an \
ordered TIMELINE — detection, escalation, mitigation, resolution — with times \
exactly as stated, followed by contributing factors and customer impact. Describe \
what systems and processes did, never what a person did wrong: name roles, not \
blame. action_items are remediations, each with an owner and a priority when \
stated. decisions are agreed follow-ups and any policy change. Mark unknowns as \
open questions rather than guessing at a cause.",
    },
    // ── Research ──────────────────────────────────────────────────────────────
    NotesTemplate {
        id: "user_interview",
        name: "User research",
        description: "What the user does today, in their words — pain, workarounds, quotes.",
        category: "research",
        icon: "lucide:search",
        tags: &["research", "discovery", "user", "interview"],
        guidance: "This is a user research or discovery interview. Preserve the \
participant's OWN WORDS: include verbatim quotes in key_points, marked with \
quotation marks, rather than paraphrasing away their phrasing. Capture their \
current workflow, the pain in it, and the workarounds they have built. Separate \
what they DO (observed behaviour) from what they SAY they want (stated \
preference) — the two diverge and the distinction is the whole point. action_items \
are researcher follow-ups. decisions are only process decisions the team made; a \
research interview rarely produces product decisions and you must not invent one.",
    },
    NotesTemplate {
        id: "vendor_eval",
        name: "Vendor evaluation",
        description: "Capabilities claimed, pricing, limits, and the questions still open.",
        category: "research",
        icon: "lucide:building-2",
        tags: &["vendor", "procurement", "evaluation", "pricing"],
        guidance: "This is a vendor or tooling evaluation call. key_points cover \
claimed capabilities, pricing and licensing terms (quote figures exactly), \
integration and security posture, and any stated limitation. Distinguish a \
demonstrated capability from a roadmap promise — label the latter 'ROADMAP:'. \
action_items are evaluation follow-ups (trials, references, security review). \
decisions are procurement steps agreed. List unanswered questions explicitly.",
    },
    // ── Leadership ────────────────────────────────────────────────────────────
    NotesTemplate {
        id: "board_update",
        name: "Board / investor update",
        description: "Metrics as stated, the questions asked, and what leadership committed to.",
        category: "leadership",
        icon: "lucide:landmark",
        tags: &["board", "investor", "metrics", "update"],
        guidance: "This is a board or investor meeting. Accuracy beats brevity here: \
quote every metric, period and figure exactly as stated, and never round or \
recompute. key_points cover performance against plan, the strategic topics \
discussed, and each question a board member asked with the answer given. \
action_items are commitments made to the board with their timing. decisions are \
board-level approvals or directions. Flag anything presented as a projection so it \
cannot later be read as a result.",
    },
    NotesTemplate {
        id: "performance_review",
        name: "Performance review",
        description: "Evidence cited, goals set, and development commitments — kept factual.",
        category: "leadership",
        icon: "lucide:target",
        tags: &["review", "performance", "goals", "hr"],
        guidance: "This is a performance review. This record may be read back later, \
so stay strictly factual: key_points capture the evidence and examples actually \
cited, the feedback given in the words used, and the person's own response. \
action_items are development commitments from both sides, with timing. decisions \
are agreed goals, rating or level outcomes, and support committed. Do not add \
evaluative language nobody used, and do not soften or sharpen feedback.",
    },
    NotesTemplate {
        id: "strategy_offsite",
        name: "Strategy session",
        description: "Options weighed, the tradeoffs argued, and what was actually decided.",
        category: "leadership",
        icon: "lucide:map",
        tags: &["strategy", "offsite", "planning", "tradeoffs"],
        guidance: "This is a strategy session or offsite. key_points capture each \
option considered WITH the tradeoffs argued for and against it — a strategy record \
whose reasoning is missing is useless six months later. Note where the group \
disagreed and whether it resolved. action_items are follow-up work with owners. \
decisions are what was actually chosen; anything left open belongs in key_points as \
an open question, never in decisions.",
    },
];

/// Look up a template by id (case-insensitive), or `None`.
pub fn by_id(id: &str) -> Option<&'static NotesTemplate> {
    let id = id.trim().to_lowercase();
    TEMPLATES.iter().find(|t| t.id == id)
}

/// The default template (always the first entry).
pub fn default_template() -> &'static NotesTemplate {
    &TEMPLATES[0]
}

/// Build the full system prompt for a template id, falling back to the default
/// when the id is unknown/empty.
pub fn prompt_for(id: &str) -> String {
    let t = by_id(id).unwrap_or_else(|| default_template());
    format!("{BASE_INSTRUCTION} {}", t.guidance)
}

/// A JSON view of the templates for the Settings picker and the Store tab.
///
/// `active` marks the one currently selected by the `meeting-notes-template`
/// preference — resolved through the same fallback [`prompt_for`] uses, so an id
/// that no longer exists marks `default` active rather than nothing. It is the flag
/// the Store tab's `map.installed` reads, so a card shows "Added" for the template
/// genuinely in force, not merely for one selected in this session.
pub fn catalog_json(selected_id: &str) -> serde_json::Value {
    let active = by_id(selected_id).unwrap_or_else(|| default_template()).id;
    serde_json::json!({
        "templates": TEMPLATES
            .iter()
            .map(|t| serde_json::json!({
                "id": t.id,
                "name": t.name,
                "description": t.description,
                "category": t.category,
                "icon": t.icon,
                "tags": t.tags,
                "active": t.id == active,
            }))
            .collect::<Vec<_>>()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_first_and_resolvable() {
        assert_eq!(default_template().id, "default");
        assert!(prompt_for("default").starts_with("You are an expert"));
    }

    #[test]
    fn unknown_id_falls_back_to_default() {
        assert!(by_id("nope").is_none());
        assert_eq!(prompt_for("nope"), prompt_for("default"));
    }

    #[test]
    fn known_templates_include_guidance() {
        let p = prompt_for("sales");
        assert!(p.contains("sales/customer call"));
        assert!(p.contains(BASE_INSTRUCTION));
    }

    #[test]
    fn lookup_is_case_insensitive() {
        assert_eq!(by_id("StandUp").map(|t| t.id), Some("standup"));
    }

    #[test]
    fn empty_id_falls_back_to_default() {
        assert_eq!(prompt_for(""), prompt_for("default"));
        assert_eq!(prompt_for("   "), prompt_for("default"));
    }

    #[test]
    fn catalog_json_lists_every_template_id_and_name() {
        let cat = catalog_json("");
        let arr = cat["templates"].as_array().unwrap();
        assert_eq!(arr.len(), TEMPLATES.len());
        for (entry, tpl) in arr.iter().zip(TEMPLATES.iter()) {
            assert_eq!(entry["id"], tpl.id);
            assert_eq!(entry["name"], tpl.name);
        }
    }

    #[test]
    fn every_template_prompt_embeds_the_base_contract() {
        for tpl in TEMPLATES {
            let p = prompt_for(tpl.id);
            assert!(p.contains(BASE_INSTRUCTION));
            assert!(p.contains(tpl.guidance));
        }
    }

    /// The Store tab renders these as cards, so every entry needs the fields a card
    /// draws. A template added without them would render as a blank tile.
    #[test]
    fn every_template_carries_browse_metadata() {
        for tpl in TEMPLATES {
            assert!(!tpl.description.is_empty(), "{} has no description", tpl.id);
            assert!(!tpl.icon.is_empty(), "{} has no icon", tpl.id);
            assert!(!tpl.tags.is_empty(), "{} has no tags", tpl.id);
        }
    }

    /// `category` is the Store tab's `groupBy`, and the manifest declares a label per
    /// value. An undeclared category still renders (the renderer appends a group
    /// titled by the raw value) but would show up unlabelled — so hold the set closed
    /// here, where it is cheap to notice.
    #[test]
    fn categories_are_from_the_declared_set() {
        const KNOWN: &[&str] = &[
            "general",
            "recurring",
            "revenue",
            "hiring",
            "delivery",
            "research",
            "leadership",
        ];
        for tpl in TEMPLATES {
            assert!(
                KNOWN.contains(&tpl.category),
                "{} has undeclared category {}",
                tpl.id,
                tpl.category
            );
        }
    }

    /// Ids are persisted in the `meeting-notes-template` preference, so a rename
    /// silently reverts that user to the default. Freeze the original five.
    #[test]
    fn original_template_ids_are_frozen() {
        for id in ["default", "standup", "sales", "one_on_one", "interview"] {
            assert!(
                by_id(id).is_some(),
                "template id '{id}' was renamed or removed"
            );
        }
    }

    #[test]
    fn ids_are_unique() {
        let mut ids: Vec<&str> = TEMPLATES.iter().map(|t| t.id).collect();
        let total = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), total, "duplicate template id");
    }

    #[test]
    fn catalog_marks_the_selected_template_active() {
        let cat = catalog_json("sales");
        let arr = cat["templates"].as_array().unwrap();
        let active: Vec<&str> = arr
            .iter()
            .filter(|e| e["active"] == true)
            .map(|e| e["id"].as_str().unwrap())
            .collect();
        assert_eq!(active, vec!["sales"]);
    }

    /// An unknown stored id resolves to the default everywhere — the catalog's
    /// `active` flag must agree with what `prompt_for` would actually use, or the
    /// Store would show "Added" on a template that is not in force.
    #[test]
    fn catalog_active_agrees_with_prompt_fallback() {
        let cat = catalog_json("deleted-template");
        let arr = cat["templates"].as_array().unwrap();
        let active = arr.iter().find(|e| e["active"] == true).unwrap();
        assert_eq!(active["id"], "default");
    }
}
