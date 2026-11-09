//! Owner-scoped recap search. Notes and transcripts are sealed, so this opens
//! only the caller's own rows and scores them in memory: a weighted token match
//! plus a recency boost. `POST` with a JSON body keeps queries out of URLs and
//! access logs. People's names are never searched.

use axum::{extract::State, http::HeaderMap, Json};
use chrono::{Duration, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{enter, RecapError, RecapResult};
use crate::api::MeetingsCtx;
use crate::Meeting;

/// The notes scope looks back this far by default.
const NOTES_LOOKBACK_DAYS: i64 = 90;
/// Most rows the notes scope opens.
const MAX_NOTES_ROWS: usize = 2_000;
/// Most recaps the transcript scope opens.
const MAX_TRANSCRIPT_RECAPS: usize = 200;
const MAX_RESULTS: usize = 50;

/// `POST /recaps/search` body.
#[derive(Debug, Deserialize)]
pub struct SearchBody {
    pub q: String,
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub to: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Lower-cased alphanumeric tokens of at least two characters.
pub fn tokens(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| t.chars().count() >= 2)
        .map(str::to_lowercase)
        .collect()
}

fn count_hits(field: &str, terms: &[String]) -> usize {
    let words = tokens(field);
    terms
        .iter()
        .filter(|t| words.iter().any(|w| w.starts_with(t.as_str())))
        .count()
}

/// The weighted note score of one recap (0 when no term matches).
pub fn score_notes(recap: &Meeting, terms: &[String]) -> f64 {
    let mut score = 3.0 * count_hits(&recap.title, terms) as f64;
    if let Some(notes) = &recap.notes {
        score += 2.0 * count_hits(&notes.topics.join(" "), terms) as f64;
        score += 1.5 * count_hits(&notes.summary, terms) as f64;
        let lists = [&notes.key_points, &notes.action_items, &notes.decisions];
        for list in lists {
            score += count_hits(&list.join(" "), terms) as f64;
        }
    }
    score
}

/// A gentle boost for recent recaps: 1.0 today, halving every 30 days.
pub fn recency_boost(local_day: &str, today: NaiveDate) -> f64 {
    let Ok(day) = NaiveDate::parse_from_str(local_day, "%Y-%m-%d") else {
        return 1.0;
    };
    let age = (today - day).num_days().max(0) as f64;
    1.0 + 0.5f64.powf(age / 30.0)
}

fn snippet(text: &str, terms: &[String]) -> String {
    // Work in characters: lower-casing can change byte lengths, so a byte offset
    // found in the lower-cased copy is not a valid index into `text`.
    let lower = text.to_lowercase();
    let at = terms
        .iter()
        .filter_map(|t| lower.find(t.as_str()).map(|i| lower[..i].chars().count()))
        .min()
        .unwrap_or(0);
    let tail: String = text.chars().skip(at.saturating_sub(40)).take(160).collect();
    tail.trim().to_string()
}

/// `POST /recaps/search`.
pub async fn search(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Json(body): Json<SearchBody>,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    let terms = tokens(&body.q);
    if terms.is_empty() || body.q.len() > 500 {
        return Err(RecapError::BadRequest(
            "q must hold a word of 2+ characters".into(),
        ));
    }
    let transcript_scope = match body.scope.as_deref() {
        None | Some("notes") => false,
        Some("transcript") => true,
        Some(_) => {
            return Err(RecapError::BadRequest(
                "scope must be notes or transcript".into(),
            ))
        }
    };
    let today = Utc::now().date_naive();
    let default_from = (today - Duration::days(NOTES_LOOKBACK_DAYS))
        .format("%Y-%m-%d")
        .to_string();
    let from = body.from.clone().unwrap_or(default_from);
    let rows = if transcript_scope {
        MAX_TRANSCRIPT_RECAPS
    } else {
        MAX_NOTES_ROWS
    };
    let recaps = ctx
        .engine
        .store
        .list_owned_recaps(&caller.user_id, Some(&from), body.to.as_deref(), None, rows)
        .await?;
    let mut results = Vec::new();
    for recap in &recaps {
        let day = recap.recap.as_ref().map_or("", |m| m.local_day.as_str());
        let mut score = score_notes(recap, &terms);
        let mut text = recap
            .notes
            .as_ref()
            .map(|n| n.summary.clone())
            .unwrap_or_default();
        if transcript_scope {
            let transcript: Vec<String> = ctx
                .engine
                .store
                .recap_segments(&recap.id)
                .await?
                .into_iter()
                .map(|s| s.text)
                .collect();
            let joined = transcript.join(" ");
            let hits = count_hits(&joined, &terms);
            if hits > 0 {
                score += hits as f64;
                text = joined;
            }
        }
        if score > 0.0 {
            results.push((
                score * recency_boost(day, today),
                recap,
                day,
                snippet(&text, &terms),
            ));
        }
    }
    results.sort_by(|a, b| b.0.total_cmp(&a.0));
    let limit = body.limit.unwrap_or(20).clamp(1, MAX_RESULTS);
    let items: Vec<Value> = results
        .into_iter()
        .take(limit)
        .map(|(score, recap, day, snippet)| {
            json!({
                "recap_id": recap.id,
                "title": recap.title,
                "local_day": day,
                "score": score,
                "snippet": snippet,
            })
        })
        .collect();
    Ok(Json(json!({ "results": items })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_and_recency() {
        assert_eq!(tokens("Budget, Q3 plan!"), vec!["budget", "q3", "plan"]);
        let today = NaiveDate::from_ymd_opt(2026, 11, 8).unwrap();
        assert!(recency_boost("2026-11-08", today) > recency_boost("2026-09-08", today));
    }

    #[test]
    fn snippet_centres_on_the_first_hit() {
        let text = format!("{} the budget review moved", "x ".repeat(100));
        let s = snippet(&text, &["budget".to_string()]);
        assert!(s.contains("budget"), "{s}");
        assert!(s.chars().count() <= 160);
    }
}
