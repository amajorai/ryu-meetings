//! Who is asking. Follows the `@ryu/family` precedent: Core's ext-proxy stamps a
//! verified `x-ryu-caller-user-id` (and org) header; without one a managed node
//! refuses, and a local node means its owner.
//!
//! The headerless local-owner fallback is safe only because no recap route is
//! reachable through a headerless path: recaps are excluded from the OpenAPI tool
//! surface, declare no capability `provides`, and are never written in-process.

use axum::http::HeaderMap;

/// The verified caller header Core's ext-proxy stamps.
pub const CALLER_USER_HEADER: &str = "x-ryu-caller-user-id";
/// The verified caller org header.
pub const CALLER_ORG_HEADER: &str = "x-ryu-caller-org-id";
/// The owner id of a headerless request on a local node.
pub const LOCAL_OWNER: &str = "local";
/// Longest accepted header value.
const MAX_HEADER_BYTES: usize = 256;
/// The env Core sets on a managed node (same truthy values as Core).
const MANAGED_NODE_ENV: &str = "RYU_MANAGED_NODE";

/// The resolved caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    /// The recap owner key: the member's user id, or [`LOCAL_OWNER`].
    pub user_id: String,
    pub org_id: Option<String>,
}

impl Caller {
    /// The local node's owner.
    pub fn local() -> Self {
        Self {
            user_id: LOCAL_OWNER.to_string(),
            org_id: None,
        }
    }

    /// A verified member.
    pub fn member(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            org_id: None,
        }
    }

    /// The notification recipient for this owner: the member's concrete user id,
    /// or `None` for the headerless local owner, who has none. A notice with no
    /// recipient is not sent at all: Core would otherwise deliver it to the
    /// node's active account, which need not be this owner.
    pub fn notify_target(owner: &str) -> Option<String> {
        let owner = owner.trim();
        (!owner.is_empty() && owner != LOCAL_OWNER).then(|| owner.to_string())
    }
}

/// The caller could not be resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantRequired;

fn header(headers: &HeaderMap, name: &str) -> Result<Option<String>, TenantRequired> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| TenantRequired)?.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > MAX_HEADER_BYTES || value.chars().any(char::is_control) {
        return Err(TenantRequired);
    }
    Ok(Some(value.to_owned()))
}

/// Whether Core stamped any caller header (a member's request), as opposed to a
/// headerless call from Core itself or the node bearer.
pub fn header_present(headers: &HeaderMap) -> bool {
    headers.contains_key(CALLER_USER_HEADER) || headers.contains_key(CALLER_ORG_HEADER)
}

/// Resolve the caller from Core's verified headers.
pub fn resolve_caller(headers: &HeaderMap, managed_node: bool) -> Result<Caller, TenantRequired> {
    let user = header(headers, CALLER_USER_HEADER)?;
    let org = header(headers, CALLER_ORG_HEADER)?;
    match (user, org) {
        // A caller literally named like the local fallback would alias the
        // owner's rows on a node that later adds members.
        (Some(user), _) if user == LOCAL_OWNER => Err(TenantRequired),
        (Some(user), org) => Ok(Caller {
            user_id: user,
            org_id: org,
        }),
        (None, Some(_)) => Err(TenantRequired),
        (None, None) if managed_node => Err(TenantRequired),
        (None, None) => Ok(Caller::local()),
    }
}

/// Whether this process runs on a managed node (`RYU_MANAGED_NODE`).
pub fn is_managed_node() -> bool {
    std::env::var(MANAGED_NODE_ENV).ok().is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(*name, HeaderValue::from_str(value).unwrap());
        }
        map
    }

    #[test]
    fn verified_member_header_is_the_caller() {
        let caller = resolve_caller(&headers(&[(CALLER_USER_HEADER, " u1 ")]), true).unwrap();
        assert_eq!(caller, Caller::member("u1"));
        let with_org = resolve_caller(
            &headers(&[(CALLER_USER_HEADER, "u1"), (CALLER_ORG_HEADER, "o1")]),
            true,
        )
        .unwrap();
        assert_eq!(with_org.org_id.as_deref(), Some("o1"));
    }

    #[test]
    fn headerless_is_refused_on_a_managed_node_and_the_owner_locally() {
        assert_eq!(resolve_caller(&HeaderMap::new(), true), Err(TenantRequired));
        assert_eq!(
            resolve_caller(&HeaderMap::new(), false).unwrap(),
            Caller::local()
        );
    }

    #[test]
    fn malformed_headers_are_refused() {
        assert!(resolve_caller(&headers(&[(CALLER_ORG_HEADER, "o1")]), false).is_err());
        let long = "u".repeat(MAX_HEADER_BYTES + 1);
        assert!(resolve_caller(&headers(&[(CALLER_USER_HEADER, &long)]), false).is_err());
        assert!(resolve_caller(&headers(&[(CALLER_USER_HEADER, LOCAL_OWNER)]), false).is_err());
    }

    #[test]
    fn only_a_concrete_owner_is_a_notification_target() {
        assert_eq!(Caller::notify_target(LOCAL_OWNER), None);
        assert_eq!(Caller::notify_target(" "), None);
        assert_eq!(Caller::notify_target("u1").as_deref(), Some("u1"));
    }
}
