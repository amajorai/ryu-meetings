use super::*;

// ── OpenAPI document ───────────────────────────────────────────────────────

/// This app's own manifest, read at compile time. The route contract lives there,
/// so the invariants below compare the document against the real declaration
/// rather than against a second list that could drift from it.
fn openapi_manifest() -> serde_json::Value {
    serde_json::from_str(include_str!("../../../../manifest.json")).expect("valid JSON")
}

/// The manifest sidecar whose HTTP surface this router serves: the one that
/// declares an `http.mount`. Selected BY mount rather than by index because an app
/// may declare a second, mountless sidecar (finetune already does), and
/// `sidecars[0]` would then quietly start asserting against the wrong process.
fn mounted_sidecar() -> serde_json::Value {
    openapi_manifest()["sidecars"]
        .as_array()
        .expect("sidecars must be an array")
        .iter()
        .find(|s| s["http"]["mount"].is_string())
        .expect("one sidecar must declare an http.mount")
        .clone()
}

/// A manifest route (relative to the mount, in axum's `:param` form) rewritten
/// into the form the OpenAPI document uses (absolute, in `{param}` form).
///
/// The two forms differ ON PURPOSE — the router registers paths relative to the
/// mount because Core nests it there, while the `#[utoipa::path]` annotations carry
/// the absolute EXTERNAL path a caller actually hits. Normalise here; do not
/// "align" either side.
fn doc_path_for(mount: &str, route: &str) -> String {
    let joined = if route == "/" {
        mount.to_owned()
    } else {
        format!("{mount}{route}")
    };
    joined
        .split('/')
        .map(|seg| match seg.strip_prefix(':') {
            Some(name) => format!("{{{name}}}"),
            None => seg.to_owned(),
        })
        .collect::<Vec<_>>()
        .join("/")
}

#[test]
fn openapi_doc_is_served_and_non_empty() {
    // The doc is no longer dead code: Core fetches it to derive tools.
    assert!(!super::openapi().paths.paths.is_empty());
}

#[test]
fn every_declared_route_appears_in_the_openapi_doc() {
    // The direction that decides tool yield. Core's `ext_api::lower` keeps only the
    // document operations the manifest ALSO declares, so a declared route with no
    // `#[utoipa::path]` annotation is a tool that silently never exists — nothing
    // errors, the agent simply cannot call it. (The other direction is harmless: an
    // annotated path the manifest does not declare is dropped by the same filter.)
    let sidecar = mounted_sidecar();
    let mount = sidecar["http"]["mount"].as_str().expect("an http.mount");
    let doc = super::openapi();
    for route in sidecar["http"]["routes"]
        .as_array()
        .expect("routes must be an array")
    {
        let path = route["path"].as_str().expect("a route path");
        let expected = doc_path_for(mount, path);
        if super::CAPABILITY_ONLY_ROUTES.contains(&path) {
            continue;
        }
        // Recap routes are deliberately NOT tools: Core would call them with the
        // node token and no caller header, which a local node reads as its owner.
        if path.starts_with(crate::recap::TOOL_EXCLUDED_PREFIX) {
            assert!(
                !doc.paths.paths.contains_key(&expected),
                "'{path}' is a recap route and must stay out of the OpenAPI document"
            );
            continue;
        }
        assert!(
            doc.paths.paths.contains_key(&expected),
            "'{path}' is declared in manifest.json but the OpenAPI document has no \
             '{expected}' operation — Core derives no tool for it"
        );
    }
}

/// Every `provides` row points at a declared POST route, every capability-only
/// route backs a row and stays out of the OpenAPI document, and the set is the
/// one Core consumes.
#[test]
fn provided_capabilities_point_at_declared_post_routes() {
    let manifest = openapi_manifest();
    let sidecar = mounted_sidecar();
    let declared = sidecar["http"]["routes"].as_array().expect("routes");
    let mut names: Vec<&str> = Vec::new();
    let mut provided_routes: Vec<&str> = Vec::new();
    for entry in manifest["provides"].as_array().expect("provides") {
        names.push(entry["capability"].as_str().expect("a capability"));
        assert_eq!(entry["sidecar"], sidecar["name"], "{entry}");
        let route = entry["route"].as_str().expect("a route");
        provided_routes.push(route);
        assert!(
            declared
                .iter()
                .any(|r| r["path"] == route && r["method"] == "POST"),
            "'{route}' is not a declared POST route"
        );
        assert!(entry["grant"]
            .as_str()
            .expect("a grant")
            .starts_with("meetings:"));
    }
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "data.count",
            "data.purge",
            "meetings.appendAudio",
            "meetings.get",
            "meetings.start",
        ]
    );
    let doc = super::openapi();
    let mount = sidecar["http"]["mount"].as_str().expect("an http.mount");
    for route in super::CAPABILITY_ONLY_ROUTES {
        assert!(
            provided_routes.contains(route),
            "'{route}' backs no capability"
        );
        assert!(
            !doc.paths.paths.contains_key(&doc_path_for(mount, route)),
            "'{route}' is capability plumbing and must not become an agent tool"
        );
    }
    let categories = manifest["contributes"]["data_categories"]
        .as_array()
        .expect("data categories");
    assert!(categories.iter().any(|c| c["id"] == super::DATA_CATEGORY));
}

// ─────────────────────────────────────────────────────────────────────────
// OpenAPI → LLM tool derivation
//
// Core derives an LLM tool per route from this document (fetched over
// loopback at `/openapi.json`), and the tool's ARGUMENTS come from the
// operation's `requestBody` schema. Every annotation here used to say
// `request_body = serde_json::Value`, which serialises to `{}` — so every
// write route reached the model as a tool it could see and could not call.
// These tests are the guard: they fail if a body type stops being described.
// ─────────────────────────────────────────────────────────────────────────

/// The JSON body schema documented at `route`/`method`, by pointer (`/` is
/// `~1` inside a pointer segment).
fn body_schema(doc: &serde_json::Value, route: &str, method: &str) -> serde_json::Value {
    let pointer = format!(
        "/paths/{}/{method}/requestBody/content/application~1json/schema",
        route.replace('~', "~0").replace('/', "~1")
    );
    doc.pointer(&pointer)
        .unwrap_or_else(|| panic!("no JSON request body documented at {route} {method}"))
        .clone()
}

#[test]
fn post_routes_document_their_request_body() {
    let doc = serde_json::to_value(openapi()).unwrap();
    for (route, method) in [
        ("/api/meetings", "post"),
        ("/api/meetings/{id}/title", "post"),
        ("/api/meetings/{id}/icon", "post"),
        ("/api/meetings/detect", "post"),
        ("/api/meetings/templates/select", "post"),
        ("/api/meetings/detection-config", "put"),
    ] {
        let schema = body_schema(&doc, route, method);
        // A `$ref` is correct and expected — Core resolves it against
        // `components.schemas` on import.
        assert!(
            schema.get("$ref").is_some() || schema.get("properties").is_some(),
            "a derived write tool for {route} {method} would have no arguments: {schema}"
        );
    }
}

/// The assertion above is necessary but not sufficient: a `$ref` to a type
/// that was never registered looks identical in the operation and still
/// yields zero arguments once Core tries to resolve it. Walks every content
/// type, so the multipart uploads are held to the same bar.
#[test]
fn every_request_body_ref_resolves_against_components() {
    let doc = serde_json::to_value(openapi()).unwrap();
    let schemas = &doc["components"]["schemas"];
    for (route, methods) in doc["paths"].as_object().expect("paths") {
        for (method, op) in methods.as_object().expect("operations") {
            let Some(content) = op.pointer("/requestBody/content") else {
                continue;
            };
            for (media, entry) in content.as_object().expect("content map") {
                let schema = &entry["schema"];
                let Some(reference) = schema.get("$ref").and_then(serde_json::Value::as_str) else {
                    assert!(
                        schema.get("properties").is_some(),
                        "{route} {method} ({media}) documents a body with neither a \
                         $ref nor properties — the model sees no arguments: {schema}"
                    );
                    continue;
                };
                let name = reference
                    .strip_prefix("#/components/schemas/")
                    .unwrap_or_else(|| panic!("{route} {method} points outside this document"));
                assert!(
                    schemas[name].get("properties").is_some(),
                    "{route} {method} refs '{name}', which is missing from \
                     components(schemas(...)) or carries no properties"
                );
            }
        }
    }
}

/// The two upload routes take `multipart/form-data`, not JSON. Saying
/// `application/json` there was a lie in both directions: the request would
/// 400, and Core (which reads only the JSON content) would derive a tool with
/// no arguments anyway. Documented honestly, the route contributes no LLM
/// arguments — the right answer for a binary upload no model can send.
#[test]
fn upload_routes_declare_multipart_not_json() {
    let doc = serde_json::to_value(openapi()).unwrap();
    for (route, schema_name) in [
        ("/api/meetings/{id}/chunk", "ChunkUpload"),
        ("/api/meetings/import", "ImportUpload"),
    ] {
        let content = &doc["paths"][route]["post"]["requestBody"]["content"];
        assert!(
            content.get("application/json").is_none(),
            "{route} still claims a JSON body it would reject"
        );
        assert_eq!(
            content["multipart/form-data"]["schema"]["$ref"],
            serde_json::json!(format!("#/components/schemas/{schema_name}")),
            "{route} lost its multipart form description"
        );
        assert_eq!(
            doc["components"]["schemas"][schema_name]["properties"]["file"]["format"], "binary",
            "{schema_name} no longer describes its file field as binary"
        );
    }
}

/// The payoff of typing the bodies: a `///` on a field becomes the argument
/// description the model actually reads.
#[test]
fn body_field_docs_reach_the_schema_as_argument_descriptions() {
    let doc = serde_json::to_value(openapi()).unwrap();
    let app = &doc["components"]["schemas"]["DetectBody"]["properties"]["app"];
    assert_eq!(
        app["description"],
        "The owning process / app slug (e.g. `zoom`)."
    );
    let icon = &doc["components"]["schemas"]["SetIconBody"]["properties"]["icon"];
    assert!(
        icon["description"]
            .as_str()
            .is_some_and(|d| d.contains("null` to clear")),
        "SetIconBody.icon lost its description: {icon}"
    );
}

/// A handler with no body extractor must declare no body at all — a
/// `request_body` there is a lie that makes the model send one. Its `id`
/// argument still has to survive, and that comes from `params(...)`.
#[test]
fn body_less_routes_declare_no_request_body() {
    let doc = serde_json::to_value(openapi()).unwrap();
    let op = &doc["paths"]["/api/meetings/{id}/finalize"]["post"];
    assert!(
        op.is_object(),
        "the finalize route left the document entirely"
    );
    assert!(
        op.get("requestBody").is_none(),
        "finalize documents a body its handler never reads"
    );
    assert_eq!(
        op["parameters"][0]["name"], "id",
        "finalize lost its id argument: {op}"
    );
}
