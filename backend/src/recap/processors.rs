//! The transcription route a recap's audio actually takes, named for the
//! consent record.
//!
//! The sidecar transcribes through `ryu_stt` with no per-request engine, so the
//! engine is exactly `ryu_stt::resolve_stt_engine(None)` (the `RYU_STT_ENGINE`
//! override, else the build's default). The Gateway engine sends audio to a cloud
//! provider, so its label says so and names the provider and model the same way
//! `ryu_stt` picks them.

/// The engine `ryu_stt` picks for a provider when no model is configured.
fn default_gateway_model(provider: Option<&str>) -> &'static str {
    match provider {
        Some("groq") => "whisper-large-v3",
        Some("qwen") => "qwen3-asr-flash",
        _ => "whisper-1",
    }
}

/// A human-readable name for a resolved STT route. `None` for the engine means it
/// could not be resolved, which is named as such rather than as a default.
pub fn transcription_route_label(
    engine: Option<&str>,
    provider: Option<&str>,
    model: Option<&str>,
) -> String {
    match engine {
        Some("parakeet") => "Parakeet (on this Ryu node)".to_string(),
        Some("whisper") => "whisper.cpp (on this Ryu node)".to_string(),
        Some("audiocpp") => "audio.cpp (on this Ryu node)".to_string(),
        Some("gateway") => {
            let model = model.unwrap_or_else(|| default_gateway_model(provider));
            let provider = provider.unwrap_or("default provider");
            format!("Ryu Gateway (cloud: {provider}, {model})")
        }
        Some(other) => format!("{other} (unrecognized engine)"),
        None => "unresolved (check the speech-to-text setting)".to_string(),
    }
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// The route this process's transcription takes right now.
pub fn resolved_transcription_route() -> String {
    let engine = ryu_stt::resolve_stt_engine(None).ok();
    transcription_route_label(
        engine.as_deref(),
        env_value("RYU_STT_GATEWAY_PROVIDER").as_deref(),
        env_value("RYU_STT_GATEWAY_MODEL").as_deref(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_engines_say_they_run_on_the_node() {
        assert!(
            transcription_route_label(Some("parakeet"), None, None).contains("on this Ryu node")
        );
        assert!(transcription_route_label(Some("whisper"), None, None).contains("on this Ryu node"));
        assert!(
            transcription_route_label(Some("audiocpp"), None, None).contains("on this Ryu node")
        );
    }

    #[test]
    fn the_gateway_engine_is_named_as_cloud_with_its_provider_and_model() {
        assert_eq!(
            transcription_route_label(Some("gateway"), Some("groq"), None),
            "Ryu Gateway (cloud: groq, whisper-large-v3)"
        );
        assert_eq!(
            transcription_route_label(Some("gateway"), None, Some("my-asr")),
            "Ryu Gateway (cloud: default provider, my-asr)"
        );
        assert_eq!(
            transcription_route_label(Some("gateway"), None, None),
            "Ryu Gateway (cloud: default provider, whisper-1)"
        );
    }

    #[test]
    fn an_unresolved_engine_is_never_called_default() {
        let label = transcription_route_label(None, None, None);
        assert!(!label.contains("default"));
        assert!(label.starts_with("unresolved"));
    }

    #[test]
    fn the_resolved_route_matches_the_engine_ryu_stt_uses() {
        let engine = ryu_stt::resolve_stt_engine(None).ok();
        assert_eq!(
            resolved_transcription_route(),
            transcription_route_label(
                engine.as_deref(),
                env_value("RYU_STT_GATEWAY_PROVIDER").as_deref(),
                env_value("RYU_STT_GATEWAY_MODEL").as_deref(),
            )
        );
    }
}
