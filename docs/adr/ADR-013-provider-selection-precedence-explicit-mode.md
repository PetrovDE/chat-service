# ADR-013: Provider Selection Precedence and Explicit Mode

## Context
- The service runs in an offline contour where AI HUB is the default runtime.
- UI allows explicit provider selection (`local/ollama` vs `aihub`).
- A bug caused AI HUB attempts even when user selected local provider.
- Existing policy fallback (`AI HUB -> Ollama`) is still needed for controlled outage handling.

## Decision
1. Introduce routing modes:
- `explicit`: route only to selected provider, no cross-provider fallback.
- `policy`: keep AI HUB-first with policy-gated Ollama fallback.

2. Define provider precedence:
- request payload (`model_source`, optional `provider_mode`)
- conversation state (`model_source`, `model_name`)
- server default (`DEFAULT_MODEL_SOURCE`)

3. Enforce local selection behavior:
- `model_source in {local, ollama}` always resolves to `explicit` mode.
- In this path AI HUB is not attempted.

4. Extend route telemetry:
- `route_mode`, `provider_selected`, `provider_effective`, `fallback_attempted`, `aihub_attempted`
- keep existing fallback fields for compatibility.

## Consequences
- UI provider choice becomes deterministic and authoritative.
- Explicit local requests can run fully autonomous without hidden AI HUB calls.
- Policy fallback remains available only where intended.
- API/observability consumers get clear route diagnostics and migration path for new telemetry fields.
