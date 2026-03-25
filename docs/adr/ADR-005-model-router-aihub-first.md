# ADR-005: ModelRouter AI HUB-First with Policy-Gated Ollama Fallback

## Status
Accepted (2026-03-04)

## Context
- The service must run in an offline contour where AI HUB is the primary model runtime.
- Emergency fallback to local Ollama is required for selected urgency classes.
- Direct provider switching without policy and circuit control risks silent quality drift and unstable behavior.

## Decision
1. Introduce `ModelRouter` as the single routing entry point for chat generation.
2. Keep AI HUB as the primary route in policy mode.
3. Allow Ollama only as policy-gated emergency fallback.
4. Add routing telemetry fields in chat/SSE responses:
   - `model_route`
   - `fallback_reason`
   - `fallback_allowed`
   - `fallback_policy_version`
5. Implement routing/reliability modules:
   - `app/services/llm/routing/*`
   - `app/services/llm/reliability/*`
   - `app/services/llm/provider_clients/*`

## Consequences
### Positive
- Routing behavior is deterministic, observable, and policy controlled.
- Incident analysis is improved through route telemetry.
- Silent fallback drift risk is reduced.

### Trade-Offs
- Router and reliability layers add complexity and require stronger test coverage.
- Some legacy assumptions about direct provider usage are no longer valid.
