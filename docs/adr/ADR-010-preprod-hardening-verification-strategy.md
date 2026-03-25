# ADR-010: Pre-Prod Hardening Verification Strategy Without Direct AI HUB Access

## Status
Accepted (2026-03-05)

## Context
- P8 requires end-to-end verification of `AI HUB outage -> controlled fallback -> recovery`.
- In local offline environments, direct AI HUB integration drills are not always available.
- The team still needs deterministic verification of policy and circuit behavior before release.

## Decision
1. Add deterministic end-to-end tests that simulate outage and recovery at `ModelRouter` and `CircuitBreaker` boundaries.
2. Define a separate pre-production gate: mandatory server-side drill against real AI HUB before production sign-off.
3. Keep incident and recovery workflows documented in operational runbooks.

## Consequences
### Positive
- Fallback and recovery behavior is validated consistently in offline development environments.
- Risk of undetected fallback policy regression before pre-production is reduced.
- Runbooks stay aligned with telemetry and SLO expectations.

### Trade-Offs
- Local validation does not cover full real-network/auth behavior of AI HUB.
- Additional server-side pre-production validation remains mandatory.
