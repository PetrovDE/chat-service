# Agent Instructions

## Required Reading Order

1. `docs/README.md`
2. `docs/architecture_guardrails.md`
3. `docs/module_boundaries.md`
4. Task-specific canonical contracts listed in `docs/README.md`

## Non-Negotiable Rules

- Keep engineering artifacts in English only.
- Do not introduce Cyrillic in code, comments, configs, rules, or engineering docs.
- Keep user-facing localization separate from low-level runtime modules.
- Prefer extraction and decomposition over appending logic to bloated files.
- Do not mix routing, business logic, rendering, localization, fallback policy, and debug assembly in one module.
- Use schema-first, metadata-driven matching logic.
- No silent fallback guesses.

## Before Editing Code

1. Identify canonical source-of-truth docs from `docs/README.md`.
2. Identify target layer responsibility from `docs/module_boundaries.md`.
3. Confirm architecture impact and update canonical docs if needed.
4. Plan tests for routing/fallback/debug/matching changes.

## Test Requirements

Any routing, fallback, debug contract, or matching behavior change requires test updates.

## Documentation Requirements

If architecture or contracts change, update canonical docs in the same change.
Archived docs are historical only and must not be treated as source of truth.
