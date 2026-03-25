# Architecture Guardrails

## Purpose

This document defines non-optional architecture rules for `chat-service / LLM file chat`.
All feature work, bug fixes, and refactors must comply.

## Core Guardrails

1. One module has one primary responsibility.
2. Do not mix routing, business logic, rendering, localization, fallback policy, and debug payload assembly in one module.
3. Low-level runtime modules must not contain user-facing product copy.
4. Runtime schema resolution must be schema-first and metadata-driven.
5. Do not add domain-specific hardcoded field hints for runtime matching.
6. No silent fallback guesses for columns, tables, provider routing, or execution mode.
7. Debug payloads must be assembled through shared builders/helpers only.
8. Changes that increase bloat in already overloaded modules are blocked unless extraction is explicitly justified.

## Forbidden Patterns

- Hardcoded schema aliases tied to one dataset domain in low-level routing or matching code.
- Fallback behavior that silently picks the first matching column/table.
- Branches that return localized copy from low-level execution modules.
- Endpoint handlers that implement deep orchestration logic directly.
- New features appended to large monolith files without extraction plan.

## Oversized File Policy

A module is considered high-risk when any condition is true:
- More than 500 lines of code.
- Cyclomatic complexity that causes unreadable multi-branch control flow.
- Mixed concerns across planning, execution, formatting, and telemetry.

When high-risk, new logic must be extracted into focused modules first.
If extraction is not done, the change must include a written justification in the PR and in `docs/architecture_docs_reconciliation.md` for architecture-level work.

## Testing Contracts (Mandatory)

Add or update tests for every change in:
- Routing behavior.
- Fallback behavior.
- Debug contract payload shape.
- Matching behavior and schema resolution.

Minimum expectation:
- Unit tests for branch logic and error policy.
- Integration tests for API/runtime contract behavior.

## Architecture Definition of Done

A change is done only if all are true:
1. Module boundaries are respected.
2. No forbidden pattern is introduced.
3. Contract tests for routing/fallback/debug/matching are updated.
4. Canonical docs are updated when architecture or contracts changed.
5. New or modified engineering artifacts are English only.
6. No Cyrillic characters were introduced in code, comments, config, rules, or engineering docs.

## Intended Quality Gates

Target CI and review gates:
- Import boundary checks by layer (API, orchestration, runtime, storage).
- Complexity thresholds (function and module level).
- Module size policy checks for oversized files.
- No cyclic dependency checks.
- Contract test suites for routing/fallback/debug and deterministic tabular behavior.

If automated checks are missing, reviewers must enforce these gates manually until CI enforcement is added.
