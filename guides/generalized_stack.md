# Mezzanine Generalized Stack Boundary

## Responsibility

Mezzanine owns reusable operational truth: packs, binding sets, lifecycle
state, workflow dispatch, source admission, workspace lifecycle, decisions,
evidence, projections, audit, archival, and operator/review read models.

It does not own product UI, product defaults, connector implementation,
credential storage, model/provider SDK behavior, lower lanes, or primitive
storage contracts.

## Public Interfaces

Core public interfaces are grouped by responsibility:

- `core/substrate_model`, `core/pack_model`, and `core/pack_compiler` for pure
  data, validation, manifests, and operation shapes;
- `core/config_registry` for durable installation and binding-set activation;
- `core/lifecycle_engine`, `core/workflow_runtime`,
  `core/runtime_scheduler`, and `core/execution_engine` for run admission,
  dispatch, retries, and recovery;
- `core/source_engine`, `core/object_engine`, and `core/workspace_engine` for
  source/object/workspace contracts;
- `core/evidence_engine`, `core/projection_engine`, `core/audit_engine`, and
  `core/archival_engine` for receipt reduction and read models;
- `bridges/citadel_bridge` and `bridges/integration_bridge` for lower owner
  calls.

## Dependency Rules

Allowed dependencies:

- AppKit bridge contracts at the northbound edge;
- Citadel authority/governance contracts through `bridges/citadel_bridge`;
- Jido Integration lower gateway contracts through `bridges/integration_bridge`;
- GroundPlane primitives for reusable lower refs, leases, fences, persistence
  policy, and projection helpers.

Forbidden dependencies:

- product-specific policy or copy in generic engines;
- direct connector SDK calls outside integration bridge/adapters;
- provider-default dispatch in generic workflow or lifecycle code;
- raw credential material in operation requests or durable rows;
- unsupervised process starts for workers, hooks, schedulers, or listeners.

## Provider Vocabulary Zoning

Provider names are data when they describe external objects, adapter facts,
binding config, receipts, or traces. Generic Mezzanine functions must not branch
on closed provider-family lists. Dispatch should resolve from pack manifests,
binding sets, authority decisions, credential leases, and connector manifests.

## Extravaganza Cutover Proof

Extravaganza is the current reference product for the Mezzanine generic route.
The product-facing names stayed stable, but the lower execution path now passes
through reusable Mezzanine engines and explicit provider-adapter zones.

The live proof covered:

- Linear source candidate discovery, current-state readback, source publication
  create/update fallback/same-state update, and source GraphQL tool execution;
- Codex coding-runtime turn execution;
- GitHub proposed-change evidence collection;
- GitHub proposed-change cleanup, including safe no-match and disposable
  destructive cleanup fixtures;
- aggregate product smoke across all live lanes.

The adapter modules may call provider-specific connector behavior. Generic
workflow, lifecycle, source, evidence, projection, and scheduler code must route
through product role refs, operation descriptors, binding refs, authority refs,
credential lease refs, lower request refs, and receipts. The remaining
`execute_linear_graphql_tool` implementation is deliberately classified as a
Linear adapter boundary inside `bridges/integration_bridge`; it is not a
generic public dispatch API.

## Migration And Deletion Ownership

Mezzanine cleanup work removes old direct provider dispatch, bridge-root
defaults, stale workflow activities, direct runtime calls, and compatibility
modules only after the generic binding-driven path has tests and receipt
evidence. Temporal workflow migration must be history-aware and must not strand
active runs.
