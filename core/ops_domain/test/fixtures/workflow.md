---
tracker:
  kind: linear
  endpoint: https://api.linear.app/graphql
run:
  profile: default_session
  runtime_class: session
  capability: codex.session.turn
  target: linear-default
approval:
  mode: manual
  reviewers:
    - ops_lead
    - duty_engineer
  escalation_required: true
retry:
  strategy: exponential
  max_attempts: 4
  initial_backoff_ms: 5000
  max_backoff_ms: 300000
placement:
  profile_id: default-placement
  strategy: affinity
  target_selector:
    runtime_driver: jido_session
  runtime_preferences:
    locality: same_region
workspace:
  root_mode: per_work
  sandbox_profile: strict
review:
  required: true
  required_decisions: 1
  gates:
    - operator
capability_grants:
  - capability_id: codex.session.turn
    mode: allow
  - capability_id: linear.issues.retrieve
    mode: allow
  - capability_id: linear.issues.update
    mode: allow
---
# Operator Prompt

Operate on the assigned work.

- Inspect the available context.
- Execute only the granted capabilities.
- Escalate when review is required.
