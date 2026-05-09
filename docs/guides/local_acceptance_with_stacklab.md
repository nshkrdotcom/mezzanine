# Local Acceptance With StackLab

StackLab is the external acceptance and deployment-check harness for the full
stack. It is not part of Mezzanine runtime.

## TRE Lane Acceptance

The neutral TRE lower-lane check proves the public owner path:

```text
StackLab
-> Mezzanine.IntegrationBridge.DirectRunDispatcher.invoke_run_intent/2
-> Jido.Integration.V2.invoke/3
-> Jido.Integration.V2.RuntimeRouter.ExecutionPlaneTreAdapter
-> ExecutionPlane.Process.TreRhai.execute/2
```

Run:

```bash
cd /home/home/p/g/n/stack_lab
mix stack_lab.tre_lane_check \
  --receipt-file /tmp/stack-lab-tre-lane.json
```

With a real installed TRE runner:

```bash
mix stack_lab.tre_lane_check \
  --runner-path /absolute/path/to/rex-runner \
  --receipt-file /tmp/stack-lab-tre-lane-real-runner.json
```

## What This Proves

The check proves that Mezzanine can accept an authorized lower invocation,
build a governed lower envelope, route through Jido Integration, call the
ExecutionPlane TRE adapter, and reduce receipt refs for external acceptance.

It does not prove product UX or product-specific orchestration. Product
acceptance should be layered on top of AppKit and Extravaganza headless or web
surfaces.

## Use StackLab For

- single-node local acceptance checks
- cross-repo lower lane proof
- external harnesses around owner public APIs
- deployment and substrate readiness checks

Do not move production Mezzanine, AppKit, Extravaganza, Citadel, Jido, or
ExecutionPlane behavior into StackLab.
