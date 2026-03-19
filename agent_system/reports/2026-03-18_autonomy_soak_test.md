# Autonomy Soak Test

## Configuration

- cycles: 6
- interval seconds: 1
- initial delay seconds: 1
- scheduler max cycles: 6

## Result

- pipeline runs completed: 6
- failed runs: 0
- duplicate lead keys: 0
- visible stale rows after soak: 0
- open investigations after soak: 2
- watchlist item count after soak: 19
- follow-up task count after soak: 1
- agent activity row count after soak: 28

## Observations

- cycle 1 added `Ramp / Strategic Programs Lead`
- cycle 2 added `Applied AI Startup / Business Operations Lead` as a new weak-signal lead
- cycle 3 added the `Applied AI Startup` listing batch
- cycles 4-6 were stable no-op discovery cycles
- suppressed rows remained stable and hidden
- watchlist growth stopped after early cycles instead of growing unbounded
- follow-up tasks remained idempotent

## Verdict

The local agent loops are stable enough for bounded unattended demo cycles. They are not yet true set-and-forget production automation, but they no longer show obvious duplicate explosions, stale-row leakage, or runaway activity spam during a short soak.
