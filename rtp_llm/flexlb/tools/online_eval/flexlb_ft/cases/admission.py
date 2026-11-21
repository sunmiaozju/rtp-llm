"""Admission tests cover Engine rejection, Decode admission, Prefill token pressure and queue TTL."""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from ..context import CaseContext, CaseDef, rid_base
from ..engine_ops import (
    clear_type_all,
    engine_inflight_clean,
    inject_type,
    inject_type_all,
)
from ..harness import (
    AssertUtils,
    EnvSpec,
    admission_config,
    build_flexlb_config,
    default_perf,
    http_get_json,
    wait_for,
)

ADMISSION_CASES: list[CaseDef] = []

STREAM_TIMEOUT_S = 15.0
# Mock DEFAULT_TOTAL_KV_TOKENS — squeezing with this value drives
# availableKvCache to 0.
MOCK_TOTAL_KV_TOKENS = 6_291_456


def case(name: str, profiles=None, requires=None, source: str = ""):
    """Register into ADMISSION_CASES (category is always "admission")."""

    def deco(fn):
        ADMISSION_CASES.append(
            CaseDef(
                name=name,
                category="admission",
                fn=fn,
                profiles=profiles,
                requires=requires,
                source=source,
            )
        )
        return fn

    return deco


def _master_http(ops) -> str:
    return f"http://127.0.0.1:{ops.master_http_port}"


def _prefill_names(ops) -> list[str]:
    snap = ops.snapshot()
    return [e["name"] for e in snap.get("engines", []) if e.get("role") == "prefill"]


def _all_engines_busy(ops, names: list[str]) -> bool:
    snap = ops.snapshot_by_name()
    return all(
        snap.get(n, {}).get("waiting", 0) + snap.get(n, {}).get("running", 0) >= 1
        for n in names
    )


# ===========================================================================
# Engine-side queue-depth gate (gap G8)
# ===========================================================================


@case(
    "admission_queue_depth_reject",
    requires=["enqueue_batch"],
    source="gap G8: engine queue_depth admission gate (fast reject + recovery)",
)
def admission_queue_depth(ctx: CaseContext):
    """Engine-side queue_depth gate: once every prefill holds >=1 slow
    pending request, the next enqueue is rejected FAST with "queue depth
    limit exceeded" (-> DISPATCH_FAILED schedule response), NOT an
    unbounded pile-up; after the gate is lifted the occupiers finish and
    a fresh request succeeds with no inflight leak.

    Profile semantics (v2, task #55): the gate is checked only at the
    engine's EnqueueBatch entry (BATCH dispatcher) — the
    GenerateStreamCall path never consults it — so
    requires=["enqueue_batch"] keeps the case to the BATCH-dispatch
    profiles (batch-window, single-batch).  The case runs on the shared
    default env (real per-profile config), so single-batch exercises the
    SINGLE decision axis on the same gate path.
    """
    ops = ctx.ops()
    base = rid_base(ctx, "admission")
    names = _prefill_names(ops)
    if not names:
        return False, "no prefill engines found"

    seed_pool: Optional[ThreadPoolExecutor] = None
    futures = []
    try:
        # Slow prefills keep the seed requests pending long enough.
        for n in names:
            ops.set_perf(n, prefill_fixed_ms=4000.0)
            inject_type(ops, n, "queue_depth", depth=1)

        # Fire seeds until EVERY prefill holds >=1 pending request, so the
        # probe request cannot dodge the gate by landing on a free engine.
        # With depth=1 some seeds may themselves be rejected — their errors
        # are expected and not part of the verdict.
        seed_pool = ThreadPoolExecutor(max_workers=8)
        deadline = time.monotonic() + 15.0
        while not _all_engines_busy(ops, names) and time.monotonic() < deadline:
            rid = ops.next_request_id(base)
            futures.append(
                seed_pool.submit(
                    ops.run_one_request,
                    rid,
                    input_len=512,
                    output_len=2,
                    stream_timeout_s=20.0,
                )
            )
            time.sleep(0.3)
        occupied = _all_engines_busy(ops, names)
        if not occupied:
            return False, "seeds never occupied every prefill engine"

        # Probe: must be fast-rejected with the queue-depth error.
        rid_probe = ops.next_request_id(base)
        t0 = time.monotonic()
        _, err_probe = ops.run_one_request(
            rid_probe, input_len=512, output_len=2, stream_timeout_s=20.0
        )
        reject_latency = time.monotonic() - t0

        # Lift the gate, let the seeds drain, then verify recovery.
        clear_type_all(ops, names, "queue_depth")
        for fut in futures:
            try:
                fut.result(timeout=30.0)
            except Exception:
                pass

        rid_after = ops.next_request_id(base)
        _, err_after = ops.run_one_request(
            rid_after, input_len=512, output_len=2, stream_timeout_s=STREAM_TIMEOUT_S
        )

        inflight_ok, inflight_detail = AssertUtils.inflight_clean(
            _master_http(ops), 30.0
        )
        engine_clean, engine_detail = engine_inflight_clean(ops, names)

        rejected = (
            err_probe is not None
            and "queue depth" in str(err_probe)
            and reject_latency < 3.0
        )
        passed = (
            occupied and rejected and err_after is None and inflight_ok and engine_clean
        )
        return passed, (
            f"all_engines_occupied={occupied}, "
            f"probe_rejected={rejected} "
            f"(latency={reject_latency:.2f}s, err={str(err_probe)[:80]}), "
            f"recovered={err_after is None}, "
            f"master_inflight_clean={inflight_ok}({inflight_detail}), "
            f"engine_inflight_clean={engine_clean}({engine_detail})"
        )
    except Exception as exc:
        return False, f"exception: {exc!r}"
    finally:
        clear_type_all(ops, names, "queue_depth")
        try:
            for n in names:
                ops.set_perf(n, prefill_fixed_ms=100.0)
        except Exception:
            pass
        if seed_pool is not None:
            seed_pool.shutdown(wait=True)


# ===========================================================================
# SLO queue deadline under KV pressure (gap G11a)
# ===========================================================================


def _slo_spec(ctx: CaseContext) -> EnvSpec:
    """G11a env: tight SLO deadline (scheduler.queueTimeoutMs=1500)."""
    return EnvSpec(
        label=f"admission_slo_{ctx.profile}",
        n_prefill=2,
        n_decode=2,
        perf=default_perf(),
        master_profile=ctx.profile,
        master_env={"FLEXLB_CONFIG": admission_config(queue_timeout_ms=1500)},
    )


@case(
    "admission_slo_queue_deadline",
    profiles=["batch-window"],
    source="gap G11: SLO queue deadline + kv_pressure admission (wait-then-expire)",
)
def admission_slo_deadline(ctx: CaseContext):
    """SLO/KV admission: kv_pressure squeezes every prefill's
    availableKvCache to 0; the batcher's KV gate is a WAIT condition
    (admitAndDeliverCapacityFeasiblePrefix -> CapacityBlocked), so with
    scheduler.queueTimeoutMs=1500 the request FAILS with the deadline
    error around 1.5s — fast, terminal, surfaced to the client.  Also
    covers the kv_pressure injection type cross-process (gap G6/G7).

    Recovery: clear kv_pressure and a fresh request must succeed.

    Profile semantics (v2, task #55): the KV gate + queue deadline apply
    to the scheduler queue regardless of the decision/dispatcher axes,
    but _slo_spec pins the legacy fault axes (PRIORITY + FIXED_WINDOW +
    BATCH) via FLEXLB_CONFIG — re-running under another --profile would
    execute the identical configuration, so the declaration stays
    batch-window (label honesty + regression efficiency).
    """
    env = ctx.env_manager.ensure(_slo_spec(ctx))
    ops = ctx.engine_ops(env)
    base = rid_base(ctx, "admission")
    names = _prefill_names(ops)
    if not names:
        return False, "no prefill engines found"
    try:
        inject_type_all(ops, names, "kv_pressure", tokens=MOCK_TOTAL_KV_TOKENS)
        time.sleep(1.0)  # master polls the squeezed worker status

        rid1 = ops.next_request_id(base)
        t0 = time.monotonic()
        _, err1 = ops.run_one_request(
            rid1, input_len=512, output_len=2, stream_timeout_s=12.0
        )
        fail_latency = time.monotonic() - t0

        clear_type_all(ops, names, "kv_pressure")
        time.sleep(1.0)  # status poll refreshes the KV budget

        rid2 = ops.next_request_id(base)
        _, err2 = ops.run_one_request(
            rid2, input_len=512, output_len=2, stream_timeout_s=STREAM_TIMEOUT_S
        )

        inflight_ok, inflight_detail = AssertUtils.inflight_clean(
            _master_http(ops), 20.0
        )
        err_text = str(err1 or "")
        deadline_typed = any(
            kw in err_text.lower()
            for kw in ("deadline", "expired", "exhaust", "8400", "8511", "8431")
        )
        rejected = (
            err1 is not None
            and fail_latency <= 8.0
            and fail_latency >= 1.0
            and deadline_typed
        )
        passed = rejected and err2 is None and inflight_ok
        return passed, (
            f"deadline_rejected={rejected} "
            f"(latency={fail_latency:.2f}s in [1.0, 8.0], err={err_text[:100]}), "
            f"recovered={err2 is None}, "
            f"inflight_clean={inflight_ok}({inflight_detail})"
        )
    except Exception as exc:
        return False, f"exception: {exc!r}"
    finally:
        clear_type_all(ops, names, "kv_pressure")


# ===========================================================================
# Master outstanding-capacity permit (gap G11b)
# ===========================================================================


def _request_capacity_wave(ctx: CaseContext, *, label: str, dispatcher: str,
                         queue_timeout_ms: int):
    """A per-Prefill limit of one parks overflow until completion or queue TTL."""
    config = build_flexlb_config(
        ordering="fifo", decision="single", dispatcher=dispatcher,
        queue_timeout_ms=queue_timeout_ms,
        request_timeout_ms=60_000, decision_lifetime=2.0,
        max_inflight_per_prefill_worker=1,
    )
    env = ctx.env_manager.ensure(EnvSpec(
        label=f"{label}_{ctx.profile}", n_prefill=1, n_decode=2,
        perf=default_perf(), master_profile=ctx.profile,
        master_env={"FLEXLB_CONFIG": config},
    ))
    ops = ctx.engine_ops(env)
    names = _prefill_names(ops)
    if not names:
        return False, "no prefill engine"
    sampler = None
    try:
        for name in names:
            ops.set_perf(name, prefill_fixed_ms=3000.0)
        sampler = _ParkedSampler(ops, names, _decode_names(ops)).start()
        base = rid_base(ctx, "admission")
        request_ids = [ops.next_request_id(base) for _ in range(6)]

        def run(request_id):
            started = time.monotonic()
            response = ops.schedule(request_id, input_len=512, output_len=2)
            scheduled_after = time.monotonic() - started
            if response.code != 200 or not response.success:
                return False, response.code, str(response.error_message), scheduled_after
            input_pb = None if response.enqueued_by_master else ops.build_generate_input(
                request_id, input_len=512, output_len=2)
            handle = ops.start_stream(response, request_id, input_pb=input_pb)
            ended = handle.wait_end(30.0)
            if not ended:
                handle.cancel()
            return bool(ended and handle.snap.completed and not handle.snap.error), response.code, str(handle.snap.error or ""), scheduled_after

        with ThreadPoolExecutor(max_workers=len(request_ids)) as pool:
            outcomes = list(pool.map(run, request_ids))
        sampler.stop()
        served = [item for item in outcomes if item[0]]
        rejected = [item for item in outcomes if not item[0]]
        no_count_rejection = all(code != 503 and "队列满" not in error
                                 for _, code, error, _ in outcomes)
        if queue_timeout_ms < 3000:
            expected = len(served) == 1 and len(rejected) == 5 and all(
                code == 8511 and elapsed >= queue_timeout_ms / 1000.0 * 0.8
                and any(term in error.lower() for term in ("deadline", "expired"))
                for _, code, error, elapsed in rejected)
        else:
            expected = len(served) == len(request_ids) and any(
                elapsed >= 1.0 for _, _, _, elapsed in served)
        clean, clean_detail = AssertUtils.inflight_clean(_master_http(ops), 30.0)
        for name in names:
            ops.set_perf(name, prefill_fixed_ms=100.0)
        _, recovery_error = ops.run_one_request(
            ops.next_request_id(base), input_len=512, output_len=2,
            stream_timeout_s=STREAM_TIMEOUT_S)
        passed = expected and no_count_rejection and sampler.max_parked >= 1 and clean and recovery_error is None
        return passed, (f"served={len(served)}, rejected={rejected}, parked_max={sampler.max_parked}, "
                        f"no_count_rejection={no_count_rejection}, clean={clean}({clean_detail}), "
                        f"recovery_error={recovery_error}")
    except Exception as exc:
        return False, f"exception: {exc!r}"
    finally:
        if sampler is not None:
            sampler.stop()
        for name in names:
            try:
                ops.set_perf(name, prefill_fixed_ms=100.0)
            except Exception:
                pass



@case("admission_global_queue_ttl", profiles=["batch-window"],
      source="Global overflow waits until queue TTL; no global request-count rejection.")
def admission_global_queue_ttl(ctx: CaseContext):
    return _request_capacity_wave(ctx, label="global_queue_ttl", dispatcher="non_batch", queue_timeout_ms=1500)



# ===========================================================================
# Engine prefill-concurrency gate (admission wave-2 W1)
# ===========================================================================


def _fire_request(ops, rid: int, fired: list, **kwargs) -> Optional[str]:
    """Schedule without consuming the stream — under BATCH dispatch the
    master-enqueued ledger entry stays live until the case drains it
    (the kv.py fire-and-forget pattern, kept local for parallel-edit
    decoupling like cancel.py's _schedule_with_priority)."""
    try:
        resp = ops.schedule(rid, **kwargs)
    except Exception as exc:
        return repr(exc)
    if resp.code != 200 or not resp.success:
        return f"schedule failed ({resp.code}): {resp.error_message}"
    fired.append((rid, resp))
    return None


def _drain_fired(ops, fired: list, wait_s: float = 60.0) -> list:
    """Consume every fired request to terminal state (cancel fallback).
    Returns [(rid, completed, err)] in fire order."""
    outcomes = []
    for rid, resp in fired:
        completed = False
        err = None
        try:
            handle = ops.start_stream(resp, rid)
            ended = handle.wait_end(wait_s)
            completed = ended and handle.snap.completed and not handle.snap.error
            if not completed:
                err = handle.snap.error or "stream did not complete"
        except Exception as exc:
            err = repr(exc)
        if not completed:
            try:
                ops.cancel(rid, resp)
            except Exception:
                pass
        outcomes.append((rid, completed, err))
    return outcomes


def _prefill_park_spec(ctx: CaseContext) -> EnvSpec:
    """W1 env: 1 prefill (all batches land on one engine), default
    admission axes; dispatcher maxInflightPerPrefillWorker=4 (the
    build_flexlb_config default) lets several batches reach the engine
    while the engine's maxPrefillConcurrency=1 keeps only one running."""
    return EnvSpec(
        label=f"admit_prefill_park_{ctx.profile}",
        n_prefill=1,
        n_decode=2,
        perf=default_perf(),
        master_profile=ctx.profile,
        master_env={"FLEXLB_CONFIG": admission_config()},
    )


@case(
    "engine_prefill_concurrency_gate_park",
    profiles=["batch-window"],
    requires=["enqueue_batch"],
    source=(
        "admission wave-2 W1: engine prefill-concurrency gate "
        "(maxPrefillConcurrency park — wait condition, no reject)"
    ),
)
def engine_prefill_concurrency_gate_park(ctx: CaseContext):
    """Engine prefill-concurrency gate: the gate is a WAIT condition.

    Scenario: dedicated 1P+2D env.  The single prefill engine runs with
    the production-locked maxPrefillConcurrency=1 (no /set_perf override
    — the lock is the point); prefill_fixed_ms=3000 stretches each
    batch's execution window.  Four requests are fired ~0.4s apart —
    far beyond maxCollectionWaitMs=10, so each arrives as its OWN batch,
    and the dispatcher's maxInflightPerPrefillWorker=4 window
    lets all four EnqueueBatch deliveries reach the engine while batch
    #1 is still running.

    Behaviour: JavaMockEngineCluster.schedulePrefillCompletion admits
    batch #1 (activePrefillBatches 0 -> 1) and parks batches #2-4 whole
    in the unbounded prefillPendingQueue (max_waiting_batches default
    0 = unlimited) — a WAIT, not a rejection: no request-level error is
    surfaced.  As each running batch goes terminal the parked queue
    head is admitted FIFO.

    Expected (contract): every fired request schedules successfully
    (zero rejections — the park absorbs the pressure); a snapshot poll
    observes prefill_waiting_batches >= 1 (the park is real); after the
    drain every fired request reached its terminal as a COMPLETED
    stream (FIFO order), the park is empty (waiting == 0 and
    prefill_waiting_batches == 0), the master inflight ledger is clean
    and a fresh request succeeds (recovery).

    Prediction: expected to pass — engine-side park semantics were
    ported with the v2 mock (schedulePrefillCompletion) and the batch
    lease window (4) strictly exceeds the concurrency window (1), so
    the park is deterministic.  Risk: FIXED_WINDOW coalescing —
    mitigated by the 0.4s inter-fire gap (40x the collection window).
    """
    env = ctx.env_manager.ensure(_prefill_park_spec(ctx))
    ops = ctx.engine_ops(env)
    base = rid_base(ctx, "admission")
    names = _prefill_names(ops)
    if not names:
        return False, "no prefill engines found"
    fired: list = []
    try:
        for n in names:
            ops.set_perf(n, prefill_fixed_ms=3000.0)

        fire_errors = []
        rids = []
        for _ in range(4):
            rid = ops.next_request_id(base)
            rids.append(rid)
            err = _fire_request(ops, rid, fired, input_len=512, output_len=2)
            if err is not None:
                fire_errors.append((rid, err))
            time.sleep(0.4)  # >> maxCollectionWaitMs: one batch per fire

        # Observe the park: batch #1 runs 3s, batches #2-4 sit in
        # prefillPendingQueue — poll before the first terminal.
        park_max = 0
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            snap = ops.snapshot_by_name()
            for n in names:
                info = snap.get(n, {})
                park_max = max(
                    park_max,
                    int(info.get("prefill_waiting_batches", 0)),
                    int(info.get("waiting", 0)),
                )
            if park_max >= 1:
                break
            time.sleep(0.2)

        outcomes = _drain_fired(ops, fired, wait_s=45.0)
        completed = [rid for rid, ok, _ in outcomes if ok]
        drain_errors = [(rid, err) for rid, ok, err in outcomes if not ok]

        # Park must settle back to empty after the drain.
        def park_empty() -> bool:
            snap = ops.snapshot_by_name()
            return all(
                int(snap.get(n, {}).get("prefill_waiting_batches", 0)) == 0
                and int(snap.get(n, {}).get("waiting", 0)) == 0
                for n in names
            )

        settled = wait_for(park_empty, 10.0, 0.2)
        inflight_ok, inflight_detail = AssertUtils.inflight_clean(
            _master_http(ops), 30.0
        )
        recovery_ok, recovery_msg = ops.verify_recovery()

        passed = (
            not fire_errors
            and park_max >= 1
            and len(completed) == len(rids)
            and not drain_errors
            and settled
            and inflight_ok
            and recovery_ok
        )
        return passed, (
            f"fired={len(rids)} (fire_errors={fire_errors[:1]}), "
            f"park_observed_max={park_max}, "
            f"completed={len(completed)}/{len(rids)} "
            f"(drain_errors={drain_errors[:1]}), "
            f"park_settled_empty={settled}, "
            f"inflight_clean={inflight_ok}({inflight_detail}), "
            f"recovery={recovery_msg}"
        )
    except Exception as exc:
        return False, f"exception: {exc!r}"
    finally:
        try:
            for n in names:
                ops.set_perf(n, prefill_fixed_ms=100.0)
        except Exception:
            pass


# ===========================================================================
# Engine decode hard gate (admission wave-2 W2)
# ===========================================================================


DECODE_WAVE_REQUESTS = 280  # gate-reachability anchor — see case docstring
DECODE_HARD_GATE = 128  # mock engine decodeMaxConcurrency (CLI default)


def _decode_park_spec(ctx: CaseContext) -> EnvSpec:
    """W2 env: 2P+1D (all decode traffic concentrates on one engine).

    The master-side decode routing cap is raised far above the engine's
    128 hard gate (maxEngineRequests 132 -> 5000) so the ENGINE gate is
    the only admission edge in play: every fired request is routed and
    delivered, and whatever overflows 128 running slots must park in the
    engine's decodePendingQueue instead of being bounced anywhere."""
    config = json.loads(admission_config())
    config["router"]["roles"]["decode"]["availability"]["maxEngineRequests"] = 5000
    return EnvSpec(
        label=f"admit_decode_park_{ctx.profile}",
        n_prefill=2,
        n_decode=1,
        perf=default_perf(),
        master_profile=ctx.profile,
        master_env={"FLEXLB_CONFIG": json.dumps(config, separators=(",", ":"))},
    )


def _decode_names(ops) -> list[str]:
    snap = ops.snapshot()
    return [e["name"] for e in snap.get("engines", []) if e.get("role") == "decode"]


@case(
    "engine_decode_hard_gate_unbounded_park",
    profiles=["batch-window"],
    requires=["enqueue_batch"],
    source=(
        "admission wave-2 W2: engine decode hard gate "
        "(decodeMaxConcurrency=128 unbounded park — no queue-pressure reject)"
    ),
)
def engine_decode_hard_gate_unbounded_park(ctx: CaseContext):
    """Engine decode hard gate: unbounded park, never a queue-pressure
    rejection.

    Scenario: dedicated 2P+1D env with the master decode routing cap
    raised to 5000, so the ONLY admission edge is the engine's
    decodeMaxConcurrency=128 hard gate (the production waiting_streams_
    semantics).  decode_scale=10 stretches each decode step
    ((19.5 + 0.175 x running) ms x 10) and output_len=64 keeps every
    request ~25 decode steps (≈ 10.4s residency at the gate, 2.6
    tokens/step MTP fold) in the engine.

    Gate-reachability construction (2026-09-05 revision, verdict §4.5
    option (a)): the 2026-09-04 construction (150 fires x output_len=32)
    measured decode_running_max=78 (< gate 128) with decode_waiting_max=0
    — that residency did not stack past the gate, so the park proof
    never activated (the half-false-green finding).  The load is
    re-anchored on the measured arrival model: steady-state running =
    arrival_rate x residency, with arrival ≈ 19-25 req/s (prefill-bound,
    back-solved from running_max=78 @ residency ≈ 4.1s) and residency
    scaling linearly in output_len.  Doubling the residency
    (output_len=64, ≈ 10.4s at the gate) and deepening the wave to 280
    fires puts the modelled peak at running ≈ 155-180 > 128 with the
    overflow (~30-50 requests) parked in decodePendingQueue.  If a
    future run STILL measures sub-gate (engine-version drift, slower
    machine), the gate-aware clause below degrades to the invariant
    half ONLY and the measured running_max is reported as the next
    calibration anchor — an honest no-proof pass, never a fabricated
    one (the fallback semantics verdict §4.5 endorses).

    Behaviour: scheduleDecodeCompletion admits the first 128
    TransferToDecode arrivals as running and parks every overflow in
    the UNBOUNDED decodePendingQueue (no cap, no rejection — unlike
    the prefill gate this queue never bounces a request under queue
    pressure).  As running slots free up, parked requests are admitted
    wave by wave.

    Expected (contract): all 280 Schedule calls succeed (zero
    rejections — the engine-side form of a waitable gate); IF the
    observed decode_running_max reaches the 128 gate a snapshot poll
    must also observe decode waiting >= 1 (the overflow parked — a
    filled gate with no park is a vanished-overflow failure); after the
    drain >= 95% of the fired requests completed their streams, the
    decode park is empty (waiting == 0), the master inflight ledger is
    clean and a fresh request succeeds (recovery).

    Prediction: the invariant half (zero rejections, >= 95% drain,
    clean ledgers) is load-level independent and holds; the park half
    activates on the modelled running peak ≈ 155-180 (anchor above).
    Drain budget: 280 x 64 = 17,920 decode tokens at the ≈ 790 tok/s
    full-gate rate ≈ 23s plus the sub-gate tail, under the 45s
    per-stream cap; if a slow machine stretches waves the completion
    bar is the 95% ratio, not perfection.
    """
    env = ctx.env_manager.ensure(_decode_park_spec(ctx))
    ops = ctx.engine_ops(env)
    base = rid_base(ctx, "admission")
    decode_engines = _decode_names(ops)
    if not decode_engines:
        return False, "no decode engines found"
    fired: list = []
    try:
        for n in decode_engines:
            ops.set_perf(n, decode_scale=10.0)

        fire_errors = []
        for i in range(DECODE_WAVE_REQUESTS):
            rid = ops.next_request_id(base)
            err = _fire_request(ops, rid, fired, input_len=512, output_len=64)
            if err is not None:
                fire_errors.append((rid, err))
            if (i + 1) % 25 == 0:
                time.sleep(0.05)  # tiny pacing, keeps batches flowing

        # Observe the park across the full fill window (no early exit:
        # waiting_max and running_max are both peak gauges — the pair
        # proves the gate filled and overflowed, which a waiting-only
        # early break would truncate; under a sub-gate wave both stay
        # low and the gauges record the load shape as diagnostics).
        # The 18s window spans the ~11-15s prefill supply phase plus the
        # stack to the modelled peak (see the construction note above).
        waiting_max = 0
        running_max = 0
        deadline = time.monotonic() + 18.0
        while time.monotonic() < deadline:
            snap = ops.snapshot_by_name()
            for n in decode_engines:
                info = snap.get(n, {})
                waiting_max = max(waiting_max, int(info.get("waiting", 0)))
                running_max = max(running_max, int(info.get("running", 0)))
            time.sleep(0.2)

        outcomes = _drain_fired(ops, fired, wait_s=45.0)
        completed = sum(1 for _, ok, _ in outcomes if ok)
        drain_errors = [(rid, err) for rid, ok, err in outcomes if not ok]

        def decode_park_empty() -> bool:
            snap = ops.snapshot_by_name()
            return all(
                int(snap.get(n, {}).get("waiting", 0)) == 0 for n in decode_engines
            )

        settled = wait_for(decode_park_empty, 15.0, 0.3)
        inflight_ok, inflight_detail = AssertUtils.inflight_clean(
            _master_http(ops), 60.0
        )
        recovery_ok, recovery_msg = ops.verify_recovery()

        completion_ratio = completed / DECODE_WAVE_REQUESTS
        # Gate-aware park proof: when the wave stacked past the gate the
        # overflow MUST be observable as decode waiting (parked, never
        # rejected or dropped); a sub-gate wave (running_max < gate)
        # parks nothing by construction and only the zero-rejection /
        # full-drain invariant applies.
        gate_filled = running_max >= DECODE_HARD_GATE
        park_proven = waiting_max >= 1 if gate_filled else True
        passed = (
            not fire_errors
            and park_proven
            and completion_ratio >= 0.95
            and settled
            and inflight_ok
            and recovery_ok
        )
        return passed, (
            f"fired={DECODE_WAVE_REQUESTS} "
            f"(fire_errors={len(fire_errors)}, first={fire_errors[:1]}), "
            f"decode_waiting_max={waiting_max}, decode_running_max={running_max} "
            f"(gate={DECODE_HARD_GATE}, gate_filled={gate_filled}, "
            f"park_proven={park_proven}), "
            f"completed={completed}/{DECODE_WAVE_REQUESTS} "
            f"({completion_ratio:.0%}, drain_errors={drain_errors[:2]}), "
            f"park_settled_empty={settled}, "
            f"inflight_clean={inflight_ok}({inflight_detail}), "
            f"recovery={recovery_msg}"
        )
    except Exception as exc:
        return False, f"exception: {exc!r}"
    finally:
        try:
            for n in decode_engines:
                ops.set_perf(n, decode_scale=1.0)
        except Exception:
            pass


# ===========================================================================
# PRIORITY incomer fast reject without preemption (admission wave-2 W3)
# ===========================================================================


def _schedule_with_priority(ops, request_id: int, priority: int, **kwargs):
    """Schedule RPC carrying an explicit priority (proto field 14) — the
    same local copy as cancel.py's (protobuf messages are mutable;
    engine_ops is owned by another agent in this wave)."""
    req = ops.build_schedule_request(request_id, **kwargs)
    req.priority = priority
    stub = ops.schedule_pb2_grpc.FlexlbServiceStub(ops._channel(ops.master_target()))
    return stub.Schedule(req, timeout=30.0)


def _incomer_spec(ctx: CaseContext) -> EnvSpec:
    """PRIORITY without preemption: concurrent Decode requests share the retained worker request/KV caps."""
    config = json.loads(admission_config())
    return EnvSpec(
        label=f"admit_incomer_{ctx.profile}",
        n_prefill=1,
        n_decode=1,
        perf=default_perf(),
        master_profile=ctx.profile,
        master_env={"FLEXLB_CONFIG": json.dumps(config, separators=(",", ":"))},
    )


@case(
    "admission_priority_concurrent_decode",
    profiles=["batch-window"],
    requires=["enqueue_batch"],
    source="PRIORITY without preemption admits concurrent Decode requests within resource caps.",
)
def admission_priority_concurrent_decode(ctx: CaseContext):
    """A higher-priority request runs alongside an accepted Decode request; neither preempts the other."""
    env = ctx.env_manager.ensure(_incomer_spec(ctx))
    ops = ctx.engine_ops(env)
    base = rid_base(ctx, "admission")
    decode_engines = _decode_names(ops)
    if not decode_engines:
        return False, "no decode engines found"
    victim_handle = None
    incomer_handle = None
    try:
        # Victim takes the single admission permit (held until terminal).
        victim_rid = ops.next_request_id(base)
        victim_keys = [victim_rid * 100 + 1]
        victim_resp = _schedule_with_priority(
            ops,
            victim_rid,
            30,
            input_len=512,
            output_len=200,
            block_keys=victim_keys,
        )
        if victim_resp.code != 200 or not victim_resp.success:
            return False, (f"victim schedule failed: {victim_resp.error_message}")
        victim_input = (
            None
            if victim_resp.enqueued_by_master
            else ops.build_generate_input(
                victim_rid,
                input_len=512,
                output_len=200,
                block_keys=victim_keys,
            )
        )
        victim_handle = ops.start_stream(victim_resp, victim_rid, input_pb=victim_input)

        # Victim must be past prefill and RUNNING on decode before the
        # incomer arrives — otherwise the probe would race the transfer.
        def victim_running() -> bool:
            snap = ops.snapshot_by_name()
            return any(
                int(snap.get(n, {}).get("running", 0)) >= 1 for n in decode_engines
            )

        running = wait_for(victim_running, 10.0, 0.1)
        if not running:
            return False, "victim never reached RUNNING on decode"

        # Incomer: the acceptance permit freed at the victim's
        # DecodeAccepted event, so the incomer is ADMITTED fast and runs
        # alongside the victim (new-B semantics).
        incomer_rid = ops.next_request_id(base)
        incomer_keys = [incomer_rid * 100 + 1]
        t0 = time.monotonic()
        incomer_resp = _schedule_with_priority(
            ops,
            incomer_rid,
            70,
            input_len=512,
            output_len=2,
            block_keys=incomer_keys,
        )
        accept_latency = time.monotonic() - t0
        incomer_msg = str(incomer_resp.error_message)
        incomer_accepted = incomer_resp.code == 200 and incomer_resp.success
        if incomer_accepted:
            incomer_input = (
                None
                if incomer_resp.enqueued_by_master
                else ops.build_generate_input(
                    incomer_rid,
                    input_len=512,
                    output_len=2,
                    block_keys=incomer_keys,
                )
            )
            incomer_handle = ops.start_stream(
                incomer_resp, incomer_rid, input_pb=incomer_input
            )

        # Victim finishes unmolested (no preemption -> no 8429); the
        # incomer completes alongside it.
        victim_ended = victim_handle.wait_end(30.0)
        victim_completed = (
            victim_ended
            and victim_handle.snap.completed
            and not victim_handle.snap.error
        )
        incomer_completed = False
        if incomer_handle is not None:
            incomer_ended = incomer_handle.wait_end(30.0)
            incomer_completed = (
                incomer_ended
                and incomer_handle.snap.completed
                and not incomer_handle.snap.error
            )
        incomer_outputs = (
            len(incomer_handle.snap.outputs) if incomer_handle is not None else 0
        )

        # Post-wave hygiene: a fresh request must still succeed.
        recovery_ok, recovery_msg = ops.verify_recovery()
        inflight_ok, inflight_detail = AssertUtils.inflight_clean(
            _master_http(ops), 30.0
        )
        engine_clean, engine_detail = engine_inflight_clean(
            ops, _prefill_names(ops) + decode_engines, 15.0
        )

        accepted_fast = accept_latency < 3.0
        passed = (
            incomer_accepted
            and accepted_fast
            and incomer_completed
            and victim_completed
            and recovery_ok
            and inflight_ok
            and engine_clean
        )
        return passed, (
            f"incomer_accepted={incomer_accepted} "
            f"(code={incomer_resp.code}, latency={accept_latency:.2f}s, "
            f"msg={incomer_msg[:80]}), "
            f"incomer_completed={incomer_completed} "
            f"(outputs={incomer_outputs}), "
            f"victim_completed={victim_completed} "
            f"(outputs={len(victim_handle.snap.outputs)}), "
            f"recovery={recovery_msg}, "
            f"inflight_clean={inflight_ok}({inflight_detail}), "
            f"engine_clean={engine_clean}({engine_detail})"
        )
    except Exception as exc:
        return False, f"exception: {exc!r}"
    finally:
        if incomer_handle is not None:
            incomer_handle.cancel()
        if victim_handle is not None:
            victim_handle.cancel()


# ===========================================================================
# Master batcher-queue capacity gate (admission wave-2 A5)
# ===========================================================================


def _master_side_parked(ops, prefill_names, decode_names) -> tuple:
    """Requests live on the master ledger but absent from every engine.

    The A5-vs-W1 discriminator: a request parked on the ENGINE shows up
    in the engine snapshot's waiting/running, while a request parked on
    the MASTER (batcher queue depth / placementWaiters) only inflates
    the scheduler ledger — nothing else can see it.
    """
    data = http_get_json(f"{_master_http(ops)}/rtp_llm/inflight_status", timeout=5)
    if data is None:
        return -1, "inflight_status unavailable"
    sched = int(data.get("scheduler_inflight", 0))
    snap = ops.snapshot_by_name()

    def live(n: str) -> int:
        info = snap.get(n, {})
        return int(info.get("waiting", 0)) + int(info.get("running", 0))

    engine_live = sum(live(n) for n in prefill_names)
    engine_live += sum(live(n) for n in decode_names)
    return sched - engine_live, f"sched={sched}, engine_live={engine_live}"


def _fire_tracked(
    ops, rid: int, fired: list, timeout_s: float = 30.0, **kwargs
) -> Optional[str]:
    """Schedule AND immediately open the response stream, recording the
    fire instant so per-request terminal timing (completion order,
    deadline expiry) is measurable — the W3 victim-handle pattern,
    batched.  Fire errors are reported like _fire_request's.

    ``timeout_s`` is the client gRPC deadline for the Schedule RPC: a
    capacity-blocked submitter parks as a Blocked request with the RPC
    still OPEN (verdict §1.1), so callers that expect parking pass a
    generous deadline.  NON_BATCH responses (enqueued_by_master=False)
    open the direct GenerateStreamCall with an input_pb rebuilt from
    the SAME kwargs — start_stream's default-shape fallback would
    desynchronize the engine's view from the master's schedule (the
    trap engine_ops.verify_recovery documents)."""
    try:
        resp = ops.schedule(rid, timeout_s=timeout_s, **kwargs)
    except Exception as exc:
        return repr(exc)
    if resp.code != 200 or not resp.success:
        return f"schedule failed ({resp.code}): {resp.error_message}"
    input_pb = None
    if not resp.enqueued_by_master:
        input_pb = ops.build_generate_input(rid, **kwargs)
    try:
        handle = ops.start_stream(resp, rid, input_pb=input_pb)
    except Exception as exc:
        return repr(exc)
    fired.append((rid, handle, time.monotonic()))
    return None


class _ParkedSampler:
    """Background master-side-parked time-series sampler (verdict §3 fix).

    The parked window lives DURING the concurrent fire: a Blocked park
    holds the Schedule RPC open and the park drains the moment capacity
    frees, so a sampler that only starts AFTER the fires settle
    structurally misses the window — the 2026-09-04 run measured
    parked_max=0 on exactly that defect while the wave demonstrably
    queued.  This thread samples :func:`_master_side_parked` every
    ``interval_s`` while the fires are in flight (the decode_hard_gate
    L863-872 poll-loop cadence on the master.py coldstart-sampler
    thread shape), recording ``(monotonic_t, parked, detail)`` samples;
    the caller asserts on the series AFTER joining the fires.  HTTP
    failures (parked == -1) are counted, never recorded as samples.
    """

    def __init__(self, ops, prefill_names, decode_names, interval_s: float = 0.2):
        self._ops = ops
        self._prefill = list(prefill_names)
        self._decode = list(decode_names)
        self._interval = interval_s
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.samples: list[tuple[float, int, str]] = []
        self.http_failures = 0

    def _loop(self) -> None:
        while not self._stop.is_set():
            parked, detail = _master_side_parked(self._ops, self._prefill, self._decode)
            if parked < 0:
                self.http_failures += 1
            else:
                self.samples.append((time.monotonic(), parked, detail))
            self._stop.wait(self._interval)

    def start(self) -> "_ParkedSampler":
        self._thread = threading.Thread(
            target=self._loop, name="parked-sampler", daemon=True
        )
        self._thread.start()
        return self

    def stop(self, timeout_s: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout_s)

    @property
    def max_parked(self) -> int:
        return max((p for _, p, _ in self.samples), default=-1)

    @property
    def max_detail(self) -> str:
        best = max(self.samples, key=lambda s: s[1], default=None)
        return best[2] if best is not None else "no samples"

def _await_tracked(fired: list, wait_s: float = 45.0) -> list:
    """Concurrently await every tracked stream.

    Returns [(rid, fire_t, end_t, completed, err)] in fire order, with
    fire/end as monotonic instants so callers can assert per-request
    timing.  Unfinished streams are cancelled (drain hygiene)."""

    def _one(item):
        rid, handle, t0 = item
        try:
            ended = handle.wait_end(wait_s)
            completed = ended and handle.snap.completed and not handle.snap.error
            err = (
                None if completed else (handle.snap.error or "stream did not complete")
            )
        except Exception as exc:
            completed, err = False, repr(exc)
        if not completed:
            try:
                handle.cancel()
            except Exception:
                pass
        return (rid, t0, time.monotonic(), completed, err)

    if not fired:
        return []
    with ThreadPoolExecutor(max_workers=len(fired)) as pool:
        return list(pool.map(_one, fired))






@case("admission_prefill_request_capacity_park", profiles=["batch-window"], requires=["enqueue_batch"],
      source="BATCH maxInflightPerPrefillWorker limits concurrent batches; completion wakes worker-queue waiters.")
def admission_prefill_request_capacity_park(ctx: CaseContext):
    return _request_capacity_wave(ctx, label="prefill_request_park", dispatcher="batch", queue_timeout_ms=60000)



@case("admission_prefill_request_capacity_deadline", profiles=["batch-window"], requires=["enqueue_batch"],
      source="Request-capacity backpressure preserves the queue deadline; already delivered work retains ownership.")
def admission_prefill_request_capacity_deadline(ctx: CaseContext):
    return _request_capacity_wave(ctx, label="prefill_request_deadline", dispatcher="batch", queue_timeout_ms=1500)



# ===========================================================================
# Master placement pool gate (admission wave-2 A4)
# ===========================================================================






@case("admission_nonbatch_request_capacity_wait", profiles=["single-nonbatch"], requires=["generate_stream"],
      source="NON_BATCH maxInflightPerPrefillWorker limits outstanding requests; completion resumes global waiters.")
def admission_nonbatch_request_capacity_wait(ctx: CaseContext):
    return _request_capacity_wave(ctx, label="nonbatch_request_wait", dispatcher="non_batch", queue_timeout_ms=60000)



# ===========================================================================
# Engine waiting-batch cap gate (admission wave-3 B3)
# ============================================================================


def _waiting_cap_spec(ctx: CaseContext) -> EnvSpec:
    """B3 env: 1 prefill (every batch lands on one engine, so the cap
    pressure is concentrated), the legacy fault axes and default
    admission knobs — the waiting-queue cap itself is applied at RUNTIME
    via /set_perf max_waiting_batches (ef76751553), so the env shape is
    the plain W1 one."""
    return EnvSpec(
        label=f"admit_wcap_{ctx.profile}",
        n_prefill=1,
        n_decode=2,
        perf=default_perf(),
        master_profile=ctx.profile,
        master_env={"FLEXLB_CONFIG": admission_config()},
    )


@case(
    "admission_engine_waiting_batch_cap_reject",
    profiles=["batch-window"],
    requires=["enqueue_batch"],
    source=(
        "admission wave-3 B3: engine prefill waiting-queue cap "
        "(max_waiting_batches whole-batch backpressure reject — the "
        "non-waitable complement of W1's unbounded park)"
    ),
)
def admission_engine_waiting_batch_cap_reject(ctx: CaseContext):
    """Engine waiting-batch cap gate: the cap is NOT a wait condition.

    Scenario: dedicated 1P+2D env (the W1 shape); prefill_fixed_ms=3000
    stretches each batch's execution window and /set_perf applies the
    runtime cap max_waiting_batches=1 (>0 = queued-batch ceiling, 0 =
    unbounded — the default).  Three requests are fired 0.4s apart (each
    its own batch, 40x the 10ms collection window): batch #1 is admitted
    running, batch #2 parks in prefillPendingQueue (waiting = 1 = cap —
    a snapshot poll proves the saturated state BEFORE the probe fires),
    and batch #3 finds waiting >= cap at schedulePrefillCompletion.

    Behaviour: the cap hit is a WHOLE-BATCH backpressure reject, not a
    park — schedulePrefillCompletion returns false before claiming any
    counter, every member of batch #3 is rolled back and the
    EnqueueBatch ack carries the batch-level error "prefill waiting
    queue full (backpressure): waiting=1 cap=1"; DefaultBatchDispatcher
    wraps it as EngineRejectedException ("EnqueueBatch rejected request
    N: prefill waiting queue full ...") and the master completes the
    request terminal — synchronous, typed, no queueing.  The cap counts
    QUEUED batches only (running is not charged), so the two in-flight
    occupants are untouched.

    Expected (contract): the probe terminates FAST (< 3s from fire,
    no park residence) with the backpressure error family ("prefill
    waiting queue full" + "backpressure") in its terminal error; the
    occupants (1 running + 1 queued) complete normally; the gate
    RECOVERS UNDER THE SAME PRESSURE — with batches #1/#2 still
    occupying the engine, /set_perf max_waiting_batches=0 (unbounded)
    lets a fourth fire park in the queue and run to completion; after
    the drain the engine park is empty, the master inflight ledger is
    clean and a fresh request succeeds (recovery).

    Prediction: expected to pass — the runtime override, the cap check
    (waiting >= cap rejects before claiming any counter) and the
    ack-error surface are covered by SetPerfMaxWaitingBatchesTest (4/4
    green); the only novel wiring is the master's
    EngineRejectedException-to-terminal path, already exercised by the
    enqueue-ack fault family.  Risk: batch coalescing collapsing the
    fires into fewer batches — mitigated by the 0.4s inter-fire gap and
    the pre-probe waiting>=1 observation (the case fails loudly rather
    than probing an unsaturated cap).
    """
    env = ctx.env_manager.ensure(_waiting_cap_spec(ctx))
    ops = ctx.engine_ops(env)
    base = rid_base(ctx, "admission")
    names = _prefill_names(ops)
    if not names:
        return False, "no prefill engines found"
    fired: list = []
    try:
        for n in names:
            ops.set_perf(n, prefill_fixed_ms=3000.0, max_waiting_batches=1)

        # Occupants: batch #1 running (3s window), batch #2 queued —
        # together they saturate waiting=1=cap.
        fire_errors = []
        for _ in range(2):
            rid = ops.next_request_id(base)
            err = _fire_tracked(ops, rid, fired, input_len=512, output_len=2)
            if err is not None:
                fire_errors.append((rid, err))
            time.sleep(0.4)  # >> maxCollectionWaitMs: one batch per fire
        if fire_errors:
            return False, f"occupant fire failed: {fire_errors[:1]}"

        # Cap-state proof BEFORE the probe: batch #2 sits in
        # prefillPendingQueue (waiting=1=cap).
        def cap_saturated() -> bool:
            snap = ops.snapshot_by_name()
            return all(
                int(snap.get(n, {}).get("prefill_waiting_batches", 0)) >= 1
                for n in names
            )

        cap_observed = wait_for(cap_saturated, 8.0, 0.1)
        if not cap_observed:
            return (
                False,
                "batch #2 never reached the waiting queue (cap never saturated)",
            )

        # Probe batch #3: whole-batch backpressure reject.  The master
        # surfaces the engine's EnqueueBatch reject SYNCHRONOUSLY on the
        # Schedule RPC — code 8510, "Delivery failed: EnqueueBatch rejected
        # request N: prefill waiting queue full (backpressure): waiting=1
        # cap=1" (DefaultBatchDispatcher wraps the ack error and the RPC
        # returns it to the caller) — so the probe accepts EITHER surface:
        # the RPC-level typed reject, or a successful fire whose stream
        # then terminates with the backpressure family.
        rid3 = ops.next_request_id(base)
        r3_t0 = time.monotonic()
        r3_err = ""
        try:
            resp3 = ops.schedule(rid3, input_len=512, output_len=2)
            if resp3.code != 200 or not resp3.success:
                r3_err = f"schedule failed ({resp3.code}): " f"{resp3.error_message}"
            else:
                handle3 = ops.start_stream(resp3, rid3)
                ended3 = handle3.wait_end(10.0)
                r3_err = str(handle3.snap.error or "") if ended3 else "no terminal"
        except Exception as exc:
            r3_err = repr(exc)
        reject_latency = time.monotonic() - r3_t0
        rejected = (
            "prefill waiting queue full" in r3_err.lower()
            and "backpressure" in r3_err.lower()
            and reject_latency < 3.0
        )

        # Gate recovery UNDER THE SAME PRESSURE: relax the cap while
        # batch #1 still runs and batch #2 still queues — the next fire
        # must park in the (now unbounded) queue and complete.
        for n in names:
            ops.set_perf(n, max_waiting_batches=0)
        snap_at_r4 = ops.snapshot_by_name()
        waiting_at_r4 = max(
            int(snap_at_r4.get(n, {}).get("prefill_waiting_batches", 0)) for n in names
        )
        rid4 = ops.next_request_id(base)
        fire_err4 = _fire_tracked(ops, rid4, fired, input_len=512, output_len=2)

        outcomes = _await_tracked(fired, wait_s=45.0)
        occupant_ok = len(outcomes) >= 2 and all(
            ok and err is None for _, _, _, ok, err in outcomes[:2]
        )
        # rid4 is fired[-1] whenever its fire succeeded; a synchronously
        # rejected probe (rid3) never enters `fired`, so index by identity,
        # not by ordinal.
        recovers_ok = (
            fire_err4 is None
            and len(fired) >= 3
            and outcomes[-1][0] == rid4
            and outcomes[-1][3]
            and outcomes[-1][4] is None
        )

        def park_empty() -> bool:
            snap = ops.snapshot_by_name()
            return all(
                int(snap.get(n, {}).get("prefill_waiting_batches", 0)) == 0
                and int(snap.get(n, {}).get("waiting", 0)) == 0
                for n in names
            )

        settled = wait_for(park_empty, 10.0, 0.2)
        inflight_ok, inflight_detail = AssertUtils.inflight_clean(
            _master_http(ops), 30.0
        )
        recovery_ok, recovery_msg = ops.verify_recovery()

        passed = (
            cap_observed
            and rejected
            and occupant_ok
            and recovers_ok
            and settled
            and inflight_ok
            and recovery_ok
        )
        return passed, (
            f"cap_observed={cap_observed}, "
            f"probe_rejected={rejected} "
            f"(latency={reject_latency:.2f}s, err={r3_err[:80]}), "
            f"occupants_completed={occupant_ok}, "
            f"gate_recovers_under_pressure={recovers_ok} "
            f"(waiting_at_r4={waiting_at_r4}, fire_err4={fire_err4}), "
            f"park_settled_empty={settled}, "
            f"inflight_clean={inflight_ok}({inflight_detail}), "
            f"recovery={recovery_msg}"
        )
    except Exception as exc:
        return False, f"exception: {exc!r}"
    finally:
        try:
            for n in names:
                ops.set_perf(n, prefill_fixed_ms=100.0, max_waiting_batches=0)
        except Exception:
            pass


# ===========================================================================
# Engine prefill KV block-pool gate (admission wave-3 B2)
# ============================================================================


LACKMEM_POOL_BLOCKS = 17  # reserve=ceil(5% x 17)=1: two 8-block leases fit
LACKMEM_KEYS_PER_REQUEST = 8  # per-request block_cache_keys count (= need)


def _lack_mem_spec(ctx: CaseContext) -> EnvSpec:
    """B2 env: 1 prefill with a 17-block KV pool (KV v2 block pool:
    reserve = ceil(5% x 17) = 1 block).  Two 8-block leases fill the
    pool (8+8 = 16 held of 17 — the remaining free block sits below the
    reserve margin), so a THIRD 8-block request fails the
    TOTAL_AND_AVAILABLE gate.  The decode pool stays at the harness
    default: decode-side admission (ceil(512/1024) = 1 block) must
    never interfere with the prefill gate."""
    return EnvSpec(
        label=f"admit_lackmem_{ctx.profile}",
        n_prefill=1,
        n_decode=2,
        perf=default_perf(),
        master_profile=ctx.profile,
        master_env={"FLEXLB_CONFIG": admission_config()},
        prefill_cache_blocks=LACKMEM_POOL_BLOCKS,
    )


def _lease_keys(rid: int) -> list:
    """Per-request block keys derived from the rid — disjoint key spaces
    (no shared prefix), so every lease is a fresh LACKMEM_KEYS_PER_REQUEST
    block allocation with zero prefix reuse."""
    return [rid * 100 + i for i in range(1, LACKMEM_KEYS_PER_REQUEST + 1)]


@case(
    "admission_engine_kv_lack_mem_fast_reject",
    profiles=["batch-window"],
    requires=["enqueue_batch"],
    source=(
        "admission wave-3 B2: engine prefill KV block-pool gate "
        "(KV v2 BlockLease admission — 602 LACK_MEM synchronous fast "
        "reject, the non-waitable engine-side complement of the "
        "master KV squeeze in admission_slo_queue_deadline)"
    ),
)
def admission_engine_kv_lack_mem_fast_reject(ctx: CaseContext):
    """Engine prefill KV block-pool gate: 602 LACK_MEM fast reject.

    Scenario: dedicated 1P+2D env with a 17-block prefill KV pool
    (EnvSpec prefill_cache_blocks=17; reserve = ceil(5% x 17) = 1 block
    — the KV v2 TOTAL_AND_AVAILABLE gate).  Every request carries 8
    per-request block_cache_keys with input_len=512 — the engine-side
    need caliber is the KEY COUNT (8 blocks), while the master-side KV
    gate compares the request's seqLen (512 tokens) against the
    engine-reported available tokens (>= 1 block = 1024 even at peak
    occupancy), so the master gate never intercepts: the ENGINE gate
    is the only admission edge in play.  prefill_fixed_ms=3000 holds
    batch #1 running while batch #2 queues; both leases are
    provisioned at enqueue (the KV v2 admission-lease semantics), so
    after two fires held_blocks = 16 of 17 — a snapshot poll proves
    the pool-full state BEFORE the probe fires.

    Behaviour: the third 8-block request fails acquireBlockLease at
    EnqueueBatch Phase-1.5 — the ack carries the per-request error
    code 602 (MALLOC_FAILED, never the master's 8431) with "LACK_MEM:
    insufficient KV cache blocks (need=8, avail=1, spb=1024)";
    DefaultBatchDispatcher wraps it as EngineRejectedException
    ("EnqueueBatch rejected request N: LACK_MEM: ...").  New-B
    channel: the master surfaces the reject SYNCHRONOUSLY ON THE
    Schedule RPC (code 8510, "Delivery failed: EnqueueBatch rejected
    request N error_code=602: LACK_MEM: ..." — the same synchronous
    RPC surface admission_engine_waiting_batch_cap_reject accepts);
    the legacy stream-terminal path is kept as the accepted alternate.
    Synchronous, typed, no park, no queue (the non-waitable complement
    of admission_slo_queue_deadline, where the SAME master KV surface
    parks because that squeeze is a WAIT condition).  The rejected
    request leaves no residue (lease acquisition rolled back,
    requestStates -> "rejected").

    Expected (contract): the probe terminates FAST (< 3s from fire,
    no park residence) with the LACK_MEM family in its rejection
    ("lack_mem" + "insufficient kv cache" + the "enqueuebatch
    rejected" wrapper), read from EITHER surface — the Schedule RPC
    reject (8510-wrapped, primary under new-B) or the stream terminal
    (legacy); the two occupants complete normally and their leases
    hand back to the LRU on completion (pool recovery — pure-LRU
    blocks count as available again); a fresh 8-block request on the
    recovered pool succeeds; the master inflight and engine ledgers
    drain clean and recovery holds.

    Prediction: expected to pass — the 602 ack surface is
    BlockPoolCapacityTest's master-visible contract (11 hash-channel
    blocks vs a 10-block pool) and the EngineRejectedException
    terminal path is the enqueue-ack fault family's.  The 17-block
    sizing makes the arithmetic exact: 8+8 admitted with the reserve
    to spare, the third 8-block need denied outright by the available
    check (8 > 1) — no borderline rounding.  Risk: the two occupancy
    fires coalescing into ONE batch — both leases still provision at
    enqueue, so the pool still saturates (only the "third request"
    ordinal shifts); the 0.4s inter-fire gap keeps them separate
    anyway.
    """
    env = ctx.env_manager.ensure(_lack_mem_spec(ctx))
    ops = ctx.engine_ops(env)
    base = rid_base(ctx, "admission")
    names = _prefill_names(ops)
    if not names:
        return False, "no prefill engines found"
    fired: list = []
    try:
        for n in names:
            ops.set_perf(n, prefill_fixed_ms=3000.0)

        # Occupants: batch #1 running (lease taken at enqueue), batch #2
        # queued (lease ALSO taken at enqueue — KV v2 admission leases).
        fire_errors = []
        for _ in range(2):
            rid = ops.next_request_id(base)
            err = _fire_tracked(
                ops,
                rid,
                fired,
                input_len=512,
                output_len=2,
                block_keys=_lease_keys(rid),
            )
            if err is not None:
                fire_errors.append((rid, err))
            time.sleep(0.4)  # >> maxCollectionWaitMs: one batch per fire
        if fire_errors:
            return False, f"occupant fire failed: {fire_errors[:1]}"

        # Pool-full proof BEFORE the probe: held = 16 of 17 blocks.
        held_seen = 0

        def pool_full() -> bool:
            snap = ops.snapshot_by_name()
            nonlocal held_seen
            held_seen = max(
                held_seen,
                max(int(snap.get(n, {}).get("held_blocks", 0)) for n in names),
            )
            return all(
                int(snap.get(n, {}).get("held_blocks", 0))
                >= 2 * LACKMEM_KEYS_PER_REQUEST
                for n in names
            )

        pool_observed = wait_for(pool_full, 8.0, 0.1)
        if not pool_observed:
            return False, (
                f"occupancy leases never saturated the pool "
                f"(held_max={held_seen}/{LACKMEM_POOL_BLOCKS})"
            )

        # Probe: 602 LACK_MEM synchronous fast reject (no park, no
        # queue).  New-B channel: the master surfaces the EnqueueBatch
        # reject SYNCHRONOUSLY ON THE Schedule RPC (code 8510 wrapping
        # the 602 ack error) — accept EITHER surface, the RPC-level
        # typed reject (primary) or a successful fire whose stream then
        # terminates with the LACK_MEM family (legacy), the same
        # dual-surface probe admission_engine_waiting_batch_cap_reject
        # uses.
        rid3 = ops.next_request_id(base)
        r3_t0 = time.monotonic()
        r3_err = ""
        try:
            resp3 = ops.schedule(
                rid3, input_len=512, output_len=2, block_keys=_lease_keys(rid3)
            )
            if resp3.code != 200 or not resp3.success:
                r3_err = f"schedule failed ({resp3.code}): {resp3.error_message}"
            else:
                handle3 = ops.start_stream(resp3, rid3)
                ended3 = handle3.wait_end(10.0)
                r3_err = str(handle3.snap.error or "") if ended3 else "no terminal"
        except Exception as exc:
            r3_err = repr(exc)
        reject_latency = time.monotonic() - r3_t0
        err_low = r3_err.lower()
        rejected = (
            "lack_mem" in err_low
            and "insufficient kv cache" in err_low
            and "enqueuebatch rejected" in err_low
            and reject_latency < 3.0
        )

        # Occupants complete; their leases hand back to the LRU on
        # completion — the pool recovers (pure-LRU counts as available).
        outcomes = _await_tracked(fired, wait_s=45.0)
        occupant_ok = len(outcomes) >= 2 and all(
            ok and err is None for _, _, _, ok, err in outcomes[:2]
        )

        avail_seen = 0

        def pool_recovered() -> bool:
            snap = ops.snapshot_by_name()
            nonlocal avail_seen
            avail_seen = max(
                avail_seen,
                max(int(snap.get(n, {}).get("available_blocks", 0)) for n in names),
            )
            return all(
                int(snap.get(n, {}).get("available_blocks", 0))
                >= LACKMEM_KEYS_PER_REQUEST
                for n in names
            )

        recovered_seen = wait_for(pool_recovered, 10.0, 0.2)

        # Post-release probe: a fresh 8-block lease on the recovered pool.
        fired4: list = []
        rid4 = ops.next_request_id(base)
        fire_err4 = _fire_tracked(
            ops,
            rid4,
            fired4,
            input_len=512,
            output_len=2,
            block_keys=_lease_keys(rid4),
        )
        outcomes4 = _await_tracked(fired4, wait_s=STREAM_TIMEOUT_S)
        lease4_ok = (
            fire_err4 is None
            and len(outcomes4) == 1
            and outcomes4[0][3]
            and outcomes4[0][4] is None
        )

        inflight_ok, inflight_detail = AssertUtils.inflight_clean(
            _master_http(ops), 30.0
        )
        engine_clean, engine_detail = engine_inflight_clean(
            ops, names + _decode_names(ops), 15.0
        )
        recovery_ok, recovery_msg = ops.verify_recovery()

        passed = (
            pool_observed
            and rejected
            and occupant_ok
            and recovered_seen
            and lease4_ok
            and inflight_ok
            and engine_clean
            and recovery_ok
        )
        return passed, (
            f"pool_observed={pool_observed} "
            f"(held={held_seen}/{LACKMEM_POOL_BLOCKS}), "
            f"probe_rejected={rejected} "
            f"(latency={reject_latency:.2f}s, err={r3_err[:80]}), "
            f"occupants_completed={occupant_ok}, "
            f"pool_recovers={recovered_seen} "
            f"(avail={avail_seen}/{LACKMEM_POOL_BLOCKS}), "
            f"fresh_lease_succeeds={lease4_ok} (fire_err4={fire_err4}), "
            f"inflight_clean={inflight_ok}({inflight_detail}), "
            f"engine_clean={engine_clean}({engine_detail}), "
            f"recovery={recovery_msg}"
        )
    except Exception as exc:
        return False, f"exception: {exc!r}"
    finally:
        try:
            for n in names:
                ops.set_perf(n, prefill_fixed_ms=100.0)
        except Exception:
            pass
