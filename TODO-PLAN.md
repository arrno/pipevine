# Implementation Plan: Cancellation & Visibility Enhancements

## Guiding Principles

-   Extend existing pipeline/stage/worker plumbing instead of replacing it.
-   Keep current public APIs (`Pipeline.run`, `Stage.run`, decorators) compatible by adding optional parameters/state rather than breaking signatures.
-   Centralize lifecycle control inside `Pipeline` so stages/workers stay focused on transformation logic.

## A. Cooperative Cancellation

1. **Pipeline runtime context**

    - Add lazily-created `PipelineRuntime` on `Pipeline` instances holding:
        - `cancel_event: asyncio.Event`
        - `reason: str | None`
        - `tasks: set[asyncio.Task]`
        - `processes: set[multiprocessing.Process]`
    - Provide helper methods `_register_task(...)`, `_register_process(...)` used by generator and stages.

2. **`Pipeline.cancel()`**

    - Idempotently set the event and stash the reason.
    - Push sentinel fan-out starting at the head of the stream (`__generate` queue) equal to downstream consumer count (tracked in runtime, see Stage plan).
    - Call new per-stage `request_cancel(reason)` hook to let each stage inject shutdown sentinels into their internal queues.
    - Mark pipeline state so `run()` refuses to start once canceled.

3. **Generator shutdown**

    - `__generate` keeps a reference to the created task; on cancel, call `cancel()` and await inside runtime cleanup.
    - If the upstream iterator exposes `close()`/`aclose()` use it. Fall back to draining loop that ignores further results once cancel is set.

4. **Worker awareness**
    - Thread/async workers check `cancel_event.is_set()` after inbound `get()` and before outbound `put()`; on cancellation they drop the current item and push sentinel.
    - For `mp_worker`, share cancel intent via extra sentinel(s) in the inbound MP queue; make `_mp_task` exit early when sentinel arrives without waiting for buffer.

## B. Track & Await Everything

1. **Task registration**

    - Update `Pipeline.__generate`, `Stage.run`, `async_util` pump/mux helpers, and `worker()` to accept optional `register_task` callable.
    - Whenever `asyncio.create_task` is invoked, pass the task to the registrar; Stage passes registrar down to helpers/workers.

2. **Processes**

    - Extend `Stage.run` to capture the `Process` handles returned by `mp_worker` and register them with `Pipeline`.

3. **`Pipeline.run` guard**

    - Wrap body in `try/finally` that:
        - Cancels outstanding tasks (if not already done) and awaits them with `asyncio.gather(return_exceptions=True)`.
        - Joins registered processes with timeout, force-terminating stragglers.

4. **`Pipeline.iter` thread**
    - Store background thread and event; ensure `cancel()` joins this thread so sync iteration exits cleanly.

## C. Bounded Queues & Teardown

1. **Queue sizing audit**

    - Ensure every freshly constructed `asyncio.Queue` / `Queue` / `MPQueue` receives an explicit `maxsize` (reuse existing buffer parameters).
    - Expose per-stage effective buffer size via metrics (see section D) for observability.

2. **Non-blocking cancel put**
    - During cancellation, use `put_nowait()` with fallback short timeout loops when injecting sentinels to avoid deadlocks if consumers disappeared.
    - Update async helper pumps to respect cancel event: if set, stop reading upstream and finish by delivering required sentinels.

## D. Surface Errors & Visibility

1. **Error policy API**

    - Introduce `ErrorPolicy` enum (`DROP`, `EMIT`, `RAISE`, `SIDE`) with optional `error_handler` callback on `Stage`.
    - Modify worker loops to branch on policy:
        - `DROP`: current behaviour (count/log only).
        - `EMIT`: wrap `Err` and push downstream (respecting queue bounds).
        - `RAISE`: signal pipeline cancel with reason, propagate Err to pipeline result.
        - `SIDE`: invoke callback, then act like `DROP` unless callback raises.

2. **Metrics & logging**

    - Add lightweight `StageMetrics` (processed, succeeded, failed, emitted_errors) stored on Stage runtime and exposed via `Pipeline.snapshot()`.
    - Integrate `logging` module with rate-limited warnings (e.g., simple timestamp gate per stage) so repetitive errors don't spam.

3. **Result reporting**
    - Ensure `Pipeline.run()` returns `Err` when cancellation was triggered by `RAISE`/explicit cancel, including reason string.
    - Keep existing ok-result on graceful completion.

## E. Testing & Migration Strategy

1. **Incremental rollout**

    - Start by introducing runtime/registration plumbing with no behaviour change (tests should still pass).
    - Layer cancellation handling,
    - Lastly, layer in error policy behavior

2. **Tests**

    - Add async tests covering:
        - Explicit `pipeline.cancel()` mid-run stops workers and drains queues.
        - `RAISE` policy aborts pipeline and surfaces reason.
        - Multiprocessing stage cancellation joins processes.
        - `EMIT` policy forwards `Err` objects down the line.
        - Resource cleanup verified via lack of pending tasks (use `asyncio.all_tasks`).

3. **Backwards compatibility validation**
    - Run existing integration suite to confirm no regressions.
    - Document new APIs in README once implementation stabilizes (tracked separately).
