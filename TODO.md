# Pipeline Must-Haves for Safety (Without Bloat)

These are the minimal features needed to prevent leaks, deadlocks, and unsafe teardown in the pipeline.

---

## 1. Cooperative Cancellation

-   Add an **idempotent** `Pipeline.cancel(reason: str | None = None)`:
    -   Sets an internal `asyncio.Event` (`cancel_event`).
    -   Stops the source generator.
    -   Broadcasts **exactly one** sentinel per downstream consumer to unblock queues.
    -   Marks the pipeline as closing (rejects new work).
-   Workers periodically check `cancel_event.is_set()` (after `get()`, before `put()`).

---

## 2. Track and Await Everything

-   Keep references to all:
    -   `asyncio.Task`s you spawn.
    -   `multiprocessing.Process` objects if using multi-proc.
-   In `run()`, use `try/finally` to:
    -   Cancel tasks and await them.
    -   Gracefully join processes, then force-terminate stragglers if needed.

---

## 3. Bounded Queues + Non-Blocking Teardown

-   All queues should be bounded (prevents unbounded memory use).
-   On cancel:
    -   **Inject sentinels** to ensure downstream workers exit.
    -   Refuse new writes.
    -   Avoid blocking during teardown (`put_nowait()` or short timeout).

---

## 4. Surface Errors Intentionally

-   Keep the `err_to_value` wrapper for safety, but don’t _only_ drop failures.
-   Add a per-stage option for error handling:
    -   `drop` → ignore and move on (default).
    -   `emit` → pass error objects downstream.
    -   `raise` → fail-fast and trigger pipeline cancel.
    -   `side` → send to a user-provided error callback.
-   Always **log and count** errors (rate-limited), even if `drop` is used.
