# Workflow Control Plane

Piko now has two separate execution contracts:

* `scheduled_job` and `job_run` remain the APScheduler-backed legacy job trigger and execution record.
* `workflow_run`, `workflow_task`, `workflow_task_dependency`, `workflow_task_event`, and `workflow_task_manifest` form the durable DAG control plane.

The scheduler only supplies a trigger entry. It does not interpret a DAG and it does not rewrite a legacy `job_run` into a workflow task. Applications create a `WorkflowDefinition`, call `PikoApp.create_workflow_run()` or a repository directly, and run registered stages through `WorkflowWorker`.

## Contract

`WorkflowControlBackend` is declared in `piko/workflow/repository.py`. `InMemoryWorkflowRepository` is the reference fake; `MySQLWorkflowRepository` is the production implementation. Both expose the same operations for run creation, claim, heartbeat, recovery, activation, fenced retry/failure/cancel, audited manual control, finalization, run/task lookup, manifest lookup, and event lookup.

`rerun=True` 只有在同一 `(workflow_id, idempotency_key)` 已存在原始 run 时才创建带 `:rerun:<run_id>` 后缀的新 run；首次创建不存在原始 run 时仍建立基础幂等映射。内存和 MySQL 后端遵守相同语义。

`TaskStatus.SUCCEEDED` is technical success only. `TaskResult.result_status` is the business result and supports `complete`, `partial`, `empty`, `unavailable`, and `failed` without collapsing valid terminal values to `unknown`. Dependencies default to technical `succeeded` plus business `complete`; other business statuses must be explicitly allowed on each edge. Any failed, canceled, blocked, or disallowed upstream blocks its dependent task instead of leaving it pending indefinitely.

## MySQL fencing

`claim_ready_tasks()` uses an InnoDB transaction and `SELECT ... FOR UPDATE SKIP LOCKED`. It sets `running`, owner, a random fencing token, lease, heartbeat, and `attempt` in the same transaction. Recovery clears ownership and leaves `attempt` unchanged. Heartbeat, retry, failure, cancel, and finalization all use conditional updates containing task id, owner, token, running state, and an unexpired lease; zero affected rows means the caller no longer owns the task.

The manifest is unique by task and idempotency key. A stale worker is rejected before its business hook runs, so it cannot write a manifest after a lease handoff. A handler that needs to write authoritative business output returns a `TaskResult` with an optional `business_hook`; `WorkflowWorker` passes that hook into the default finalization path. The hook receives the same transaction object as the manifest, task event, task status, and run status updates (`AsyncSession` for MySQL, `MemoryTransaction` for the fake). `WorkflowFinalizer.finalize()` remains available for explicit application-side finalization. If any part fails, the transaction rolls back.

Operator actions use `PikoApp.control_workflow_task()` or `WorkflowControlBackend.control_task()` with `retry`, `cancel`, or `unblock` and a required `reason_digest`. Each successful action emits a `manual_<action>` audit event and clears ownership before making the task available again.

Applications register stages with `@app.workflow("stage")` or `app.register_workflow_handler()`. `PikoApp.startup()` creates the workflow worker after database recovery and activation; its shutdown grace and cancellation cleanup windows are capped below the application's total shutdown budget. On forced shutdown, in-flight tasks are fenced back to `retry_waiting` (or terminal `failed` at max attempts) before handler cleanup is allowed to finish. The workflow worker is started even when no handler is registered, but it never claims an unregistered stage.

`PersistenceWriter` is intentionally outside this boundary. Its queue and disk fallback are useful for legacy asynchronous sinks, but they are not a substitute for a workflow finalization transaction. A workflow handler must use a repository transaction hook for authoritative output.

## Contract coverage mapping

| Behavior | Piko contract test | Piko implementation |
| --- | --- | --- |
| Idempotent run creation and explicit rerun | `test_registration.py` | `WorkflowDefinition.rerun`, unique run idempotency key |
| One claim winner, registered stages, SKIP LOCKED | `test_claim_and_concurrency.py`, `test_mysql_concurrency.py` | fake lock plus MySQL row locks |
| Lease heartbeat, recovery, unchanged attempt | `test_lease_and_heartbeat.py`, `test_mysql_recovery.py` | token/lease predicates and recovery transitions |
| Stale token cannot finalize or write retry/failure/cancel | `test_fencing.py`, MySQL stale-worker test | conditional UPDATE plus manifest-before-hook fence |
| Retry backoff, timeout, bounded cancellation | `test_retry_and_timeout.py`, chaos tests | `WorkflowWorker` retry path and orphan tracking |
| DAG fan-out/fan-in and no partial activation | `test_dag_activation.py` | `DependencySpec` and pure dependency decision |
| Atomic business output, manifest, task, event, run | `test_finalization_transaction.py`, MySQL failure tests | `WorkflowFinalizer` and repository transaction |
| Same contract for memory and MySQL, including recovery, fencing, cancellation, timeout, and business statuses | `test_dual_backend_contract.py`, `test_dual_backend_extended_contract.py` | parametrized backend contract fixtures |
| Worker default business hook and PikoApp lifecycle | `test_worker_finalization.py`, `test_workflow_app_lifecycle.py` | `TaskResult.business_hook`, `PikoApp.start_workflow_worker()` |
| Stop-before-claim, forced shutdown, and crash recovery | `test_shutdown_recovery.py`, `test_dual_backend_extended_contract.py`, chaos tests | worker stop check, explicit retry recovery, bounded grace |
| Technical/business result separation and safe observability | `test_idempotency.py`, `test_observability.py` | manifest result status, separate Prometheus labels, redaction |

The SQL repository and schema use generic workflow identifiers and remain separate from the legacy scheduler schema. Business-specific handlers and output tables are intentionally outside Piko core.

## Verification

The mutation harness is `scripts/run-workflow-contract-mutations.py`. It injects the ten requested defects, including a real MySQL `SKIP LOCKED` defect, and requires the focused contract test to fail. Integration tests use only `PIKO_TEST_MYSQL_DSN`; no connection string is stored in the repository.

Production types are checked with `uv run pyright`; tests and validation scripts are checked separately with `uv run pyright -p pyrightconfig.tests.json`.
