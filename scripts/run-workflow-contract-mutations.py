"""Run targeted source mutations and require the workflow contract tests to fail.

The MySQL mutations are intentionally run against PIKO_TEST_MYSQL_DSN when it is
provided; a fake backend cannot prove row-lock behavior.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_SOURCE = "piko/workflow/repository.py"
TYPES_SOURCE = "piko/workflow/types.py"
WORKER_SOURCE = "piko/workflow/worker.py"
TERMINAL_STATUS_TEST = (
    "tests/workflow/integration/test_dual_backend_extended_contract.py::"
    "test_terminal_business_status_is_preserved_on_both_backends"
)
DISABLED_CONDITION = "if False:"


@dataclass(frozen=True)
class Mutation:
    name: str
    source: str
    needle: str
    replacement: str
    test_node: str
    integration: bool = False


MUTATIONS = (
    Mutation(
        "remove-lock-token-condition",
        REPOSITORY_SOURCE,
        "and task.lock_token == token",
        "and True",
        "tests/workflow/integration/test_dual_backend_contract.py::test_fencing_blocks_stale_retry_and_finalize_on_both_backends",
    ),
    Mutation(
        "remove-skip-locked",
        "piko/workflow/mysql_repository.py",
        ".limit(limit)\n                        .with_for_update(skip_locked=True)",
        ".limit(limit)\n                        .with_for_update()",
        "tests/workflow/integration/test_mysql_concurrency.py::test_mysql_claim_skips_row_locked_by_another_transaction",
        True,
    ),
    Mutation(
        "recovery-increments-attempt",
        REPOSITORY_SOURCE,
        "status=target,\n                    available_at=None if terminal else now,",
        "status=target,\n                    attempt=task.attempt + 1,\n                    available_at=None if terminal else now,",
        "tests/workflow/integration/test_dual_backend_contract.py::test_recovery_retry_and_max_attempts_are_backend_identical",
    ),
    Mutation(
        "remove-max-attempts-limit",
        REPOSITORY_SOURCE,
        "if task.attempt >= task.max_attempts:",
        DISABLED_CONDITION,
        "tests/workflow/integration/test_dual_backend_contract.py::test_recovery_retry_and_max_attempts_are_backend_identical",
    ),
    Mutation(
        "allow-stale-finalize",
        REPOSITORY_SOURCE,
        'if not self._owns(current, task.owner or "", task.lock_token or "", now):',
        DISABLED_CONDITION,
        "tests/workflow/integration/test_dual_backend_contract.py::test_fencing_blocks_stale_retry_and_finalize_on_both_backends",
    ),
    Mutation(
        "activate-after-partial-business-result",
        "piko/workflow/state_machine.py",
        "if business not in edge.allowed_business_statuses:",
        DISABLED_CONDITION,
        "tests/workflow/integration/test_dual_backend_contract.py::test_dag_activation_requires_all_dependencies_and_is_idempotent",
    ),
    Mutation(
        "technical-success-is-business-complete",
        TYPES_SOURCE,
        "BusinessResultStatus.PARTIAL.value,\n        BusinessResultStatus.EMPTY.value,",
        "BusinessResultStatus.EMPTY.value,",
        TERMINAL_STATUS_TEST,
    ),
    Mutation(
        "drop-failed-business-status",
        TYPES_SOURCE,
        "BusinessResultStatus.FAILED.value,\n        BusinessResultStatus.UNAVAILABLE.value,",
        "BusinessResultStatus.UNAVAILABLE.value,",
        TERMINAL_STATUS_TEST,
        True,
    ),
    Mutation(
        "drop-empty-business-status",
        TYPES_SOURCE,
        "BusinessResultStatus.EMPTY.value,\n    )",
        "BusinessResultStatus.PARTIAL.value,\n    )",
        TERMINAL_STATUS_TEST,
        True,
    ),
    Mutation(
        "remove-force-recover-retry",
        WORKER_SOURCE,
        'await self._with_timeout(\n                self._retry(\n                    item.task,\n                    "shutdown_timeout",\n                    f"stage={item.task.stage} task_id={item.task.task_id} shutdown timeout",\n                ),\n                remaining,\n            )',
        "await asyncio.sleep(0)",
        "tests/workflow/integration/test_dual_backend_extended_contract.py::test_forced_shutdown_recovers_running_task_on_both_backends",
        True,
    ),
    Mutation(
        "remove-shutdown-budget-clamp",
        "piko/app.py",
        "shutdown_grace_seconds=min(worker_config.shutdown_grace_seconds, grace_budget),",
        "shutdown_grace_seconds=worker_config.shutdown_grace_seconds,",
        "tests/test_workflow_app_lifecycle.py::test_app_bounds_worker_shutdown_inside_total_budget",
    ),
    Mutation(
        "force-recovery-ignores-max-attempts",
        REPOSITORY_SOURCE,
        "if task.attempt >= task.max_attempts:",
        DISABLED_CONDITION,
        "tests/workflow/integration/test_dual_backend_extended_contract.py::test_forced_shutdown_honors_max_attempts_on_both_backends",
        True,
    ),
    Mutation(
        "remove-finalization-rollback",
        REPOSITORY_SOURCE,
        "(\n                    self.tasks,\n                    self.manifests,\n                    self.events,\n                    self.business_outputs,\n                    self.runs,\n                    self._next_event_id,\n                ) = snapshot",
        "pass",
        "tests/workflow/contracts/test_finalization_transaction.py::test_failure_after_business_write_rolls_back_all_authoritative_state",
    ),
    Mutation(
        "claim-after-stop",
        WORKER_SOURCE,
        "if self._stopping.is_set():\n            return\n        available = self.config.concurrency - len(self._inflight)",
        "if False:\n            return\n        available = self.config.concurrency - len(self._inflight)",
        "tests/workflow/contracts/test_shutdown_recovery.py::test_stop_during_activation_does_not_claim_new_work",
    ),
    Mutation(
        "heartbeat-loss-still-writes-state",
        WORKER_SOURCE,
        'await self._cancel_handler(handler_task, task, "ownership_lost", write_state=False)',
        'await self._cancel_handler(handler_task, task, "ownership_lost", write_state=True)',
        "tests/workflow/chaos/test_heartbeat_loss.py",
    ),
)


def apply_mutation(root: Path, mutation: Mutation) -> None:
    path = root / mutation.source
    content = path.read_text(encoding="utf-8")
    if content.count(mutation.needle) != 1:
        raise RuntimeError(f"{mutation.name}: expected one source match")
    path.write_text(content.replace(mutation.needle, mutation.replacement), encoding="utf-8")


def main() -> int:
    if any(m.integration for m in MUTATIONS) and not os.environ.get("PIKO_TEST_MYSQL_DSN"):
        print("PIKO_TEST_MYSQL_DSN is required for MySQL mutations", file=sys.stderr)
        return 2
    survived: list[str] = []
    for mutation in MUTATIONS:
        with tempfile.TemporaryDirectory(prefix="piko-workflow-mutant-") as directory:
            temp_root = Path(directory)
            shutil.copytree(ROOT / "piko", temp_root / "piko")
            shutil.copytree(ROOT / "tests", temp_root / "tests")
            apply_mutation(temp_root, mutation)
            env = os.environ.copy()
            env["PYTHONPATH"] = os.pathsep.join(
                [str(temp_root), str(ROOT), env.get("PYTHONPATH", "")]
            )
            result = subprocess.run(
                [sys.executable, "-m", "pytest", "-q", mutation.test_node, "--maxfail=1"],
                cwd=temp_root,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                survived.append(mutation.name)
                print(f"SURVIVED {mutation.name}")
            else:
                print(f"KILLED {mutation.name}")
    if survived:
        print("Surviving workflow mutations: " + ", ".join(survived), file=sys.stderr)
        return 1
    print(f"All {len(MUTATIONS)} workflow contract mutations were killed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
