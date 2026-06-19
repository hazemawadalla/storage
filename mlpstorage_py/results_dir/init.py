"""
``mlpstorage init`` CLI dispatcher — stub (Task 1).

Task 2 replaces this stub with the full LAY-01 / D-09 / D-11 logic. The stub
exists so the Task 1 dispatch wiring (``main._main_impl`` early-return on
``args.mode == "init"``) can be exercised end-to-end without a half-shipped
file in the import graph.

Refs: 01-canonical-layout-and-init / 01-02-PLAN.md Task 1 + Task 2.
"""

from __future__ import annotations

from mlpstorage_py.config import EXIT_CODE


def run_init(args) -> EXIT_CODE:  # pragma: no cover — replaced in Task 2
    """Stub — returns SUCCESS without doing any work. Task 2 implements
    the real D-09 / D-11 / LAY-01 logic.
    """
    return EXIT_CODE.SUCCESS
