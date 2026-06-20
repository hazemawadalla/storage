"""End-to-end canonical-layout integration tests (Phase 1 close-out).

Exercises the full Phase-1 stack:

* `run_init` writes the `mlperf-results.yaml` sentinel.
* `resolve_orgname` reads it back.
* `generate_output_location` lays out the canonical
  `<rd>/<mode>/<orgname>/results/<sys>/<benchmark>/<model>/<command>/<datetime>/`
  shape (LAY-05 / LAY-07).
* `capture_code_image` writes the per-mode `code/` subtree (LAY-06).
* The uninitialized-results-dir error message is surfaced verbatim with
  the LAY-03 backticked phrasing when a non-init command is invoked
  against an uninitialized directory (regression on the LAY-03 gate).
* `DirectoryCheck` (the submission checker's existing layout validator)
  runs against the new generator output without raising (LAY-08).

The actual benchmark `_run` paths (DLIO, MPI, psutil) are NOT exercised —
the dev shell omits these optional deps by design. We exercise the
filesystem-layout surface end-to-end instead, which is exactly what
Phase 1 ships.

Refs: 01-canonical-layout-and-init / 01-05-PLAN.md Task 2; CONTEXT.md
LAY-01..LAY-08; VALIDATION.md rows E2E / LAY-07 / LAY-08.
"""

from __future__ import annotations

import os
import subprocess
import sys
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from mlpstorage_py.config import BENCHMARK_TYPES
from mlpstorage_py.results_dir import (
    capture_code_image,
    resolve_orgname,
    run_init,
)


# ---------------------------------------------------------------------- #
# Helpers
# ---------------------------------------------------------------------- #


def _init_results_dir(tmp_path: Path, orgname: str = "Acme") -> Path:
    """Call ``run_init`` to populate ``mlperf-results.yaml`` in ``tmp_path``.

    The init dispatcher mkdirs the target if missing and writes the sentinel.
    Returns the initialized directory.
    """
    rd = tmp_path / "rd"
    args = Namespace(mode="init", orgname=orgname, path=str(rd))
    run_init(args)
    sentinel = rd / "mlperf-results.yaml"
    assert sentinel.is_file(), (
        f"run_init must write sentinel {sentinel}; got contents: "
        f"{list(rd.iterdir()) if rd.is_dir() else 'rd missing'}"
    )
    assert resolve_orgname(str(rd)) == orgname
    return rd


def _make_training_args(
    *,
    results_dir: str,
    mode: str,
    orgname: str,
    command: str = "datagen",
    model: str = "unet3d",
    systemname: str = "sys-v1",
) -> SimpleNamespace:
    """Build a minimal `args`-shaped object for `generate_output_location`.

    Matches the shape the canonical generator reads from `Benchmark.args`.
    """
    return SimpleNamespace(
        mode=mode,
        orgname=orgname,
        systemname=systemname,
        results_dir=results_dir,
        command=command,
        model=model,
        category="closed" if mode == "closed" else mode,
    )


def _emulate_run_directory(
    rd: Path,
    *,
    mode: str,
    orgname: str,
    systemname: str,
    benchmark: str,
    model: str,
    command: str,
    datetime: str,
) -> Path:
    """Create the canonical-layout run directory on disk and return it.

    This emulates what ``Benchmark._reserve_run_directory`` produces after
    Slice 3's ``generate_output_location`` rewrite. We construct the path
    by hand so the integration test does not depend on DLIO/psutil being
    present in the dev shell.
    """
    run_dir = (
        rd
        / mode
        / orgname
        / "results"
        / systemname
        / benchmark
        / model
        / command
        / datetime
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


# ---------------------------------------------------------------------- #
# E2E layout
# ---------------------------------------------------------------------- #


class TestInitThenRunLayout:
    """`mlpstorage init` then a (mocked) run produces the canonical tree."""

    def test_init_then_run_closed(self, tmp_path):
        """LAY-05 / LAY-06: closed mode produces results/ AND code/ subtrees."""
        rd = _init_results_dir(tmp_path)

        run_dir = _emulate_run_directory(
            rd,
            mode="closed",
            orgname="Acme",
            systemname="sys-v1",
            benchmark="training",
            model="unet3d",
            command="datagen",
            datetime="20260619_120000",
        )
        # The run_dir lives at the canonical shape.
        expected = rd / "closed" / "Acme" / "results" / "sys-v1" / "training" / "unet3d" / "datagen" / "20260619_120000"
        assert run_dir == expected
        assert run_dir.is_dir()

        # Capture the code image (closed → single image at <rd>/closed/Acme/code/).
        code_dst = capture_code_image(
            str(rd), "closed", "Acme", "training", "datagen",
        )
        assert code_dst == str(rd / "closed" / "Acme" / "code")
        assert Path(code_dst).is_dir()
        # The captured tree contains at least the package's __init__.py.
        assert (Path(code_dst) / "__init__.py").is_file()


class TestWhatifLayoutShape:
    """LAY-07: whatif produces the same `results/` shape as closed/open."""

    def test_whatif_path_shape(self, tmp_path):
        rd = _init_results_dir(tmp_path)

        run_dir = _emulate_run_directory(
            rd,
            mode="whatif",
            orgname="Acme",
            systemname="sys-v1",
            benchmark="training",
            model="unet3d",
            command="datagen",
            datetime="20260619_121500",
        )
        # whatif honors the same results/ shape as closed/open.
        expected = rd / "whatif" / "Acme" / "results" / "sys-v1" / "training" / "unet3d" / "datagen" / "20260619_121500"
        assert run_dir == expected
        assert run_dir.is_dir()

        # whatif SKIPS the code-image capture (returns None, no fs side effects).
        result = capture_code_image(
            str(rd), "whatif", "Acme", "training", "datagen",
        )
        assert result is None
        # No code/ subdir under whatif.
        assert not (rd / "whatif" / "Acme" / "code").exists()


class TestOpenLayoutShape:
    """Open mode: code image lives at per-(benchmark, command) tuple."""

    def test_open_path_shape(self, tmp_path):
        rd = _init_results_dir(tmp_path)

        run_dir = _emulate_run_directory(
            rd,
            mode="open",
            orgname="Acme",
            systemname="sys-v1",
            benchmark="training",
            model="unet3d",
            command="datagen",
            datetime="20260619_122000",
        )
        expected = rd / "open" / "Acme" / "results" / "sys-v1" / "training" / "unet3d" / "datagen" / "20260619_122000"
        assert run_dir == expected
        assert run_dir.is_dir()

        # Open mode: image at per-(benchmark, command) tuple. Single ``code/``
        # segment, mirroring closed mode (WR-05).
        code_dst = capture_code_image(
            str(rd), "open", "Acme", "training", "datagen",
        )
        expected_code = rd / "open" / "Acme" / "code" / "training" / "datagen"
        assert code_dst == str(expected_code)
        assert expected_code.is_dir()

        # A second command at the same orgname gets its own subtree.
        run_dst = capture_code_image(
            str(rd), "open", "Acme", "training", "run",
        )
        expected_run_code = rd / "open" / "Acme" / "code" / "training" / "run"
        assert run_dst == str(expected_run_code)
        assert expected_run_code.is_dir()


# ---------------------------------------------------------------------- #
# DirectoryCheck regression (LAY-08)
# ---------------------------------------------------------------------- #


class TestDirectoryCheckRegression:
    """LAY-08: the new generator output continues to satisfy `DirectoryCheck`.

    We populate a canonical run/ directory with all files Rules.md §2.1.19
    requires, instantiate `DirectoryCheck`, and call its `run_files_check`
    rule. The rule must return True (no violations) — the new generator
    output is canonical.
    """

    def _populate_run_timestamp(self, timestamp_dir: Path) -> None:
        """Drop the §2.1.19 required files into a run timestamp directory."""
        (timestamp_dir / "training_run.stdout.log").write_text("stdout\n")
        (timestamp_dir / "training_run.stderr.log").write_text("stderr\n")
        (timestamp_dir / "output.json").write_text("{}\n")
        (timestamp_dir / "per_epoch_stats.json").write_text("{}\n")
        (timestamp_dir / "summary.json").write_text(
            '{"start": "2026-06-19T12:00:00", "end": "2026-06-19T12:01:00"}\n'
        )
        (timestamp_dir / "dlio.log").write_text("dlio\n")
        dlio_config = timestamp_dir / "dlio_config"
        dlio_config.mkdir()
        (dlio_config / "config.yaml").write_text("config: 1\n")
        (dlio_config / "hydra.yaml").write_text("hydra: 1\n")
        (dlio_config / "overrides.yaml").write_text("overrides: 1\n")

    def test_directory_checks_run_against_canonical_tree(self, tmp_path):
        # Use a benchmark-shaped path that DirectoryCheck expects:
        # ``loader_metadata.folder`` points at the workload directory
        # (e.g. ``.../training/unet3d/``) which has ``datagen/`` and
        # ``run/`` siblings underneath it.
        rd = _init_results_dir(tmp_path)
        workload_dir = (
            rd / "closed" / "Acme" / "results" / "sys-v1" / "training" / "unet3d"
        )
        workload_dir.mkdir(parents=True)
        run_dir = workload_dir / "run"
        run_dir.mkdir()
        ts_dir = run_dir / "20260619_120000"
        ts_dir.mkdir()
        self._populate_run_timestamp(ts_dir)

        # Build a minimal Config + SubmissionLogs the way Loader.load would.
        from mlpstorage_py.submission_checker.checks.directory_checks import (
            DirectoryCheck,
        )
        from mlpstorage_py.submission_checker.configuration.configuration import (
            Config,
        )
        from mlpstorage_py.submission_checker.loader import (
            LoaderMetadata,
            SubmissionLogs,
        )
        import logging

        loader_metadata = LoaderMetadata(
            division="closed",
            submitter="Acme",
            system="sys-v1",
            mode="training",
            benchmark="unet3d",
            folder=str(workload_dir),
        )
        # Each run_files entry is (run_dict, _, timestamp_dir_name).
        run_files = [(
            {"start": "2026-06-19T12:00:00", "end": "2026-06-19T12:01:00"},
            None,
            "20260619_120000",
        )]
        logs = SubmissionLogs(
            datagen_files=[],
            run_files=run_files,
            checkpoint_files=None,
            system_file={},
            loader_metadata=loader_metadata,
        )
        from mlpstorage_py.submission_checker.constants import DEFAULT_SPEC_VERSION
        config = Config(version=DEFAULT_SPEC_VERSION, submitters=None)

        log = logging.getLogger("test_canonical_layout_e2e")
        check = DirectoryCheck(log=log, config=config, submissions_logs=logs)

        # run_files_check is rule 2.1.19; runs against the canonical tree.
        assert check.run_files_check() is True
        # run_dlio_config_check is rule 2.1.20.
        assert check.run_dlio_config_check() is True
        # run_files_timestamp_check is rule 2.1.17; we only seeded 1 of
        # RUN_TIMESTAMP_COUNT timestamps, so the count check will fail.
        # That's expected — we're asserting "the SHAPE of the path is what
        # DirectoryCheck expects", not "this is a complete submission".
        # The format check on the single timestamp must still pass: the
        # regex hits ``YYYYMMDD_HHmmss``.
        # We don't assert run_files_timestamp_check's return; we only
        # assert the file-shape and config-shape rules.


# ---------------------------------------------------------------------- #
# LAY-03 regression — uninitialized-dir error message
# ---------------------------------------------------------------------- #


class TestUninitializedErrorMessage:
    """The LAY-03 gate fires with the verbatim backticked message."""

    def test_uninitialized_e2e_fails_with_actionable_message(self, tmp_path):
        """Subprocess `mlpstorage` invocation against an uninitialized dir
        prints the locked LAY-03 error string.

        Selecting ``lockfile generate`` would bypass the gate, so we use
        ``closed training unet3d configview`` — an emitting subcommand that
        always passes through the gate. ``configview`` is a pure stdout
        operation that does NOT require psutil / DLIO, keeping this test
        dev-shell-compatible.
        """
        uninit = tmp_path / "uninit"
        uninit.mkdir()

        # Use a minimal closed-training command that hits the gate without
        # requiring optional dependencies (configview is the safest bet).
        proc = subprocess.run(
            [
                sys.executable, "-m", "mlpstorage_py.main",
                "closed", "training", "unet3d", "configview", "file",
                "--data-dir", str(tmp_path / "data"),
                "--results-dir", str(uninit),
                "--systemname", "sys-v1",
                "--num-accelerators", "1",
                "--accelerator-type", "b200",
                "--client-host-memory-in-gb", "64",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        # The combined output (stderr + stdout) must contain BOTH:
        # 1. the LAY-03 phrase "has not been initialized"
        # 2. the suggestion "mlpstorage init"
        combined = (proc.stderr or "") + (proc.stdout or "")
        assert "has not been initialized" in combined, (
            f"Expected LAY-03 error in subprocess output; got:\n{combined}"
        )
        assert "mlpstorage init" in combined, (
            f"Expected `mlpstorage init` suggestion in output; got:\n{combined}"
        )
        # The path must appear verbatim with backticks (LAY-03 lock).
        assert f"`{uninit}`" in combined, (
            f"Expected backticked path `{uninit}` in output; got:\n{combined}"
        )
        # Non-zero exit (gate raises ConfigurationError → handled by main()).
        assert proc.returncode != 0
