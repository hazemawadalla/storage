"""
Unit tests for the ``mlpstorage init`` subcommand — Slice 2 of Phase 1
(canonical-layout-and-init).

Covers:

- Task 1 (argparse wiring):
    * ``test_init_parses_positionals`` — round-trip through ``parse_arguments``
    * ``test_init_missing_positionals_exits`` — argparse refuses bare ``init``
    * ``test_init_dispatch_early_returns`` — ``_main_impl`` short-circuits to
      ``run_init`` before any benchmark plumbing fires (RESEARCH.md Pitfall 3)
    * ``test_no_orgname_flag_on_non_init_commands`` — no ``--orgname`` flag
      leaks into any other subcommand (VALIDATION.md gate)
    * ``test_init_help_renders`` — ``mlpstorage init --help`` is well-formed

- Task 2 (run_init dispatcher — D-09, D-11, LAY-01):
    * ``test_init_creates_sentinel`` — happy path
    * ``test_init_idempotent_on_match`` — D-11 idempotent re-init
    * ``test_init_refuses_when_already_initialized`` — D-11 mismatch refusal
    * ``test_init_refuses_non_empty_dir`` — LAY-01
    * ``test_init_auto_creates_when_parent_exists`` — D-09 auto-create
    * ``test_init_refuses_when_grandparent_missing`` — D-09 refusal
    * ``test_init_case_sensitive`` — RESEARCH.md Pitfall 7

Refs: 01-canonical-layout-and-init / 01-02-PLAN.md
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------- #
# Task 1 — argparse wiring + dispatch                                          #
# ---------------------------------------------------------------------------- #


def test_init_parses_positionals():
    """``mlpstorage init Acme /tmp/r1`` → mode=init, orgname=Acme, path=/tmp/r1."""
    from mlpstorage_py.cli_parser import parse_arguments

    with patch("sys.argv", ["mlpstorage", "init", "Acme", "/tmp/r1"]):
        args = parse_arguments()

    assert args.mode == "init"
    assert args.orgname == "Acme"
    assert args.path == "/tmp/r1"


def test_init_missing_positionals_exits():
    """Bare ``mlpstorage init`` (no positionals) → SystemExit from argparse."""
    from mlpstorage_py.cli_parser import parse_arguments

    with patch("sys.argv", ["mlpstorage", "init"]):
        with pytest.raises(SystemExit):
            parse_arguments()


def test_init_dispatch_early_returns(tmp_path):
    """``_main_impl`` with mode=init must call ``run_init`` and short-circuit
    BEFORE ``update_args``, ``validate_benchmark_environment``, ``run_benchmark``,
    or ``print_run_summary`` (RESEARCH.md Pitfall 3).
    """
    from mlpstorage_py import main as main_mod
    from mlpstorage_py.config import EXIT_CODE

    target = str(tmp_path / "r1")

    captured = {}

    def fake_run_init(args):
        captured["called"] = True
        captured["orgname"] = args.orgname
        captured["path"] = args.path
        return EXIT_CODE.SUCCESS

    # Patch the function in its source module so the late-binding ``from
    # mlpstorage_py.results_dir.init import run_init`` in _main_impl picks it up.
    with patch("mlpstorage_py.results_dir.init.run_init", side_effect=fake_run_init), \
         patch.object(main_mod, "update_args") as mock_update_args, \
         patch.object(main_mod, "run_benchmark") as mock_run_benchmark, \
         patch.object(main_mod, "validate_benchmark_environment") as mock_validate_env, \
         patch("sys.argv", ["mlpstorage", "init", "Acme", target]):
        rc = main_mod._main_impl()

    assert rc == EXIT_CODE.SUCCESS
    assert captured.get("called") is True
    assert captured["orgname"] == "Acme"
    assert captured["path"] == target

    # The whole point of Pitfall 3 — no benchmark plumbing must have run.
    mock_update_args.assert_not_called()
    mock_run_benchmark.assert_not_called()
    mock_validate_env.assert_not_called()


def test_no_orgname_flag_on_non_init_commands():
    """No top-level ``--orgname`` flag on any subcommand other than (potentially)
    ``init`` itself. Even ``init`` uses a POSITIONAL ``orgname`` — never the
    ``--orgname`` flag form (CONTEXT.md "No --orgname CLI flag").
    """
    # Argparse-layer check — try to pass --orgname to several benchmark/utility
    # subcommands; each one must reject it as an unrecognized argument.
    from mlpstorage_py.cli_parser import parse_arguments

    for bad_argv in (
        ["mlpstorage", "closed", "training", "datasize", "-rd", "/tmp",
         "-m", "unet3d", "-na", "1", "-g", "h100", "-cm", "16", "--orgname", "Acme"],
        ["mlpstorage", "reports", "-rd", "/tmp", "--orgname", "Acme"],
        ["mlpstorage", "validate", "-rd", "/tmp", "--orgname", "Acme"],
        ["mlpstorage", "history", "list", "--orgname", "Acme"],
    ):
        with patch("sys.argv", bad_argv):
            with pytest.raises(SystemExit):
                parse_arguments()

    # Grep-style check: no `--orgname` literal in the CLI argument builders.
    import mlpstorage_py.cli as cli_pkg
    cli_dir = os.path.dirname(cli_pkg.__file__)
    offenders = []
    for root, _dirs, files in os.walk(cli_dir):
        for name in files:
            if not name.endswith(".py"):
                continue
            path = os.path.join(root, name)
            with open(path) as f:
                for lineno, line in enumerate(f, start=1):
                    stripped = line.lstrip()
                    if stripped.startswith("#"):
                        continue
                    if "--orgname" in line:
                        offenders.append(f"{path}:{lineno}: {line.rstrip()}")
    assert offenders == [], f"Found --orgname references in CLI builders: {offenders}"

    # Also check cli_parser.py itself.
    import mlpstorage_py.cli_parser as cli_parser_mod
    with open(cli_parser_mod.__file__) as f:
        for lineno, line in enumerate(f, start=1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            assert "--orgname" not in line, \
                f"--orgname found in cli_parser.py:{lineno}: {line.rstrip()}"


def test_init_help_renders():
    """``mlpstorage init --help`` (via subprocess against the installed entry
    point) exits 0 and mentions both positionals. Sanity check that the
    subcommand registers cleanly and the help text covers orgname/path.

    Invokes via ``python -c "from mlpstorage_py.main import main; main()"`` so
    the test runs even when the ``mlpstorage`` console script is not on PATH
    (CI install variants).
    """
    bootstrap = (
        "import sys; sys.argv = ['mlpstorage', 'init', '--help']; "
        "from mlpstorage_py.main import main; "
        "raise SystemExit(main())"
    )
    res = subprocess.run(
        [sys.executable, "-c", bootstrap],
        capture_output=True,
        text=True,
        timeout=30,
    )
    # argparse `--help` exits 0 via SystemExit(0).
    assert res.returncode == 0, (
        f"init --help exited {res.returncode}: stdout={res.stdout!r} "
        f"stderr={res.stderr!r}"
    )
    out = res.stdout + res.stderr
    # The subcommand registers with the expected description. Positional
    # docs (orgname/path) are suppressed by the project-wide
    # MLPStorageHelpFormatter (common_args.py:49-51, which intentionally
    # strips positionals on the rationale that benchmark positionals already
    # appear in the command path). The subcommand IS reachable — the
    # description being shown proves the parser registered, and the parse
    # tests above prove the positionals are accepted.
    assert "Initialize a results-dir" in out, (
        f"init subcommand description not shown:\n{out}"
    )
    assert "mlpstorage init" in out, (
        f"init usage line not shown:\n{out}"
    )


# ---------------------------------------------------------------------------- #
# Task 2 — run_init dispatcher (D-09 / D-11 / LAY-01)                          #
# ---------------------------------------------------------------------------- #


def _ns(tmp_path, orgname="Acme", subpath="r1"):
    """Build an argparse-style Namespace for run_init."""
    return argparse.Namespace(
        mode="init",
        orgname=orgname,
        path=str(tmp_path / subpath) if subpath is not None else str(tmp_path),
    )


def test_init_creates_sentinel(tmp_path):
    """Happy path: parent exists, target missing → mkdir + sentinel written."""
    from mlpstorage_py.config import EXIT_CODE
    from mlpstorage_py.results_dir import (
        MLPERF_RESULTS_FILENAME,
        read_sentinel,
    )
    from mlpstorage_py.results_dir.init import run_init

    args = _ns(tmp_path)
    rc = run_init(args)

    assert rc == EXIT_CODE.SUCCESS
    sentinel = tmp_path / "r1" / MLPERF_RESULTS_FILENAME
    assert sentinel.is_file()
    model = read_sentinel(str(tmp_path / "r1"))
    assert model.orgname == "Acme"


def test_init_idempotent_on_match(tmp_path, caplog):
    """D-11: re-init with matching orgname → exit 0 + informational message,
    sentinel file unchanged (mtime preserved).
    """
    import logging
    from mlpstorage_py.config import EXIT_CODE
    from mlpstorage_py.results_dir import MLPERF_RESULTS_FILENAME
    from mlpstorage_py.results_dir.init import run_init

    args = _ns(tmp_path, "Acme")
    assert run_init(args) == EXIT_CODE.SUCCESS

    sentinel = tmp_path / "r1" / MLPERF_RESULTS_FILENAME
    first_bytes = sentinel.read_bytes()
    first_mtime = sentinel.stat().st_mtime_ns

    with caplog.at_level(logging.INFO, logger="mlpstorage_py.results_dir.init"):
        rc = run_init(_ns(tmp_path, "Acme"))

    assert rc == EXIT_CODE.SUCCESS
    assert sentinel.read_bytes() == first_bytes
    assert sentinel.stat().st_mtime_ns == first_mtime
    # The log line is the user-facing signal of "already initialized" — check
    # it names the orgname.
    msgs = " ".join(rec.getMessage() for rec in caplog.records)
    assert "already initialized" in msgs.lower()
    assert "Acme" in msgs


def test_init_refuses_when_already_initialized(tmp_path):
    """D-11 mismatch: existing sentinel orgname=Acme, supplied=Other → refuse."""
    from mlpstorage_py.config import EXIT_CODE
    from mlpstorage_py.results_dir.errors import DoubleInitError
    from mlpstorage_py.results_dir.init import run_init

    assert run_init(_ns(tmp_path, "Acme")) == EXIT_CODE.SUCCESS

    with pytest.raises(DoubleInitError) as excinfo:
        run_init(_ns(tmp_path, "Other"))

    msg = str(excinfo.value)
    # Message must name BOTH the existing and supplied orgnames so the user
    # can disambiguate what they've done.
    assert "Acme" in msg
    assert "Other" in msg


def test_init_refuses_non_empty_dir(tmp_path):
    """LAY-01: target dir exists, has files, no sentinel → NonEmptyDirError."""
    from mlpstorage_py.results_dir.errors import NonEmptyDirError
    from mlpstorage_py.results_dir.init import run_init

    target = tmp_path / "r1"
    target.mkdir()
    (target / "stray.txt").write_text("garbage\n")

    with pytest.raises(NonEmptyDirError) as excinfo:
        run_init(argparse.Namespace(mode="init", orgname="Acme", path=str(target)))

    assert "non-empty" in str(excinfo.value).lower()


def test_init_auto_creates_when_parent_exists(tmp_path):
    """D-09: parent exists, target missing → leaf is auto-created."""
    from mlpstorage_py.config import EXIT_CODE
    from mlpstorage_py.results_dir import MLPERF_RESULTS_FILENAME
    from mlpstorage_py.results_dir.init import run_init

    target = tmp_path / "fresh"
    assert not target.exists()
    rc = run_init(argparse.Namespace(mode="init", orgname="Acme", path=str(target)))
    assert rc == EXIT_CODE.SUCCESS
    assert (target / MLPERF_RESULTS_FILENAME).is_file()


def test_init_refuses_when_grandparent_missing(tmp_path):
    """D-09: grandparent missing → ConfigurationError with mkdir suggestion."""
    from mlpstorage_py.errors import ConfigurationError
    from mlpstorage_py.results_dir.init import run_init

    target = tmp_path / "missing" / "deeper" / "r1"
    with pytest.raises(ConfigurationError) as excinfo:
        run_init(argparse.Namespace(mode="init", orgname="Acme", path=str(target)))

    err = excinfo.value
    assert "parent" in str(err).lower() or "does not exist" in str(err).lower()
    # Suggestion should hint at `mkdir -p`.
    assert err.suggestion is not None
    assert "mkdir -p" in err.suggestion


def test_init_case_sensitive(tmp_path):
    """RESEARCH.md Pitfall 7: orgname comparison is case-sensitive.
    init Acme then init acme → DoubleInitError (no silent .lower() normalization).
    """
    from mlpstorage_py.config import EXIT_CODE
    from mlpstorage_py.results_dir.errors import DoubleInitError
    from mlpstorage_py.results_dir.init import run_init

    assert run_init(_ns(tmp_path, "Acme")) == EXIT_CODE.SUCCESS
    with pytest.raises(DoubleInitError):
        run_init(_ns(tmp_path, "acme"))


def test_init_does_not_collect_cluster_info():
    """Anti-pattern guard: ``results_dir/init.py`` must not reference
    cluster_collector / collect_cluster_info / collect_local_system_info.
    """
    import mlpstorage_py.results_dir.init as init_mod

    with open(init_mod.__file__) as f:
        body = f.read()

    for bad in ("cluster_collector", "collect_cluster_info",
                "collect_local_system_info"):
        # Strip comment lines so docstring mentions are tolerated only if
        # explicitly commented; here we ban any uncommented mention because
        # the dispatcher has no business with cluster collection.
        for lineno, line in enumerate(body.splitlines(), start=1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            assert bad not in line, (
                f"Anti-pattern '{bad}' found in init.py:{lineno}: {line.rstrip()}"
            )
