"""End-to-end integration tests for the systemname.yaml write hook in
Benchmark.run() — Phase 02 / Plan 02-05.

These tests exercise the FULL Benchmark.run() lifecycle (with lifecycle
methods mocked to avoid DLIO/MPI) and assert that the LIFE-01 hook fires:

- `args.command == 'run'` produces the canonical YAML file BEFORE
  `_start_timeseries_collection()` runs and BEFORE `_run()` launches.
- `args.command == 'datagen'` does NOT produce the file (D-12).
- Per-mode separation: `closed`/`open`/`whatif` land at three distinct paths (D-11).
- Second `run()` against the same path is a byte-identical no-op (D-9).
- Filesystem write failures abort `Benchmark.run()` BEFORE `_run()` (D-9 fail-closed).
- `schema_validator.validate_file()` reports errors only on the intentional
  blanks — never on the filled fields populated by `node_dict_from_host`
  (SER-03 success criterion #4).
- Homogeneous fleets produce one stanza with `quantity == fleet_size`;
  heterogeneous fleets produce multiple stanzas summing to `fleet_size`
  (SER-01 success criterion #3).

These tests use VectorDBBenchmark as the concrete Benchmark subclass — it
has the simplest `__init__` of any benchmark in the codebase and inherits
`run()` directly from `Benchmark`, which is the surface this plan covers.
"""

from __future__ import annotations

import sys
import time
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

# Stub heavy deps the benchmark imports expect (matches kvcache test file pattern).
for _dep in ('pyarrow', 'pyarrow.ipc', 'psutil'):
    if _dep not in sys.modules:
        sys.modules[_dep] = MagicMock()

from mlpstorage_py.cluster_collector import HostSystemInfo
from mlpstorage_py.rules.models import (
    HostCPUInfo,
    HostInfo,
    HostMemoryInfo,
)


# ---------------------------------------------------------------------------
# Host fixtures (mirror tests/unit/test_auto_generator_write.py)
# ---------------------------------------------------------------------------


def _make_host(
    *,
    cpu_model: str = "Intel(R) Xeon Platinum 8480+",
    num_cores: int = 56,
    num_sockets: int = 2,
    mem_bytes: int = 274_877_906_944,  # 256 GiB exact
    os_name: str = "Rocky Linux",
    os_version: str = "9.5",
    hostname: str = "h1",
) -> HostInfo:
    """Build a HostInfo with sensible Phase 2 defaults."""
    return HostInfo(
        hostname=hostname,
        cpu=HostCPUInfo(
            model=cpu_model,
            num_cores=num_cores,
            num_logical_cores=num_cores * 2,
            num_sockets=num_sockets,
            architecture="x86_64",
        ),
        memory=HostMemoryInfo(total=mem_bytes),
        system=HostSystemInfo(
            hostname=hostname,
            os_release={"NAME": os_name, "VERSION_ID": os_version},
        ),
    )


# ---------------------------------------------------------------------------
# Benchmark construction helper
# ---------------------------------------------------------------------------


def _vdb_args(tmp_path, *, command='run', mode='closed', orgname='Acme',
              systemname='sys-v1') -> Namespace:
    """Args namespace for VectorDBBenchmark construction."""
    return Namespace(
        debug=False,
        verbose=False,
        what_if=False,
        stream_log_level='INFO',
        mode=mode,
        orgname=orgname,
        systemname=systemname,
        results_dir=str(tmp_path),
        command=command,
        config='default',
        vdb_engine='milvus',
        host='127.0.0.1',
        port=19530,
        collection=None,
        category=None,
        num_query_processes=1,
        batch_size=1,
        runtime=60,
        queries=None,
        report_count=100,
    )


_RUN_DATETIME_COUNTER = [0]


def _unique_run_datetime() -> str:
    """Return a unique YYYYMMDD_HHMMSS-formatted string so back-to-back
    benchmark constructions in the same test don't collide on `reserve_run_directory`."""
    _RUN_DATETIME_COUNTER[0] += 1
    base = time.strftime("%Y%m%d_%H%M%S")
    # Append a monotonic counter; the run-directory reserver tolerates any
    # nonsense after the timestamp because it bumps on collision anyway,
    # but we want the very first attempt to be unique to avoid the
    # 10-bump collision-budget exhaustion.
    return f"{base}_{_RUN_DATETIME_COUNTER[0]:04d}"


def _make_benchmark(tmp_path, hosts, *, command='run', mode='closed',
                    orgname='Acme', systemname='sys-v1',
                    timeseries_side_effect=None):
    """Construct a VectorDBBenchmark with all lifecycle methods mocked.

    `_collect_cluster_start` is mocked to install `self._cluster_info_start`
    as a MagicMock with `host_info_list=hosts` — the production write hook
    consumes this attribute directly.
    """
    args = _vdb_args(tmp_path, command=command, mode=mode,
                     orgname=orgname, systemname=systemname)
    output_dir = str(tmp_path / f"output_{_RUN_DATETIME_COUNTER[0]}")
    with patch('mlpstorage_py.benchmarks.base.generate_output_location') as mock_gen, \
         patch('mlpstorage_py.benchmarks.vectordbbench.read_config_from_file', return_value={}), \
         patch('mlpstorage_py.benchmarks.vectordbbench.VectorDBBenchmark.verify_benchmark'), \
         patch('mlpstorage_py.benchmarks.vectordbbench.VectorDBBenchmark._validate_vdb_dependencies'):
        mock_gen.return_value = output_dir
        from mlpstorage_py.benchmarks.vectordbbench import VectorDBBenchmark
        bm = VectorDBBenchmark(args, run_datetime=_unique_run_datetime())

    # Mock lifecycle methods so we can drive `Benchmark.run()` without DLIO/MPI.
    bm._validate_environment = MagicMock()

    def _cluster_start_side_effect():
        bm._cluster_info_start = MagicMock(host_info_list=hosts)
        bm._collection_method = 'mpi'

    bm._collect_cluster_start = MagicMock(side_effect=_cluster_start_side_effect)
    bm._start_timeseries_collection = MagicMock(side_effect=timeseries_side_effect)
    bm._stop_timeseries_collection = MagicMock()
    bm._collect_cluster_end = MagicMock()
    bm.write_timeseries_data = MagicMock()
    bm._run = MagicMock(return_value=0)
    return bm


def _yaml_path(tmp_path, mode='closed', orgname='Acme', systemname='sys-v1') -> Path:
    return tmp_path / mode / orgname / 'systems' / f"{systemname}.yaml"


# ---------------------------------------------------------------------------
# LIFE-01 happy path
# ---------------------------------------------------------------------------


def test_full_run_writes_systemname_yaml(tmp_path):
    """LIFE-01 happy path: `command='run'` → file at canonical path with
    `quantity == 3` for a homogeneous 3-host fleet."""
    hosts = [_make_host(hostname=f"h{i}") for i in range(3)]
    bm = _make_benchmark(tmp_path, hosts)

    target = _yaml_path(tmp_path)
    # Pre-assert: file does not exist before run().
    assert not target.exists()

    rc = bm.run()
    assert rc == 0
    assert target.exists()

    data = yaml.safe_load(target.read_text())
    clients = data['system_under_test']['clients']
    assert len(clients) == 1
    assert clients[0]['quantity'] == 3

    bm._collect_cluster_start.assert_called_once()
    bm._start_timeseries_collection.assert_called_once()
    bm._run.assert_called_once()


# ---------------------------------------------------------------------------
# Hook ordering: file lands BEFORE _start_timeseries_collection()
# ---------------------------------------------------------------------------


def test_hook_fires_before_timeseries(tmp_path):
    """D-9 hook point: file must exist on disk BEFORE
    `_start_timeseries_collection()` runs."""
    hosts = [_make_host()]
    file_existed_at_timeseries = []
    target = _yaml_path(tmp_path)

    def _timeseries_side_effect():
        file_existed_at_timeseries.append(target.exists())

    bm = _make_benchmark(tmp_path, hosts,
                         timeseries_side_effect=_timeseries_side_effect)
    bm.run()

    assert file_existed_at_timeseries == [True], (
        "systemname.yaml must exist on disk before "
        "_start_timeseries_collection() runs"
    )


# ---------------------------------------------------------------------------
# D-12: datagen does NOT write
# ---------------------------------------------------------------------------


def test_datagen_does_not_write(tmp_path):
    """D-12: `command='datagen'` → writer's own gate returns None, no file."""
    hosts = [_make_host()]
    bm = _make_benchmark(tmp_path, hosts, command='datagen')

    bm.run()

    target = _yaml_path(tmp_path)
    assert not target.exists()
    # The rest of the lifecycle still proceeds.
    bm._run.assert_called_once()


# ---------------------------------------------------------------------------
# D-11: per-mode separation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["closed", "open", "whatif"])
def test_per_mode_separation(tmp_path, mode):
    """D-11: each mode lands its file at `<rd>/<mode>/<org>/systems/<sn>.yaml`."""
    hosts = [_make_host()]
    bm = _make_benchmark(tmp_path, hosts, mode=mode)
    bm.run()

    target = _yaml_path(tmp_path, mode=mode)
    assert target.exists(), f"expected file at {target}"


def test_per_mode_three_distinct_files(tmp_path):
    """D-11 cross-check: cycling all three modes against the same results-dir
    produces three distinct files at distinct paths."""
    hosts = [_make_host()]
    for mode in ("closed", "open", "whatif"):
        bm = _make_benchmark(tmp_path, hosts, mode=mode)
        bm.run()

    paths = [_yaml_path(tmp_path, mode=m) for m in ("closed", "open", "whatif")]
    assert all(p.exists() for p in paths)
    # All three are at DIFFERENT paths.
    assert len({str(p) for p in paths}) == 3


# ---------------------------------------------------------------------------
# D-9: second-run no overwrite
# ---------------------------------------------------------------------------


def test_second_run_no_overwrite(tmp_path):
    """LIFE-01 success criterion #5: second run produces no error and byte-identical file."""
    hosts = [_make_host()]
    bm1 = _make_benchmark(tmp_path, hosts)
    bm1.run()
    target = _yaml_path(tmp_path)
    snapshot_bytes = target.read_bytes()
    snapshot_mtime = target.stat().st_mtime

    # Second run, fresh benchmark, same systemname/results-dir.
    bm2 = _make_benchmark(tmp_path, hosts)
    bm2.run()

    assert target.read_bytes() == snapshot_bytes, (
        "file content must be byte-identical after second run (D-9 no-op-if-exists)"
    )
    assert target.stat().st_mtime == snapshot_mtime, (
        "file mtime must be unchanged after second run (no rewrite)"
    )


# ---------------------------------------------------------------------------
# D-9: filesystem failure aborts Benchmark.run() BEFORE _run()
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="filesystem semantics differ on Windows",
)
def test_filesystem_failure_propagates(tmp_path):
    """D-9 fail-closed: a non-FileExistsError filesystem error during the
    write (e.g. PermissionError / ENOSPC / IsADirectoryError) propagates
    out of `Benchmark.run()` and prevents `_run()` from being called.

    We inject the error via `patch('os.open')` so we can reproduce the
    OSError behavior deterministically across Linux/macOS without relying
    on uid-specific filesystem-permission tricks. The patch targets the
    write site inside `auto_generator`, NOT global os.open (which would
    break unrelated I/O in the lifecycle methods).
    """
    hosts = [_make_host()]
    bm = _make_benchmark(tmp_path, hosts)

    # Inject a non-FileExistsError into the writer's os.open call.
    def _raise_perm(*args, **kwargs):
        raise PermissionError("simulated write failure")

    with patch('mlpstorage_py.system_description.auto_generator.os.open',
               side_effect=_raise_perm):
        with pytest.raises(PermissionError):
            bm.run()

    # The benchmark MUST NOT have reached _run() after a write failure.
    bm._run.assert_not_called()


# ---------------------------------------------------------------------------
# SER-03 success criterion #4: validator errors only on intentional blanks
# ---------------------------------------------------------------------------


def test_validator_errors_only_on_blanks(tmp_path):
    """SER-03 success criterion #4: `schema_validator.validate_file()` reports
    errors ONLY on the intentional blanks (D-2 / D-3 / D-14 omissions) — never
    on the fields populated by `node_dict_from_host` from real cluster data."""
    from mlpstorage_py.system_description import schema_validator

    hosts = [_make_host()]
    bm = _make_benchmark(tmp_path, hosts)
    bm.run()
    target = _yaml_path(tmp_path)
    assert target.exists()

    errors = schema_validator.validate_file(str(target))
    # `validate_file` returns a list of human-readable strings of the form
    # "system_under_test -> clients -> 0 -> chassis -> model_name: ..."
    # Build a set of dotted paths for easier substring assertions.
    error_paths = {e.split(":", 1)[0].strip() for e in errors}

    # Intentional blanks — at least these locations MUST appear among errors.
    expected_blanks = [
        "system_under_test -> solution",  # D-14
        "system_under_test -> deployment",  # D-14
        "system_under_test -> clients -> 0 -> friendly_description",  # D-2
        "system_under_test -> clients -> 0 -> chassis -> model_name",  # D-2 (Phase 3)
    ]
    for path in expected_blanks:
        assert any(path in p for p in error_paths), (
            f"expected error at intentional blank {path!r}; got errors:\n"
            + "\n".join(sorted(error_paths))
        )

    # Stub list fields (D-3) — at least one error under networking[*] and drives[*].
    assert any("networking" in p for p in error_paths), (
        "expected at least one error under clients[].networking[*]"
    )
    assert any("drives" in p for p in error_paths), (
        "expected at least one error under clients[].drives[*]"
    )

    # Filled fields — these MUST NOT appear in any error path.
    forbidden_in_errors = [
        "chassis -> cpu_model",
        "chassis -> cpu_qty",
        "chassis -> cpu_cores",
        "chassis -> memory_capacity",
        "operating_system -> name",
        "operating_system -> version",
    ]
    for field in forbidden_in_errors:
        for ep in error_paths:
            assert field not in ep, (
                f"filled field {field!r} unexpectedly appears in error path "
                f"{ep!r} — node_dict_from_host should have populated it"
            )


# ---------------------------------------------------------------------------
# SER-01 success criterion #3: homogeneous vs heterogeneous quantity grouping
# ---------------------------------------------------------------------------


def test_homogeneous_fleet_quantity_equals_fleet_size(tmp_path):
    """SER-01 success criterion #3: 3 identical hosts → 1 stanza, `quantity == 3`."""
    hosts = [_make_host(hostname=f"h{i}") for i in range(3)]
    bm = _make_benchmark(tmp_path, hosts)
    bm.run()

    data = yaml.safe_load(_yaml_path(tmp_path).read_text())
    clients = data['system_under_test']['clients']
    assert len(clients) == 1
    assert clients[0]['quantity'] == 3
    assert sum(c['quantity'] for c in clients) == 3


def test_heterogeneous_fleet_produces_multiple_stanzas(tmp_path):
    """SER-01 success criterion #3: 1 host with a different cpu_model →
    2 stanzas; quantities sum to fleet size."""
    hosts = [
        _make_host(hostname="h0", cpu_model="Intel(R) Xeon Platinum 8480+"),
        _make_host(hostname="h1", cpu_model="Intel(R) Xeon Platinum 8480+"),
        _make_host(hostname="h2", cpu_model="AMD EPYC 9654"),
    ]
    bm = _make_benchmark(tmp_path, hosts)
    bm.run()

    data = yaml.safe_load(_yaml_path(tmp_path).read_text())
    clients = data['system_under_test']['clients']
    assert len(clients) == 2
    assert sum(c['quantity'] for c in clients) == 3
    # D-7 sort: largest quantity first.
    assert clients[0]['quantity'] >= clients[1]['quantity']


# ---------------------------------------------------------------------------
# CR-01 regression — production single-node-fallback path
#
# The 12 tests above all go through `_make_benchmark`, which installs a
# `_collect_cluster_start` mock that seeds `self._cluster_info_start` BEFORE
# the Phase 2 write hook reads it at base.py:991. That masking pattern hides
# the production crash documented in `02-VERIFICATION.md` (status=gaps_found)
# and independently confirmed as CR-01 BLOCKER in `02-REVIEW.md`:
#
#   - `Benchmark.__init__` never initializes `self._cluster_info_start`.
#   - `_collect_cluster_start()` has an early-return branch at base.py:634-636
#     that fires on (a) `datagen`/`configview` commands and (b) any benchmark
#     whose `--hosts` default is None (e.g. VectorDB at cli/vectordb_args.py:107).
#   - On the early-return path, `_cluster_info_start` is never assigned, and
#     the write hook at base.py:991 raises AttributeError when it reads it.
#   - The catch-all `except Exception` at base.py:997 relabels the error as
#     "Failed to write systemname.yaml: ..." and re-raises — aborting the
#     benchmark BEFORE DLIO launches. (This is a regression from Phase 1:
#     production `datagen` used to complete normally.)
#
# These two regression tests construct a `VectorDBBenchmark` WITHOUT the
# `_collect_cluster_start` mock that `_make_benchmark` installs, so the real
# `_collect_cluster_start` runs and hits the early-return path. Both
# subcases (datagen and run-with-no-hosts) MUST run without AttributeError
# after the init-side fix in `Benchmark.__init__` lands.
# ---------------------------------------------------------------------------


def _make_benchmark_no_cluster_mock(tmp_path, *, command='run', mode='closed',
                                    orgname='Acme', systemname='sys-v1'):
    """Construct VectorDBBenchmark WITHOUT mocking `_collect_cluster_start`.

    This is the production-path harness that exposes CR-01. Everything else
    that `_make_benchmark` mocks is still mocked here (validate_environment,
    timeseries, cluster_end, write_timeseries_data, _run) — only the
    cluster-start mock is omitted so the real early-return path executes.
    """
    args = _vdb_args(tmp_path, command=command, mode=mode,
                     orgname=orgname, systemname=systemname)
    output_dir = str(tmp_path / f"output_{_RUN_DATETIME_COUNTER[0]}")
    with patch('mlpstorage_py.benchmarks.base.generate_output_location') as mock_gen, \
         patch('mlpstorage_py.benchmarks.vectordbbench.read_config_from_file', return_value={}), \
         patch('mlpstorage_py.benchmarks.vectordbbench.VectorDBBenchmark.verify_benchmark'), \
         patch('mlpstorage_py.benchmarks.vectordbbench.VectorDBBenchmark._validate_vdb_dependencies'):
        mock_gen.return_value = output_dir
        from mlpstorage_py.benchmarks.vectordbbench import VectorDBBenchmark
        bm = VectorDBBenchmark(args, run_datetime=_unique_run_datetime())

    # Mock everything EXCEPT _collect_cluster_start — the whole point of this
    # harness is to let the real early-return execute and trigger CR-01.
    bm._validate_environment = MagicMock()
    bm._start_timeseries_collection = MagicMock()
    bm._stop_timeseries_collection = MagicMock()
    bm._collect_cluster_end = MagicMock()
    bm.write_timeseries_data = MagicMock()
    bm._run = MagicMock(return_value=0)
    return bm


def test_run_does_not_raise_when_cluster_info_start_attribute_is_uninitialized_datagen(tmp_path):
    """CR-01 datagen-subcase: `command='datagen'` hits `_collect_cluster_start`
    early-return at base.py:634-636 (datagen short-circuit in
    `_should_collect_cluster_info`). The write hook at base.py:991 then reads
    `self._cluster_info_start`. Before the fix this raises AttributeError;
    after the fix the attribute is None-by-init, the writer's D-12 command
    gate at auto_generator.py:443 fires cleanly, and no file is written.
    """
    bm = _make_benchmark_no_cluster_mock(tmp_path, command='datagen')

    # Precautionary patch on the D-8 fallback's local-collection call.
    # For datagen this path is unreachable (D-12 gate fires first), but the
    # patch keeps the test hermetic against any future change that lets it
    # run on this subcase.
    with patch(
        'mlpstorage_py.system_description.auto_generator.collect_local_system_info',
        return_value={'meminfo': {'MemTotal': 0}, 'cpuinfo': '', 'os_release': {}},
    ):
        # PRIMARY ASSERTION (RED before fix, GREEN after):
        # Pre-fix, base.py:991 reads `self._cluster_info_start` after the
        # early-return at base.py:634-636 left it unset → AttributeError,
        # relabeled by the catch-all at base.py:997 as
        # "Failed to write systemname.yaml: ...". Post-fix, the init-side
        # default makes the read succeed, D-12 gates the write off cleanly,
        # and run() returns 0.
        rc = bm.run()

    assert rc == 0
    # Post-fix lock: the attribute is present (init-side fix) and None.
    assert hasattr(bm, '_cluster_info_start')
    assert bm._cluster_info_start is None
    # D-12 datagen-no-write contract: no file at canonical D-11 path.
    assert not _yaml_path(tmp_path, mode='closed', orgname='Acme',
                          systemname='sys-v1').exists()
    # The early-return left _cluster_info_start as the init-side default.
    assert bm._cluster_info_start is None


def test_run_does_not_raise_when_cluster_info_start_attribute_is_uninitialized_run(tmp_path):
    """CR-01 run-subcase: `command='run'` with no `--hosts` (mirrors VectorDB's
    `cli/vectordb_args.py:107` `default=None`) hits the same early-return at
    base.py:634-636 because both `_should_collect_cluster_info()` (no hosts)
    and `_should_use_ssh_collection()` (no hosts) return False. The write
    hook then reads `self._cluster_info_start`. Before the fix this raises
    AttributeError; after the fix the attribute is None-by-init, the writer's
    D-12 gate passes (command=='run'), the D-8 fallback at
    auto_generator.py:374-378 takes over with `cluster_info=None`, and the
    file IS written at the canonical D-11 path via local collection.
    """
    bm = _make_benchmark_no_cluster_mock(tmp_path, command='run')

    # D-8 fallback hermetic stub: _resolve_host_info_list(None) calls
    # collect_local_system_info(). Feed it a minimal HostInfo-compatible
    # dict so HostInfo.from_collected_data produces a populated host.
    fake_local = {
        'hostname': 'localhost',
        'meminfo': {'MemTotal': 274_877_906_944},
        'cpuinfo': (
            'processor\t: 0\n'
            'physical id\t: 0\n'
            'model name\t: Intel(R) Xeon Platinum 8480+\n'
            'cpu cores\t: 56\n'
        ),
        'os_release': {'NAME': 'Rocky Linux', 'VERSION_ID': '9.5'},
        'cmdline': '',
        'uname': {'machine': 'x86_64'},
    }
    with patch(
        'mlpstorage_py.system_description.auto_generator.collect_local_system_info',
        return_value=fake_local,
    ):
        # PRIMARY ASSERTION (RED before fix, GREEN after):
        # Pre-fix, base.py:991 reads `self._cluster_info_start` after the
        # early-return at base.py:634-636 left it unset → AttributeError,
        # relabeled by the catch-all at base.py:997 as
        # "Failed to write systemname.yaml: ...". Post-fix, the init-side
        # default makes the read succeed, D-12 gates pass through, D-8
        # fallback runs collect_local_system_info(), and the file lands.
        rc = bm.run()

    assert rc == 0
    # Post-fix lock: the attribute is present (init-side fix) and None.
    assert hasattr(bm, '_cluster_info_start')
    assert bm._cluster_info_start is None
    target = _yaml_path(tmp_path, mode='closed', orgname='Acme',
                       systemname='sys-v1')
    # D-8 fallback ran; D-12 gate passed; D-9 atomic write succeeded.
    assert target.exists()
    data = yaml.safe_load(target.read_text())
    # Non-empty valid YAML, system_under_test.clients populated.
    assert 'system_under_test' in data
    clients = data['system_under_test']['clients']
    assert isinstance(clients, list)
    assert len(clients) >= 1
    assert sum(c.get('quantity', 0) for c in clients) >= 1
