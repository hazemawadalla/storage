---
phase: 04-sysctl-environment-and-drives-coverage
plan: 03
subsystem: cluster_collector
tags: [collector, drives, lsblk, pattern-b, COLL-07, D-30, D-31, D-33, D-36]
requires:
  - mlpstorage_py/cluster_collector.py (sysctl + environment blocks from Plans 04-01 / 04-02 as the insertion-point precedent)
  - MPI_COLLECTOR_SCRIPT Pattern B body (carried from Plans 03-02, 03-03, 04-01, 04-02)
  - subprocess + json modules (already imported in module; subprocess newly added to MPI script imports)
provides:
  - mlpstorage_py.cluster_collector.collect_drives
  - mlpstorage_py.cluster_collector._LSBLK_ARGS
  - mlpstorage_py.cluster_collector._DRIVE_VIRTUAL_NAME_PREFIXES
  - mlpstorage_py.cluster_collector._DRIVE_VIRTUAL_TRANS
  - mlpstorage_py.cluster_collector._DRIVE_ACCEPTED_TRANS
  - MPI_COLLECTOR_SCRIPT inline twins of collect_drives + 4 constants
  - collect_local_system_info result['drives'] key (always-present list contract; D-33 omit-when-empty fires at the auto_generator transform layer in Plan 04-04, not here)
affects:
  - mlpstorage_py/cluster_collector.py (module + MPI script body; subprocess added to script imports)
  - tests/unit/test_cluster_collector.py (TestDrivesCollector + TestDrivesMPIScriptParity + TestDrivesWiring)
tech-stack:
  added:
    - "First subprocess.run invocation inside MPI_COLLECTOR_SCRIPT (sysctl + environment were pure file/env reads; drives collector is the first script-side subprocess shell-out)."
  patterns:
    - "D-2 universal-failure rule at two scopes: outer (FileNotFoundError / TimeoutExpired / JSONDecodeError / non-zero exit → []) and per-row (malformed size / missing keys → skip single row)."
    - "D-31 four-rule filter chain in early-continue ladder form (mirror of collect_networking lines 933-967)."
    - "D-36 Pattern B MPI script twin discipline (untyped form: `frozenset(['...'])` literal instead of `Final[frozenset]`, no PEP-585 subscripted generics, manual sync between module + script bodies)."
    - "RESEARCH Q1 quirks honored: (a) empty-TRAN-nvme-name rescue, (b) RM string/int/bool variants, (c) decimal GB nameplate convention (// 10**9)."
key-files:
  created:
    - .planning/phases/04-sysctl-environment-and-drives-coverage/04-03-SUMMARY.md
  modified:
    - mlpstorage_py/cluster_collector.py
    - tests/unit/test_cluster_collector.py
decisions:
  - "RM coercion uses `rm in (1, True, '1')` membership check instead of the PLAN's verbatim `str(row.get('rm','0')) == '1'` form. Rule 1 fix (auto-fix bug) — during smoke verification, real WSL2 dev shell lsblk -J emits `'rm': false` / `'rota': true` as JSON booleans (not strings or ints). Python's `str(True) == 'True'`, not `'1'`, so the PLAN's verbatim form would have silently failed to filter removable drives on hosts using newer util-linux JSON output. RESEARCH Q1 documented string-vs-int variance but missed the boolean variant. The membership check handles all three (string '1', int 1, bool True) uniformly. Mirrored in both module + script bodies. New regression test `test_removable_rm_bool_skipped` locks the contract."
  - "Pattern B script inlines _LSBLK_ARGS + the three filter constants (one tuple + two frozensets) as untyped form. Frozenset literal uses `frozenset(['a','b'])` rather than `frozenset({'a','b'})` for parity with the existing Plan 04-01 script style (the latter is technically 3.8-safe but the iterable-arg form is the established convention in the script body)."
  - "`import subprocess` added to MPI_COLLECTOR_SCRIPT imports block. This is the first subprocess.run invocation inside the script body — Plans 03-02 (chassis), 03-03 (networking), 04-01 (sysctl), and 04-02 (environment) all used pure file/env reads. Sole new module dependency from the script side."
  - "The TestDrivesMPIScriptParity test injects a mock `subprocess` symbol into the exec'd script namespace (`ns['subprocess'] = mock_subprocess`) BEFORE calling `ns['collect_drives']()`. The script body references the bare `subprocess` name at call time; replacing the namespace symbol intercepts the call without monkeypatching builtins. The mock preserves the real `subprocess.TimeoutExpired` and `subprocess.SubprocessError` attributes so the script's `except (subprocess.TimeoutExpired, subprocess.SubprocessError, ...)` tuple still resolves at module-load time inside exec."
  - "D-33 omit-when-empty behavior is the auto_generator transform layer's responsibility (Plan 04-04), NOT this collector's. `collect_drives()` returns `[]` for both 'lsblk absent' and 'lsblk returned but all rows filtered' per the universal D-2 rule; `collect_local_system_info` wires that as `result['drives'] = []` (always-present-list contract); the splice layer in Plan 04-04 then conditionally omits the key. This matches the architectural split documented in 04-02-SUMMARY.md Forward Notes."
metrics:
  duration_minutes: ~22
  completed_date: 2026-06-23
  tasks_completed: 2
  files_created: 1
  files_modified: 2
  commits: 2
---

# Phase 04 Plan 03: Drives Collector Summary

Drives collection via `lsblk -J -b -d -o NAME,MODEL,VENDOR,SIZE,ROTA,TRAN,RM` (COLL-07) with the D-31 four-rule filter chain, D-30 emit shape, D-33 universal `[]` on absence/empty, and D-36 Pattern B MPI script twin. Shipped in two-commit RED/GREEN cadence; 20 new tests green, no regressions across 247-test cluster_collector suite, one Rule 1 fix for util-linux JSON boolean RM observed during dev-shell smoke verification.

## What Shipped

**1. Module-side collector** — `mlpstorage_py/cluster_collector.py`:

- `_LSBLK_ARGS: Final[Tuple[str, ...]] = ('lsblk', '-J', '-b', '-d', '-o', 'NAME,MODEL,VENDOR,SIZE,ROTA,TRAN,RM')` — single source of truth.
- `_DRIVE_VIRTUAL_NAME_PREFIXES: Final[Tuple[str, ...]] = ('loop', 'dm-', 'zram', 'ram', 'sr', 'fd')` (D-31 rule 2 name-prefix).
- `_DRIVE_VIRTUAL_TRANS: Final[frozenset] = frozenset({'loop', 'zram'})` (D-31 rule 2 virtual TRAN).
- `_DRIVE_ACCEPTED_TRANS: Final[frozenset] = frozenset({'nvme', 'sata', 'sas'})` (D-31 rule 3 accept list).
- `collect_drives() -> List[Dict[str, Any]]` — outer try wraps the `subprocess.run(list(_LSBLK_ARGS), capture_output=True, text=True, timeout=10)` call, JSON parse, and the per-row filter loop. Non-zero returncode → `[]`. Per-row try/except isolates malformed rows. D-31 chain applied in order:
  1. RM=1 skip via `rm in (1, True, '1')` membership (string/int/bool variants per RESEARCH Q1 + Rule 1 fix).
  2. Virtual NAME prefix skip via `name.startswith(_DRIVE_VIRTUAL_NAME_PREFIXES)`.
  3. Virtual TRAN skip via `tran in _DRIVE_VIRTUAL_TRANS`.
  4. Unknown-TRAN drop unless `(tran == '' and name.startswith('nvme'))`, in which case rescue to `tran = 'nvme'`.
- Per-row emit: `{vendor_name: (row.get('vendor') or '').strip(), model_name: (row.get('model') or '').strip(), interface: tran, capacity_in_GB: int(row['size']) // 10**9}`. Decimal GB nameplate convention per RESEARCH Q1.

**2. `collect_local_system_info` wiring** — three-line try/except block immediately after the Plan 04-02 environment block, mirroring chassis_model / networking / sysctl / environment exactly:
```python
try:
    result['drives'] = collect_drives()
except Exception as e:
    result['errors']['drives'] = str(e)
    result['drives'] = []
```

**3. Pattern B twins in `MPI_COLLECTOR_SCRIPT`** (D-36):
- Added `import subprocess` to the script's import block (first subprocess shell-out in the script body).
- Inline `_LSBLK_ARGS` (tuple), `_DRIVE_VIRTUAL_NAME_PREFIXES` (tuple), `_DRIVE_VIRTUAL_TRANS` (`frozenset([...])` iterable-arg form), `_DRIVE_ACCEPTED_TRANS` (same).
- Inline `collect_drives()` (untyped twin — no `Final[]`, no `List[Dict[str, Any]]` annotations).
- Parallel try/except wiring in script's `collect_local_info` after the environment block.

**4. Tests** — 20 new tests across 3 new classes:

| Class                                                          | Tests | Purpose                                                                                       |
| -------------------------------------------------------------- | ----- | --------------------------------------------------------------------------------------------- |
| `tests/unit/test_cluster_collector.py::TestDrivesCollector`    | 16    | D-31 four-rule filter chain + D-30 emit shape + RESEARCH Q1 quirks + D-33 universal failure cases (15 planned + 1 Rule 1 regression `test_removable_rm_bool_skipped`) |
| `tests/unit/test_cluster_collector.py::TestDrivesMPIScriptParity` | 1   | Pattern B (D-36) — exec script, inject mock subprocess into ns, assert collect_drives parity |
| `tests/unit/test_cluster_collector.py::TestDrivesWiring`       | 3     | result['drives'] always-present list contract + happy-path + failure-isolation                |

## Two-Commit RED/GREEN Cadence

| Commit  | Type        | Files                                                                                             | Purpose                                                                                                       |
| ------- | ----------- | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| d4d9e9c | test(04-03) | tests/unit/test_cluster_collector.py                                                              | RED — 19 failing tests across TestDrivesCollector + TestDrivesMPIScriptParity + TestDrivesWiring (AttributeError / ImportError / AssertionError mix). |
| 2766aeb | feat(04-03) | mlpstorage_py/cluster_collector.py, tests/unit/test_cluster_collector.py (Rule 1 regression test) | GREEN — collect_drives module + MPI script twin + collect_local_system_info wiring + RM-bool Rule 1 fix + regression test. |

## Whether `import subprocess` / `import json` Needed Adding

| Symbol            | Module side                                                 | MPI script side                                  |
| ----------------- | ----------------------------------------------------------- | ------------------------------------------------ |
| `import subprocess` | Already present (line 15; sysctl + other code uses it)    | **Added in GREEN commit** — first script-side subprocess usage |
| `import json`     | Already present (line 10)                                   | Already present (line 1515; cgroups uses it) — no change |

## MPI Parity Test — Subprocess Injection Pattern

The script body references the bare `subprocess` name at call time (`subprocess.run(...)`, `subprocess.TimeoutExpired`, `subprocess.SubprocessError`). To intercept the call without monkeypatching builtins, the parity test injects a mock symbol into the exec'd namespace BEFORE invoking `ns['collect_drives']()`:

```python
mock_subprocess = MagicMock()
mock_subprocess.run = lambda *a, **k: _lsblk_cp(payload)
# Preserve real exception types so the script's except (subprocess.TimeoutExpired,
# subprocess.SubprocessError, ...) tuple resolves cleanly:
mock_subprocess.TimeoutExpired = subprocess.TimeoutExpired
mock_subprocess.SubprocessError = subprocess.SubprocessError
ns["subprocess"] = mock_subprocess

monkeypatch.setattr(cc.subprocess, "run", lambda *a, **k: _lsblk_cp(payload))

a = ns["collect_drives"]()
b = collect_drives()
assert a == b
```

This is the new pattern in this plan — Plans 04-01 (sysctl) and 04-02 (environment) used file-tree / monkeypatch.setenv fixtures and didn't need namespace injection. Future Pattern B parity tests for any collector that shells out (or for the existing chassis_model, which does file I/O) can follow this same shape if the file system layer ever needs intercepting.

## Dev-Shell Smoke Verification

`lsblk` IS installed on this WSL2 dev shell (`/usr/bin/lsblk`). Real output:

```json
{
   "blockdevices": [
      {"name": "sda", "model": "Virtual Disk", "vendor": "Msft    ",
       "size": 374251520, "rota": true, "tran": null, "rm": false},
      {"name": "sdb", "model": "Virtual Disk", ..., "tran": null, "rm": false},
      {"name": "sdc", "model": "Virtual Disk", ..., "tran": null, "rm": false},
      {"name": "sdd", "model": "Virtual Disk", ..., "tran": null, "rm": false}
   ]
}
```

`python3 -c "from mlpstorage_py.cluster_collector import collect_drives; print(collect_drives())"` returns `[]`. Reason: all four rows have `tran: null` (becomes Python `''` after `(row.get('tran') or '').lower()`) AND `name` starts with `sd` (not `nvme`), so D-31 rule 3 drops them. The D-33 universal-empty path is exercised end-to-end, validating the architectural contract that the splice layer (Plan 04-04) will omit the `drives` key from the YAML when the host has no physical drives the collector can recognize.

## RESEARCH Q1 Quirks Observed in Real WSL2 Output

| RESEARCH Q1 prediction                                                            | WSL2 actual observation                                                       | Plan 04-03 handling                                                                                            |
| --------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| TRAN values across distros include `""`, `nvme`, `sata`, `sas`, `usb`, `virtio`, etc.; older NVMe kernels emit `""` | `"tran": null` (JSON null) — Microsoft's Hyper-V virtio-block surfaces as null | `(row.get('tran') or '').lower()` collapses both `None` and `''` to `''`; rule 3 drops the row unless NAME starts with `nvme` (rescue path) |
| RM is string `"0"`/`"1"` in util-linux <2.37, int `0`/`1` in util-linux ≥2.37     | `"rm": false` / `"rm": true` (JSON boolean) — third variant not in RESEARCH | Rule 1 fix: `rm in (1, True, '1')` membership check handles all three. PLAN's verbatim `str(rm)=='1'` would have silently failed on bool (`str(True)=='True'`). |
| SIZE is bytes when `-b` passed                                                    | `"size": 374251520` (int, not string)                                         | `int(row['size'])` handles both int and string; works.                                                          |
| ROTA is string `"0"`/`"1"`                                                        | `"rota": true` / `"rota": false` (boolean)                                    | Not currently consumed by D-31 filter (rota would matter if D-31 had a "spinning disk" rule, which it doesn't). |
| JSON output requires util-linux ≥2.27 (Nov 2015)                                  | WSL2 is util-linux 2.39 — works fine                                          | No fallback needed; D-33 covers older versions via `[]` return.                                                 |

The RESEARCH Q1 surprise: util-linux 2.39 (current Ubuntu / Debian / Microsoft WSL2) emits `rm` and `rota` as JSON booleans rather than ints. RESEARCH Q1 cited the v2.37 changelog "lsblk: add --json output type coercion" but the coercion target was bool, not int, for flag-shaped fields. The Plan 04-03 Rule 1 fix (`rm in (1, True, '1')`) handles all three util-linux variants uniformly going forward.

## Verification

```bash
# All drive collector + MPI-parity + wiring tests pass (20 of 20).
python3 -m pytest tests/unit/test_cluster_collector.py::TestDrivesCollector \
    tests/unit/test_cluster_collector.py::TestDrivesMPIScriptParity \
    tests/unit/test_cluster_collector.py::TestDrivesWiring -q
# → 20 passed

# Full cluster_collector test suite (no regressions).
python3 -m pytest tests/unit/test_cluster_collector.py -q \
    --ignore=tests/unit/test_benchmarks_base.py \
    --ignore=tests/unit/test_parquet_reader.py \
    --ignore=tests/unit/test_vdb_modular_fake_backend.py
# → 247 passed

# Module-side smoke (returns [] on this WSL2 dev shell — all rows have
# tran=null and don't start with nvme; D-33 fires end-to-end).
python3 -c "from mlpstorage_py.cluster_collector import collect_drives; \
    print(type(collect_drives()).__name__, collect_drives())"
# → list []

# Two defs of collect_drives (module + script twin).
grep -v '^[[:space:]]*#' mlpstorage_py/cluster_collector.py | grep -c 'def collect_drives'
# → 2

# _LSBLK_ARGS referenced in both module and script body.
grep -c "_LSBLK_ARGS" mlpstorage_py/cluster_collector.py
# → 5  (≥ 2 required: 1 module const + 1 module consumer + 1 script const + 1 script consumer + 1 comment)
```

All plan-level `<verification>` items satisfied. All six `<success_criteria>` satisfied (filter chain, decimal GB floor, empty-TRAN-nvme rescue, omitted media_type/form_factor/performance, MPI script parity, D-33 absent/empty behavior).

## Deviations from Plan

### Rule 1 — RM coercion needed bool support (auto-fix bug)

**Found during:** Task 2 GREEN smoke verification.

**Issue:** PLAN's verbatim form `if str(row.get('rm', '0')) == '1': continue` works for util-linux string variants ('1') and int variants (1), but fails on JSON boolean variants (True). `str(True) == 'True'`, not `'1'`. This was discovered during the post-GREEN smoke run when checking actual `lsblk -J -b` output on the WSL2 dev shell — util-linux 2.39 (current Ubuntu / Debian / Microsoft WSL2) emits `"rm": false` / `"rm": true` as JSON booleans, diverging from RESEARCH Q1's predicted "string-or-int" variance.

**Risk:** A submitter running on a host with util-linux ≥2.37+ (the JSON-boolean variant) AND with an attached removable drive that lsblk recognizes (e.g., a USB SSD presenting as TRAN `sata` or `sas`) would have that drive INCORRECTLY emit into `clients[].drives[]`. The drive would have wrong `capacity_in_GB` (size of the USB device, not the actual server storage), and the cross-host fingerprint grouping in Plan 04-04 would split otherwise-identical hosts based on which one had the USB stick plugged in.

**Fix:** Replaced the str-coercion form with a membership check that handles all three variants uniformly:

```python
rm = row.get('rm', 0)
if rm in (1, True, '1'):
    continue
```

Mirrored in both module and MPI script bodies. Added `test_removable_rm_bool_skipped` regression test that covers the WSL2-observed variant (`rm: false` for a surviving NVMe drive + `rm: true` for a removable SATA drive).

**Why Rule 1 (not Rule 2 or 4):** This is a correctness bug in the filter implementation, not a missing critical feature (Rule 2) and not an architectural change (Rule 4). The filter contract was always "skip removable drives"; the fix preserves the contract under a wider input domain.

**Files modified:** mlpstorage_py/cluster_collector.py (2 fix sites — module + script body), tests/unit/test_cluster_collector.py (1 new test).
**Commit:** 2766aeb (folded into GREEN since it's a structural part of making the contract work on real hosts).

No Rule 2 / Rule 3 / Rule 4 deviations triggered.

## Authentication Gates

None.

## Threat Flags

None — `collect_drives()` shells out to `/usr/bin/lsblk` with a hard-coded literal arg list (no user input flows into the command line); the JSON parse is on lsblk's controlled stdout; the per-row try/except prevents a hostile lsblk output from crashing the collector. The Pattern B duplication does not introduce new code-injection surface beyond what Plans 03-02, 03-03, 04-01, and 04-02 already shipped.

## Known Stubs

None — all production code paths shipped in this plan flow data end-to-end. `result['drives']` is populated by `collect_drives()` which calls real subprocess. The downstream YAML emit path (auto_generator transform extension in Plan 04-04, HostInfo extension in Plan 04-05) is the next two plans' responsibility but does not block this plan's correctness.

## Forward Notes for Plan 04-04

Plan 04-04 ships the auto_generator transform-layer extensions:

- Three new fingerprint signature extractors: `_sysctl_signature`, `_environment_signature`, `_drive_signature`.
- Generalized `_resolve_fingerprint_key` dispatch with `_EXTRACTOR_SOURCE_KEYS` map.
- `_FINGERPRINT_KEYS` grows from Phase-3 8-tuple to 11-tuple (3 new callable extractors at tail).
- `_splice_stub_lists` extended with the D-33 conditional drives-omit branch: when `client.get('drives')` is empty/missing, `client.pop('drives', None)` removes the key entirely (instead of the Phase 2 `_DRIVE_STUB` unconditional splice).

D-33's downstream contract: a host with no recognized drives (lsblk absent, all rows filtered, container without device access, WSL2 with virtio-block) will produce a client stanza with NO `drives:` key in the YAML. This is the SER-02 signal to the submitter that "the collector couldn't determine this; submitter must hand-fill if applicable."

Plan 04-05 then ships HostInfo.drives field + node_dict_from_host per-host group_by_fingerprint pass for drives (collapsing identical drive rows into unit_count'd stanzas, mirroring the networking per-host grouping pattern from Phase 3).

## Self-Check: PASSED

- mlpstorage_py/cluster_collector.py: `collect_drives` defined in module (line ~1252) and inline in MPI_COLLECTOR_SCRIPT (line ~2280); non-comment grep returns 2.
- mlpstorage_py/cluster_collector.py: `_LSBLK_ARGS` defined in both module and script body (grep returns 5 references including consumers).
- mlpstorage_py/cluster_collector.py: `collect_local_system_info` wired with try/except (mirror of environment block) — `result['drives']` always-present-list contract.
- mlpstorage_py/cluster_collector.py: MPI script's `collect_local_info` wired with parallel try/except.
- mlpstorage_py/cluster_collector.py: `import subprocess` present in MPI script imports block.
- tests/unit/test_cluster_collector.py: 3 new test classes (20 tests total) green.
- Commit d4d9e9c (RED): present in `git log --oneline -5`.
- Commit 2766aeb (GREEN): present in `git log --oneline -5`.
- Manual smoke `collect_drives()` returns `[]` end-to-end on WSL2 dev shell (D-33 path verified live).
- No regressions: 247 cluster_collector tests pass (was 246 + 19 new RED → 246 + 20 new GREEN).
