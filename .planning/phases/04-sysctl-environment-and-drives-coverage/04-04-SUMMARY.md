---
phase: 04-sysctl-environment-and-drives-coverage
plan: 04
subsystem: auto_generator
tags: [transform, fingerprint, splice, D-33, D-34, D-35, COLL-05, COLL-06, COLL-07]
requires:
  - mlpstorage_py/system_description/auto_generator.py — Phase-3 8-tuple _FINGERPRINT_KEYS, _network_signature D-22 template, _resolve_fingerprint_key hardcoded networking dispatch, _splice_stub_lists Phase-2 unconditional drives stub line
  - Plans 04-01 (sysctl collector), 04-02 (environment collector + redactors), 04-03 (drives collector) — the collectors whose output the new fingerprint signatures consume; not strictly required for this plan's correctness (signatures are pure transforms over dict inputs) but required for the end-to-end value Plan 04-05 will close
provides:
  - mlpstorage_py.system_description.auto_generator._sysctl_signature
  - mlpstorage_py.system_description.auto_generator._environment_signature
  - mlpstorage_py.system_description.auto_generator._drive_signature
  - mlpstorage_py.system_description.auto_generator._EXTRACTOR_SOURCE_KEYS (module-level dispatch map)
  - extended _FINGERPRINT_KEYS (8-tuple → 11-tuple per D-34)
  - generalized _resolve_fingerprint_key (name → source-key dispatch via _EXTRACTOR_SOURCE_KEYS)
  - _splice_stub_lists D-33 drives-omit branch (replaces Phase-2 unconditional _DRIVE_STUB splice)
affects:
  - mlpstorage_py/system_description/auto_generator.py (3 signature defs + dispatch map + extended fingerprint keys + generalized dispatch + extended splicer + _DRIVE_STUB Phase-2-legacy comment)
  - tests/unit/test_auto_generator.py (35 new tests across 8 new classes + 5 existing tests updated for the D-33 contract)
  - tests/integration/test_systemname_yaml_end_to_end.py (1 Rule 3 contract-test update for the D-33 drives-omit assertion)
tech-stack:
  added:
    - "_EXTRACTOR_SOURCE_KEYS: first plain-dict dispatch table in auto_generator.py (Phase 3 had only a 1-entry hardcoded item.get('networking', [])). The pattern is now self-documenting: adding a new extractor is a 2-line change (append to _FINGERPRINT_KEYS + add source-key mapping)."
  patterns:
    - D-22 + D-34 callable fingerprint extractors with key=repr mixed-type sort defense (4 extractors now active: networking, sysctl, environment, drives)
    - D-33 conditional splice (drives present → pass-through; empty/missing → key OMITTED), distinct from D-3 Phase-2 fallback-to-blank-stub for networking
    - D-35 strict fingerprint policy locked in tests (TestGroupByFingerprintSplitsOnNewKeys)
    - Phase-2-legacy retention pattern: _DRIVE_STUB constant kept importable for legacy test paths but no longer emitted by the splicer (allows historical / hand-written tests that explicitly construct the Phase-2 stub to still pass)
key-files:
  created:
    - .planning/phases/04-sysctl-environment-and-drives-coverage/04-04-SUMMARY.md
  modified:
    - mlpstorage_py/system_description/auto_generator.py
    - tests/unit/test_auto_generator.py
    - tests/integration/test_systemname_yaml_end_to_end.py
decisions:
  - "Three new signature extractors land verbatim per PATTERNS.md (Plan 03-04 _network_signature template copy-and-rename) — same key=repr defense, same .get(..., '') defaults, same tuple-of-sorted-tuples shape. Empty list inputs return the stable sentinel `tuple()` so hosts that collected zero entries group together rather than crashing the sorted() call."
  - "_EXTRACTOR_SOURCE_KEYS lands as a module-level plain dict (NOT Final[dict]) for Phase parity with _FINGERPRINT_KEYS (which is also plain `tuple` not `Final[tuple]`). The pattern works because the assignments happen once at module load time and no code mutates the constants downstream; the convention is consistent."
  - "_resolve_fingerprint_key signature unchanged ((item, key) -> Any); only the body generalized. This preserves the existing call site in group_by_fingerprint (one line, line 200) — zero touch on the consumer side."
  - "_DRIVE_STUB retained at module scope with a Phase-2-legacy comment. Tests that explicitly construct `dict(_DRIVE_STUB)` (e.g., the legacy import test in TestSpliceStubListsDrivesOmitBranch) still work; only the splicer's unconditional emit is gone. This is a deliberate diff-minimization choice: removing _DRIVE_STUB would require touching the test that imports it, and the constant is genuinely useful as the canonical 'this is what a blank drives stanza looks like' reference even if no production code emits it."
  - "Rule 3 contract update folded into the GREEN commit (same convention as Plan 04-02's _redact migration and Plan 04-03's RM-bool fix): tests/integration/test_systemname_yaml_end_to_end.py::test_validator_errors_only_on_blanks asserted the schema validator surfaces errors under clients[].drives[*] (the Phase-2 stub-blank behavior). Per D-33, the drives key is now OMITTED entirely; `drives` is Optional on NodeDescription, so the validator does NOT surface a drives[*] error path. Test updated to assert (a) drives key absent from the on-disk YAML directly via yaml.safe_load, and (b) no drives error in the validator output."
  - "Five existing unit tests in test_auto_generator.py that locked the Phase-2 unconditional client['drives'] = [dict(_DRIVE_STUB)] splice were updated in the RED commit to assert `\"drives\" not in client` per D-33: test_splice_stub_lists_adds_to_every_client, test_splice_stub_lists_multiple_clients, test_splice_stub_lists_idempotent, test_outer_dict_with_spliced_stubs_yaml_roundtrip, and TestSpliceUpNicTraffic::test_existing_phase2_drives_stub_unchanged (renamed to test_drives_key_omitted_when_no_drives). Folding these into RED makes the GREEN commit purely additive on the production code path."
metrics:
  duration_minutes: ~18
  completed_date: 2026-06-23
  tasks_completed: 2
  files_created: 1
  files_modified: 3
  commits: 2
---

# Phase 04 Plan 04: Transform-layer extensions (3 fingerprint signatures + generalized dispatch + D-33 splice) Summary

Three new fingerprint signature extractors (`_sysctl_signature`, `_environment_signature`, `_drive_signature`), a generalized `_resolve_fingerprint_key` dispatch via the new `_EXTRACTOR_SOURCE_KEYS` map, `_FINGERPRINT_KEYS` extended from the Phase-3 8-tuple to the 11-tuple per D-34, and the `_splice_stub_lists` D-33 conditional drives-omit branch replacing the Phase-2 unconditional `_DRIVE_STUB` splice. Two-commit RED/GREEN cadence; 35 new unit tests + 1 Rule 3 contract update across 1 integration test all green; no regressions in the 1826-passing unit suite.

## What Shipped

**1. Three new D-22/D-34 signature extractors in `mlpstorage_py/system_description/auto_generator.py`** (each follows the Phase-3 `_network_signature` template verbatim — same `key=repr` defense, same `.get(..., '')` defaults, same `tuple(sorted(..., key=repr))` shape):

- `_sysctl_signature(sysctl)` → sorted multiset of `(name, value)` tuples. Mixed-type safety not strictly required (sysctl emits str values per Plan 04-01 / D-29) but `key=repr` applied for shape parity.
- `_environment_signature(environment)` → same shape as sysctl. Per D-34 the signature uses the already-redacted value from Plan 04-02 (D-23 first-4/last-4 mask on `AWS_ACCESS_KEY_ID`, D-24 length-only on `AWS_SECRET_ACCESS_KEY`).
- `_drive_signature(drives)` → sorted multiset of `(vendor_name, model_name, interface, capacity_in_GB, unit_count)` tuples. `key=repr` is LOAD-BEARING here: drives carry int `capacity_in_GB` from `collect_drives()` but the `.get(..., '')` defense produces `''` on missing-key inputs; the Plan 03-04 mixed-type crash reapplies without `key=repr`. A dedicated test `TestDriveSignature::test_mixed_type_sort_safety_per_d22_key_repr` locks this contract.

**2. `_EXTRACTOR_SOURCE_KEYS` module-level dispatch map** (NEW symbol). Maps extractor name → host-data source key:

```python
_EXTRACTOR_SOURCE_KEYS: dict = {
    "networking_sig":  "networking",
    "sysctl_sig":      "sysctl",
    "environment_sig": "environment",
    "drives_sig":      "drives",
}
```

Adding a new extractor in any future phase is now a two-line change: append to `_FINGERPRINT_KEYS` and add the source-key mapping here. Zero touch on `_resolve_fingerprint_key`.

**3. `_FINGERPRINT_KEYS` extended from 8-tuple → 11-tuple** per D-34. The Phase-3 head preserved verbatim; three new `(name, extractor)` tuples appended at the tail in slice-completion order (sysctl, environment, drives):

```python
_FINGERPRINT_KEYS = (
    "chassis.cpu_model",        # Phase 2
    "chassis.cpu_qty",          # Phase 2
    "chassis.cpu_cores",        # Phase 2
    "chassis.memory_capacity",  # Phase 2
    "chassis.model_name",       # Phase 3 / COLL-03
    "operating_system.name",    # Phase 2
    "operating_system.version", # Phase 2
    ("networking_sig", _network_signature),     # Phase 3 / COLL-04 / D-22
    ("sysctl_sig",      _sysctl_signature),      # Phase 4 / COLL-05 / D-34 — NEW
    ("environment_sig", _environment_signature), # Phase 4 / COLL-06 / D-34 — NEW
    ("drives_sig",      _drive_signature),       # Phase 4 / COLL-07 / D-34 — NEW
)
```

**4. Generalized `_resolve_fingerprint_key`**:

```python
def _resolve_fingerprint_key(item: dict, key: Any) -> Any:
    if isinstance(key, tuple):
        name, extractor = key
        return extractor(item.get(_EXTRACTOR_SOURCE_KEYS[name], []))
    return _get_dotted(item, key)
```

Function signature unchanged; only the body generalized. The existing single call site in `group_by_fingerprint` (one line) is untouched — zero ripple on consumers.

**5. `_splice_stub_lists` D-33 conditional drives-omit branch.** The Phase-2 unconditional `client['drives'] = [dict(_DRIVE_STUB)]` line is gone; the new conditional:

```python
existing_drives = client.get("drives")
if existing_drives:
    # Real drives → leave as-is.
    pass
else:
    # D-33: lsblk absent / no devices / all filtered → OMIT key entirely.
    client.pop("drives", None)
```

This is asymmetric with the networking fallback (which still splices `_NETWORKING_STUB` on the empty branch per D-3). Per the explicit user decision in CONTEXT.md / ROADMAP SC #5, an absent `drives:` block IS the intended SER-02 signal (vs. a blank stub) — submitters reading the YAML see "the collector couldn't determine this; hand-fill if applicable" by the absence itself.

**6. `_DRIVE_STUB` Phase-2-legacy comment.** Constant retained at module scope (importable, used in `TestSpliceStubListsDrivesOmitBranch::test_legacy_drive_stub_still_importable` to verify the import surface is intact), but flagged with:

```
# Phase 2 legacy stub. Retained for legacy test paths; the splicer no longer
# emits it (D-33). [...]
```

**7. Tests — 35 new tests across 8 new classes in `tests/unit/test_auto_generator.py`:**

| Class | Tests | Purpose |
|---|---|---|
| `TestSysctlSignature` | 5 | empty→`()`, single entry, order-independence, value-difference splits, missing-keys default |
| `TestEnvironmentSignature` | 5 | mirror of TestSysctlSignature |
| `TestDriveSignature` | 6 | empty, single-entry shape, order-independence, **mixed-type `key=repr` safety (load-bearing)**, unit_count split, capacity split |
| `TestExtractorSourceKeys` | 2 | dict shape; correct name→source-key mapping |
| `TestResolveFingerprintKeyGeneralized` | 6 | scalar dispatch preserved, 4 callable extractor dispatches, missing-source-key default |
| `TestFingerprintKeysExtended` | 3 | length=11; tail three entries; Phase-3 head preserved verbatim |
| `TestGroupByFingerprintSplitsOnNewKeys` | 4 | D-35 strict policy: sysctl/env/drives differences each split; identical collapses |
| `TestSpliceStubListsDrivesOmitBranch` | 4 | drives present → pass; empty/missing → omit; legacy _DRIVE_STUB still importable |

**Five existing tests updated for the D-33 contract** (in the RED commit so the GREEN commit is purely additive on production code):
- `test_splice_stub_lists_adds_to_every_client` — now asserts `"drives" not in client`
- `test_splice_stub_lists_multiple_clients` — same
- `test_splice_stub_lists_idempotent` — same
- `test_outer_dict_with_spliced_stubs_yaml_roundtrip` — same
- `TestSpliceUpNicTraffic::test_existing_phase2_drives_stub_unchanged` renamed → `test_drives_key_omitted_when_no_drives`

**One existing integration test updated for the D-33 contract** (Rule 3 — see Deviations below):
- `tests/integration/test_systemname_yaml_end_to_end.py::test_validator_errors_only_on_blanks` — drives error assertion replaced with on-disk YAML inspection (drives key must be absent) + negative assertion (no drives error in validator output).

## Two-Commit RED/GREEN Cadence

| Commit | Type | Files | Purpose |
|---|---|---|---|
| `dac14c4` | test(04-04) | tests/unit/test_auto_generator.py | RED — 35 new tests across 8 new classes + 5 existing test updates for D-33. ImportError on `_EXTRACTOR_SOURCE_KEYS`, `_sysctl_signature`, `_environment_signature`, `_drive_signature` at collection time means the WHOLE module fails to collect → all 75 test_auto_generator.py tests are RED until GREEN ships the symbols. |
| `a6dff13` | feat(04-04) | mlpstorage_py/system_description/auto_generator.py, tests/integration/test_systemname_yaml_end_to_end.py | GREEN — 3 signatures + dispatch map + extended fingerprint keys + generalized dispatch + D-33 splice + _DRIVE_STUB Phase-2-legacy comment + 1 Rule 3 contract update on the integration test (drives-error assertion → drives-key-absent assertion). |

## Mixed-Type `key=repr` Safety in Practice for GREEN

The Plan 03-04 surprise (mixed-type sort crash without `key=repr`) re-surfaced as **expected** for `_drive_signature` but did NOT manifest during the GREEN run for `_sysctl_signature` or `_environment_signature`.

**Why drives is load-bearing:** Real drives data from `collect_drives()` carries `int capacity_in_GB` and `int unit_count`, but the `.get(..., '')` defense produces `str ''` on inputs that came from a pre-grouped or partial-collection host. The `int 500` vs `str ''` collision crashes `sorted()` with `TypeError: '<' not supported between instances of 'str' and 'int'` without `key=repr`. The dedicated test `TestDriveSignature::test_mixed_type_sort_safety_per_d22_key_repr` constructs exactly this mixed-input scenario and asserts the call succeeds; it would fail with the verbatim Plan-03-04 sort form if `key=repr` were removed.

**Why sysctl/environment don't manifest the crash in practice:** Both fields emit `str` values uniformly on the happy path. Plan 04-01 collects sysctl values via `f.read(8192).rstrip('\n')` (str); Plan 04-02 collects environment via `os.environ` (str) and the redactors return str. The `.get(..., '')` default produces str, which sorts cleanly against other str. **However, `key=repr` is still applied uniformly for shape parity with `_network_signature` and `_drive_signature`** — this is the cheap-and-safe convention, and any future schema extension that introduces non-str values (e.g., adding a numeric "size in bytes" field to sysctl entries) would benefit from the defense in place rather than re-discovering it later.

**No GREEN-time surprise this plan.** Unlike Plan 03-04 where `key=repr` was added during GREEN as a Rule 1 fix, this plan shipped `key=repr` from the first GREEN diff per PATTERNS.md guidance. The PLAN.md text literally inlined the verbatim form including `key=repr`.

## Pre-existing Phase 2/3 Tests Needed Updating

**Five unit tests + one integration test.** All updates are direct consequences of the D-33 contract change this plan ships (the drives key is now conditionally omitted instead of always-spliced-as-blank-stub). The unit-test updates were folded into the RED commit `dac14c4` so the GREEN commit is purely additive on production code; the integration-test update was folded into the GREEN commit `a6dff13` per the Plan 04-02 convention (Rule 3 contract-test updates ride with the GREEN that causes the contract change).

| Test (file) | Pre-D-33 assertion | Post-D-33 assertion |
|---|---|---|
| `test_splice_stub_lists_adds_to_every_client` | `client["drives"] == [_DRIVE_STUB]` | `"drives" not in client` |
| `test_splice_stub_lists_multiple_clients` | same | same |
| `test_splice_stub_lists_idempotent` | same | same |
| `test_outer_dict_with_spliced_stubs_yaml_roundtrip` | YAML asserts drives stub dict block | YAML asserts `"drives" not in client` |
| `TestSpliceUpNicTraffic::test_existing_phase2_drives_stub_unchanged` (renamed → `test_drives_key_omitted_when_no_drives`) | same | same |
| `tests/integration/...::test_validator_errors_only_on_blanks` | `any("drives" in p for p in error_paths)` | `not any("drives" in p for p in error_paths)` + on-disk YAML assertion |

Lesson: when a contract change converts "always present" to "conditionally present", grep the test surface for hard-coded presence assertions BEFORE writing GREEN. PATTERNS.md flagged the splicer change as test-impacting; the planner's `<read_first>` section explicitly called out `test_outer_dict_with_spliced_stubs_yaml_roundtrip` and the splice-list tests as the impact zone, and the RED commit absorbed them cleanly.

## `_DRIVE_STUB` Phase-2-legacy Retention

The `_DRIVE_STUB` constant is retained at module scope with a 6-line Phase-2-legacy comment:

```python
# Phase 2 legacy stub. Retained for legacy test paths; the splicer no longer
# emits it (D-33). Phase 4 / Plan 04-04 replaced the unconditional
# `client['drives'] = [dict(_DRIVE_STUB)]` line in `_splice_stub_lists`
# with a conditional that OMITS the drives key entirely when no drives
# were collected. The constant stays importable so any downstream consumer
# (or test) that wants to construct the historical blank drives stanza can
# still do so explicitly via `dict(_DRIVE_STUB)`.
```

Justification: removing the constant would require touching the import in `tests/unit/test_auto_generator.py` (and the `test_drive_stub_shape`, `test_stub_keys_match_pydantic_fields`, `test_stub_constants_are_module_level_not_mutated_by_callers` tests that exercise it). The constant is genuinely useful as the canonical reference shape for "this is what a blank drives stanza looks like, modulo the omitted `performance` field." Keeping it is the diff-minimization choice and aligns with the PLAN.md `<must_haves>` truth #6: "_DRIVE_STUB module-level constant is retained for legacy test paths but a comment marks it as Phase-2-legacy".

The new `TestSpliceStubListsDrivesOmitBranch::test_legacy_drive_stub_still_importable` asserts the constant is still a dict with the expected shape AND that no post-splice client dict contains the stub — locking both the import contract AND the new-emit contract in one test.

## `len(_FINGERPRINT_KEYS) == 11` Confirmation

```bash
python3 -c "from mlpstorage_py.system_description.auto_generator import _FINGERPRINT_KEYS, _EXTRACTOR_SOURCE_KEYS; print(len(_FINGERPRINT_KEYS), sorted(_EXTRACTOR_SOURCE_KEYS.keys()))"
# → 11 ['drives_sig', 'environment_sig', 'networking_sig', 'sysctl_sig']
```

All four callable extractor source keys present in `_EXTRACTOR_SOURCE_KEYS`; `_FINGERPRINT_KEYS` is the 11-tuple per D-34. Plan-level `<verification>` block satisfied verbatim.

## Verification

```bash
# All Phase 4 / Plan 04-04 new test classes pass (35/35).
python3 -m pytest tests/unit/test_auto_generator.py::TestSysctlSignature \
    tests/unit/test_auto_generator.py::TestEnvironmentSignature \
    tests/unit/test_auto_generator.py::TestDriveSignature \
    tests/unit/test_auto_generator.py::TestExtractorSourceKeys \
    tests/unit/test_auto_generator.py::TestResolveFingerprintKeyGeneralized \
    tests/unit/test_auto_generator.py::TestFingerprintKeysExtended \
    tests/unit/test_auto_generator.py::TestGroupByFingerprintSplitsOnNewKeys \
    tests/unit/test_auto_generator.py::TestSpliceStubListsDrivesOmitBranch -q
# → 35 passed

# Full auto_generator unit suite (no regressions in Phase 2/3 tests).
python3 -m pytest tests/unit/test_auto_generator.py -q
# → 102 passed (67 prior + 35 new)

# Systemname.yaml integration suite (no regressions; 1 Rule 3 contract update inside).
python3 -m pytest tests/integration/test_systemname_yaml_end_to_end.py -q
# → 21 passed

# Full unit suite excluding pre-existing collection errors.
python3 -m pytest tests/unit -q \
    --ignore=tests/unit/test_benchmarks_base.py \
    --ignore=tests/unit/test_parquet_reader.py \
    --ignore=tests/unit/test_vdb_modular_fake_backend.py
# → 1826 passed, 7 failed (all 7 pre-existing _check_safe_path_component
#   MagicMock failures noted in STATE.md Deferred Items + 04-01/04-02/04-03 SUMMARYs)

# Acceptance criteria grep gates.
grep -c '^_EXTRACTOR_SOURCE_KEYS' mlpstorage_py/system_description/auto_generator.py
# → 1
grep -c 'def _drive_signature\|def _sysctl_signature\|def _environment_signature' mlpstorage_py/system_description/auto_generator.py
# → 3
grep -v '^[[:space:]]*#' mlpstorage_py/system_description/auto_generator.py | grep -c 'client.pop("drives"'
# → 1
python3 -c "from mlpstorage_py.system_description.auto_generator import _FINGERPRINT_KEYS, _EXTRACTOR_SOURCE_KEYS; print(len(_FINGERPRINT_KEYS), sorted(_EXTRACTOR_SOURCE_KEYS.keys()))"
# → 11 ['drives_sig', 'environment_sig', 'networking_sig', 'sysctl_sig']
```

All plan-level `<verification>` items and `<success_criteria>` satisfied:
1. Three new signature functions with D-22 `key=repr` defense ✓ (D-34)
2. `_FINGERPRINT_KEYS` is the 11-tuple ✓ (D-34)
3. `_resolve_fingerprint_key` generalized via `_EXTRACTOR_SOURCE_KEYS` ✓
4. Two hosts that differ on sysctl, environment, or drives split into separate stanzas ✓ (D-35; TestGroupByFingerprintSplitsOnNewKeys)
5. `_splice_stub_lists` omits `drives` key when collection produced no entries ✓ (D-33; TestSpliceStubListsDrivesOmitBranch); `_DRIVE_STUB` remains importable ✓
6. All Phase 2 + Phase 3 tests still pass ✓ (regression-free on cross-host grouping and stub splicing; 5 unit-test updates + 1 integration-test update are direct D-33 contract updates, not regressions)

## Deviations from Plan

### Rule 3 — Integration-test contract update (auto-fix blocking issue)

**Found during:** Task 2 GREEN broader-suite verification (`pytest tests/integration/test_systemname_yaml_end_to_end.py`).

**Issue:** `tests/integration/test_systemname_yaml_end_to_end.py::test_validator_errors_only_on_blanks` asserted that `schema_validator.validate_file()` surfaces at least one error path under `clients[].drives[*]` (the Phase-2 stub-blank behavior). Per D-33, this plan OMITS the `drives` key entirely from the YAML when no drives are collected; `drives` is `Optional` on `NodeDescription`, so the Pydantic validator does NOT surface an error path under drives — that IS the intended SER-02 signal (a missing block is the cue for the submitter, not a populated-but-blank block).

**Risk:** Test fails on every Phase-4 run going forward. Not a production-behavior bug — the production contract is exactly what D-33 prescribes — but a test contract that no longer matches the production contract. Plan-acceptance-block-level Rule 3 (blocking task completion, structurally required to close the GREEN gate).

**Fix:** Replace the drives-error-path assertion with two assertions that lock the new D-33 contract end-to-end:
1. Direct YAML inspection: `yaml.safe_load(target)["system_under_test"]["clients"][0]` MUST NOT contain a `drives` key.
2. Negative validator assertion: the error paths returned by `schema_validator.validate_file()` MUST NOT contain `"drives"`.

Folded into the GREEN commit `a6dff13` (same convention as Plan 04-02's Rule 3 contract-test updates for the unified-redactor change and Plan 04-03's Rule 1 RM-bool fix for the boolean variant). The PLAN.md `<acceptance_criteria>` for Task 1 explicitly allowed for and required the test-impact scan; this integration test simply wasn't on the planner's radar because the PLAN's test-impact list was scoped to `tests/unit/test_auto_generator.py`. The principle holds: any test that asserts on the post-splice client dict shape is subject to D-33 contract impact, including in integration files.

**Why Rule 3 (not Rule 1 or 4):** This is a blocking issue caused directly by the D-33 contract change this plan ships. The production code is correct; the test simply needs to assert on the new contract instead of the old one. Not a bug in production code (Rule 1), not a missing critical feature (Rule 2), not an architectural change (Rule 4).

**Files modified:** tests/integration/test_systemname_yaml_end_to_end.py (one assertion block replaced; 7 lines updated to 18 lines).
**Commit:** `a6dff13` (GREEN).

No Rule 1 / Rule 2 / Rule 4 deviations triggered.

## Authentication Gates

None.

## Threat Flags

None — this plan is pure-transform code (zero I/O, zero network surface, zero subprocess, zero file access). The new signature functions are pure functions over Python dicts; the `_resolve_fingerprint_key` generalization is a dict lookup; the `_splice_stub_lists` extension is a `dict.pop` call. No new threat surface beyond what Phase 2's `auto_generator.py` already shipped.

## Known Stubs

None — all production code paths flow data end-to-end. The Phase-2-legacy `_DRIVE_STUB` constant is retained for the legacy-test-import contract but is NOT emitted into any YAML output. The `_splice_stub_lists` D-33 branch is the load-bearing change: from "always blank drives stub" to "drives key absent when no drives collected".

Note for the Phase 4 verifier: the end-to-end value of this plan is only realized when Plan 04-05 lands — `HostInfo` doesn't yet have `sysctl`, `environment`, `drives` fields, and `node_dict_from_host` doesn't yet emit them. So even though the transform layer is wired correctly, a current `mlpstorage run` invocation will still produce YAML where the new signatures all match `()` for all hosts (every host's `item.get('sysctl', [])` is `[]` because `node_dict_from_host` doesn't emit a `sysctl` key yet) — meaning fleets DO still collapse on the new dimensions, just trivially. Plan 04-05 closes this end-to-end.

## Forward Notes for Plan 04-05

Plan 04-05 ships the final integration layer:

- `HostInfo.sysctl`, `HostInfo.environment`, `HostInfo.drives` fields appended to the dataclass (D-16 num_sockets precedent; mirrors Phase 3's `chassis_model` + `networking` extensions).
- `HostInfo.from_collected_data` reads `data.get('sysctl', [])`, `data.get('environment', [])`, `data.get('drives', [])` from the collector output dict.
- `node_dict_from_host` extended with three new emit keys:
  - `"sysctl": list(host.sysctl)` — pass-through copy, NOT per-host grouped (each sysctl key appears once per host).
  - `"environment": list(host.environment)` — pass-through copy, same reasoning.
  - `"drives": per_host_drives` — per-host `group_by_fingerprint` pass over `("vendor_name", "model_name", "interface", "capacity_in_GB")` with `"unit_count"`, collapsing identical drive rows on a single host into stanzas. Mirrors the Phase 3 per-host networking grouping pattern.
- End-to-end integration tests covering all three Phase 4 collectors' data flowing through to the emitted YAML.

The new D-33 splice branch in `_splice_stub_lists` will fire correctly once `node_dict_from_host` emits the `drives` key: a host with `collect_drives() → []` (lsblk absent / no devices / all filtered, e.g., the WSL2 dev shell where every TRAN is null) yields `node_dict_from_host(host)["drives"] = []` → `_splice_stub_lists` then pops the key → the emitted YAML carries no `drives:` block per ROADMAP SC #5.

ROADMAP SC #2 hygiene work is already complete (landed in Plan 04-02's GREEN commit). No additional roadmap reconciliation needed in 04-05.

## Self-Check: PASSED

- mlpstorage_py/system_description/auto_generator.py: `_sysctl_signature` defined: FOUND
- mlpstorage_py/system_description/auto_generator.py: `_environment_signature` defined: FOUND
- mlpstorage_py/system_description/auto_generator.py: `_drive_signature` defined: FOUND
- mlpstorage_py/system_description/auto_generator.py: `_EXTRACTOR_SOURCE_KEYS` defined at module level: FOUND
- mlpstorage_py/system_description/auto_generator.py: `_FINGERPRINT_KEYS` length is 11: VERIFIED (`python3 -c "from mlpstorage_py.system_description.auto_generator import _FINGERPRINT_KEYS; print(len(_FINGERPRINT_KEYS))"` → 11)
- mlpstorage_py/system_description/auto_generator.py: `_resolve_fingerprint_key` uses `_EXTRACTOR_SOURCE_KEYS[name]`: FOUND
- mlpstorage_py/system_description/auto_generator.py: D-33 `client.pop("drives", None)` branch present: FOUND (grep returns 1 outside comments)
- mlpstorage_py/system_description/auto_generator.py: `_DRIVE_STUB` constant retained with Phase-2-legacy comment: FOUND
- tests/unit/test_auto_generator.py: 8 new test classes (35 new tests) GREEN: VERIFIED
- tests/unit/test_auto_generator.py: 5 existing tests updated for D-33 contract: VERIFIED (passing)
- tests/integration/test_systemname_yaml_end_to_end.py: Rule 3 contract update GREEN: VERIFIED (21/21 passing)
- Commit `dac14c4` (RED): present in `git log --oneline -5`
- Commit `a6dff13` (GREEN): present in `git log --oneline -5`
- Full unit suite: 1826 passed, 7 pre-existing failures (out-of-scope per Rule 3 scope boundary); no new regressions.
