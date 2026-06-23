"""Pure-transformation core for systemname.yaml auto-generation.

This module is the in-memory transformation layer between the existing MPI
cluster collector (which produces `HostInfo` instances) and the eventual
on-disk `systemname.yaml` write step (Plan 02-04). It contains no I/O — every
function here is a pure transformation over Python dicts.

Phase 02 / Plan 02-02 deliverables (Slice 2 of the auto-collector vertical):

- `group_by_fingerprint(items, fingerprint_keys, count_field)` — generic
  quantity-grouping helper per CONTEXT.md D-4. Empty strings participate in
  the fingerprint as-is per D-5 (determinism over flattering output): failed-
  collection hosts group together as their own stanza instead of being hidden.
  Sorting (D-7) is the caller's responsibility — see Plan 02-04.

- `_get_dotted(d, dotted_key)` — internal dotted-key resolver. Missing keys
  return the empty string per D-5 so fingerprint determinism holds even on
  hosts where the collector returned partial data.

- `_FINGERPRINT_KEYS` — the six-key tuple per D-4 that defines what makes two
  hosts "the same" for quantity-grouping purposes.

Phase 02 / Plan 02-03 deliverables (Slice 3 — schema-aware blanks scaffolding):

- `_NETWORKING_STUB`, `_DRIVE_STUB` — `Final[dict]` stub literals carrying the
  empty-string / empty-list values the YAML emit needs for the `networking[]`
  and `drives[]` sublists. D-3 (CONTEXT.md): these intentionally bypass the
  StrictModel Pydantic classes since empty strings fail enum / `min_length=1`
  validation but ARE the desired emit shape (SER-02 visible to-do reminders).
  Field-name parity with `NetworkPort.model_fields` /
  `DriveInstance.model_fields` is asserted at TEST time
  (`test_stub_keys_match_pydantic_fields`) so any schema change forces the
  stubs to be updated in lockstep.

- `_splice_stub_lists(dump)` — post-dump mutator that injects exactly one
  stub `networking` entry and one stub `drives` entry into every client.
  Idempotent (re-runs REPLACE, do not append). Defensive on shape: missing
  `system_under_test` or `clients` returns the dump unchanged.

- `_build_outer_dict(stanzas)` — `{system_under_test: {clients: [...]}}`
  scaffolding. Per D-14, the top-level `solution`, `deployment`,
  `product_nodes`, `product_switches`, `total_rack_units`, and
  `rack_power_supplies` blocks are OMITTED — the auto-collector cannot supply
  legal enum values for `solution.architecture.storage_location` (Pitfall 1),
  so `schema_validator.validate_file()` will fail on the missing required
  fields, which IS the intended "submitter has work to do" UX (SER-02).

Symbols arriving in later slices of Phase 02 (NOT in this module yet):

- `write_systemname_yaml`, `_resolve_host_info_list`, atomic write,
  FileExistsError no-op → Plan 02-04.

Pitfall 2 lock: this module does NOT construct any leaf Pydantic instance
(Chassis / OperatingSystem / NodeDescription). Those models enforce
`min_length=1` on the very fields the universal collection-failure rule
demands we emit as empty strings; constructing them here would crash on any
partial-collection host. The Pydantic models live in `schema_validator.py`
and are exercised only at validation time and at test time (via
`.model_fields.keys()` reflection for schema-drift detection).
"""

import copy
import os
from pathlib import Path
from typing import Any, Final, Optional

import yaml

from mlpstorage_py.cluster_collector import collect_local_system_info
from mlpstorage_py.rules.models import HostInfo


def _network_signature(networking: list) -> tuple:
    """D-22 cross-host networking extractor: order-independent multiset of
    (type, speed, state, unit_count) tuples.

    Uses .get(..., '') for every key so per-host pre-grouped (no unit_count
    yet) and post-grouped (with unit_count) entries both work, and so down
    NICs (no speed key per Plan 03-03's emit shape) participate as
    ('ethernet', '', 'down', N) rather than crashing with KeyError.

    Two hosts that enumerated identical NICs in different listdir order
    produce equal signatures because the per-entry tuples are sorted before
    being wrapped into the outer tuple (which is the hashable value
    group_by_fingerprint hashes alongside the scalar dotted keys).

    The sort uses a `repr`-based key because mixed-type fields would
    otherwise raise `TypeError: '<' not supported between instances of
    'str' and 'int'` when an up entry's `speed=100` (int) collides with a
    down entry's `speed=""` (the .get default for the missing key). The
    repr-based key is deterministic; the resulting tuple values themselves
    keep their native types so equal multisets still hash to equal sigs.
    """
    return tuple(sorted(
        (
            (e.get("type", ""), e.get("speed", ""), e.get("state", ""), e.get("unit_count", ""))
            for e in networking
        ),
        key=repr,
    ))


# ---------------------------------------------------------------------------
# D-4 + D-22: locked fingerprint key set for quantity-grouping homogeneous
# client stanzas. Each entry is either:
#   - a dotted-path string (resolved via _get_dotted), OR
#   - a (name, callable_extractor) tuple (D-22) where the extractor is
#     invoked on item.get('networking', []) at dispatch time.
# The string-only Phase 2 form is preserved at the head of the tuple;
# chassis.model_name + the ('networking_sig', _network_signature) callable
# join in Phase 3 per D-22. Order matters only for human readability of the
# resulting fp tuples; group_by_fingerprint hashes them as a tuple so any
# consistent order works.
# ---------------------------------------------------------------------------
_FINGERPRINT_KEYS: tuple = (
    "chassis.cpu_model",
    "chassis.cpu_qty",
    "chassis.cpu_cores",
    "chassis.memory_capacity",
    "chassis.model_name",                        # NEW (Phase 3 / COLL-03)
    "operating_system.name",
    "operating_system.version",
    ("networking_sig", _network_signature),      # NEW (Phase 3 / COLL-04; D-22 callable extractor)
)


def _get_dotted(d: dict, dotted_key: str) -> Any:
    """Walk a dotted path through nested dicts.

    Examples:
        _get_dotted({"a": {"b": {"c": 42}}}, "a.b.c") == 42
        _get_dotted({"a": {}}, "a.b.c") == ""           # missing nested key
        _get_dotted({}, "x") == ""                       # missing top-level
        _get_dotted({"a": "leaf"}, "a") == "leaf"        # single segment
        _get_dotted({"a": "leaf"}, "a.b") == ""          # intermediate not a dict

    A miss at any depth returns the empty string. Per D-5, this is intentional:
    empty-string-on-miss makes the fingerprint deterministic even on hosts
    where the collector returned partial data — failed-collection hosts group
    together as their own stanza rather than crashing the grouping pass.
    """
    cur: Any = d
    for part in dotted_key.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return ""
    return cur


def _resolve_fingerprint_key(item: dict, key: Any) -> Any:
    """D-22 dispatch: scalar dotted-string keys resolve via _get_dotted;
    (name, extractor) callable tuples invoke extractor(item.get('networking', []))."""
    if isinstance(key, tuple):
        _name, extractor = key
        return extractor(item.get("networking", []))
    return _get_dotted(item, key)


def group_by_fingerprint(
    items: list[dict],
    fingerprint_keys: tuple[str, ...],
    count_field: str,
) -> list[dict]:
    """Collapse items sharing all fingerprint_keys into one entry annotated
    with count_field: N.

    Properties:

    - Order: first-occurrence (deterministic on the input order). Sorting is
      D-7 territory and lives in `write_systemname_yaml` (Plan 02-04), not
      here — keeping this helper concerns-separated.
    - Empty strings participate in the fingerprint as-is per D-5. A host whose
      cpu_model collection failed (`""`) groups with other `""` hosts; it does
      NOT mysteriously absorb into a real-CPU stanza.
    - The input list and its dicts are NOT mutated: each accepted item is
      deep-copied before being annotated with the count field. Callers can
      safely pass the same list to repeated calls.
    - Keys are resolved by `_resolve_fingerprint_key` per D-22: scalar dotted
      strings (`chassis.cpu_model`) go through `_get_dotted` (missing keys →
      `""`, preserving determinism for partial collections); `(name, extractor)`
      callable tuples invoke the extractor on `item.get('networking', [])`
      (used by `('networking_sig', _network_signature)` for the cross-host
      networking multiset).

    Args:
        items: list of dicts (e.g. node_description-shaped dicts from
            `node_dict_from_host`).
        fingerprint_keys: tuple of dotted keys defining the equivalence class.
            For Phase 2 host grouping, pass `_FINGERPRINT_KEYS`.
        count_field: name of the integer count field to inject on each
            returned stanza (e.g. `"quantity"` matching the schema).

    Returns:
        A new list of dicts, one per fingerprint equivalence class, each
        carrying `count_field=N`. Empty input → empty output.
    """
    groups: dict[tuple, dict] = {}
    for item in items:
        fp = tuple(_resolve_fingerprint_key(item, k) for k in fingerprint_keys)
        if fp not in groups:
            # Deep copy preserves the no-mutation invariant: callers' items
            # stay clean and re-grouping (e.g. in tests) is idempotent.
            groups[fp] = {**copy.deepcopy(item), count_field: 1}
        else:
            groups[fp][count_field] += 1
    return list(groups.values())


def node_dict_from_host(host: HostInfo) -> dict:
    """Map a `HostInfo` into a `NodeDescription`-shaped dict.

    Output shape (Phase 3 / Plan 03-05 deliverable — `drives` is spliced by
    Plan 02-03's `_splice_stub_lists`; `quantity` is injected by
    `group_by_fingerprint`; `networking` is now emitted directly here from
    `host.networking` via a per-host `group_by_fingerprint` pass):

        {
          "friendly_description": "",
          "chassis": {
            "model_name": <str | "">,    # COLL-03 from host.chassis_model
            "cpu_model": <str | "">,     # COLL-01
            "cpu_qty": <int | "">,       # COLL-01 via host.cpu.num_sockets (D-16)
            "cpu_cores": <int | "">,     # COLL-01 via host.cpu.num_cores
            "memory_capacity": <int | "">,  # COLL-01, GiB (D-6)
          },
          "operating_system": {
            "name": <str | "">,          # COLL-02 via os_release NAME
            "version": <str | "">,       # COLL-02 via os_release VERSION_ID
          },
          "networking": [<grouped stanzas> | []],  # COLL-04: per-host
              # group_by_fingerprint(host.networking, ("type","speed","state"),
              # "unit_count") collapses identical NICs into stanzas with
              # unit_count=N. Empty host.networking → []; downstream
              # `_splice_stub_lists` then either splices D-17 traffic=[] onto
              # up entries (real-data branch) or falls back to the Phase 2
              # _NETWORKING_STUB blank entry (fallback branch).
        }

    Defensive on every field per the universal collection-failure rule
    (CONTEXT.md D-2 / Pitfall 9): if any source is missing, blank or zero,
    that single field becomes `""` — the function never raises. Pattern F
    `(host.chassis_model or "")` coerces None/missing falsy values to the
    blank-string emit so downstream YAML serialization sees a deterministic
    str regardless of dataclass default vs. malicious None.

    Memory rounding (D-6): `host.memory.total` is bytes (see
    `HostMemoryInfo.from_proc_meminfo_dict` which converts kB → bytes at
    `rules/models.py:117`). Dividing by `1024**3` yields binary GiB; Python's
    default round-half-to-even is acceptable per D-6 since real RAM sizes
    don't typically land on a half-GiB boundary.

    OS field mapping (COLL-02 / Pitfall 4): we select **only** the `NAME` and
    `VERSION_ID` keys from `/etc/os-release` — never `PRETTY_NAME`, `ID`,
    `VERSION`, or `VERSION_CODENAME`. The `.get(k, "") or ""` idiom collapses
    both missing keys and explicit `None` values to the empty string.

    Pitfall 2: this function deliberately does NOT construct any leaf Pydantic
    instance. The `Chassis` and `OperatingSystem` models enforce
    `min_length=1` on the very fields the universal collection-failure rule
    demands we emit as empty strings — constructing them here would crash on
    any partial-collection host. The dict shape is verified against the
    Pydantic schemas at TEST time via `.model_fields.keys()` reflection
    (`test_node_dict_field_names_match_pydantic_reflection`).
    """
    # COLL-01: CPU fields. Truthy guards preserve the 0 → "" mapping for the
    # `num_sockets == 0` case where summarize_cpuinfo couldn't determine the
    # socket count and emitted the dataclass default rather than a real value.
    cpu_model = host.cpu.model if (host.cpu and host.cpu.model) else ""
    cpu_qty = host.cpu.num_sockets if (host.cpu and host.cpu.num_sockets) else ""
    cpu_cores = host.cpu.num_cores if (host.cpu and host.cpu.num_cores) else ""

    # D-6: memory_capacity in binary GiB. host.memory.total is bytes; dividing
    # by 1024**3 yields GiB; round() is round-half-to-even (Python default).
    if host.memory and host.memory.total:
        memory_capacity: Any = round(host.memory.total / (1024 ** 3))
    else:
        memory_capacity = ""

    # COLL-02 / Pitfall 4: NAME → name, VERSION_ID → version, only.
    # Pitfall 9: `.get(k, "") or ""` collapses missing-key and explicit-None.
    os_name = ""
    os_version = ""
    if host.system and host.system.os_release:
        os_name = host.system.os_release.get("NAME", "") or ""
        os_version = host.system.os_release.get("VERSION_ID", "") or ""

    # COLL-04 (Plan 03-05): per-host networking grouping. group_by_fingerprint
    # over (type, speed, state) collapses identical NICs into stanzas with
    # unit_count=N (e.g. two up 100GbE entries → one stanza with unit_count=2).
    # Empty host.networking → []; downstream _splice_stub_lists then falls
    # back to the _NETWORKING_STUB blank entry (D-3 universal-rule symmetry)
    # so the YAML still surfaces the SER-02 collector-blind UX on hosts where
    # networking collection failed entirely. Up entries on the real-data
    # branch receive the D-17 `traffic: []` splice at _splice_stub_lists time.
    if host.networking:
        per_host_networking = group_by_fingerprint(
            host.networking,
            ("type", "speed", "state"),
            "unit_count",
        )
    else:
        per_host_networking = []

    return {
        "friendly_description": "",  # SER-02 blank — human declaration
        "chassis": {
            "model_name": (host.chassis_model or ""),  # Phase 3 / COLL-03 — DMI placeholder normalization in collector; Pattern F defensive blank-on-falsy
            # rack_units OMITTED per D-2 row 4 (optional + non-derivable)
            "cpu_model": cpu_model,
            "cpu_qty": cpu_qty,
            "cpu_cores": cpu_cores,
            "memory_capacity": memory_capacity,
            # power OMITTED per D-2 row 4 (optional + non-derivable)
        },
        "networking": per_host_networking,  # Phase 3 / COLL-04 — directly emitted; drives still spliced by Plan 02-03's _splice_stub_lists
        "operating_system": {
            "name": os_name,
            "version": os_version,
        },
        # drives: spliced by Plan 02-03's _splice_stub_lists
        # environment / sysctl: Phase 4 territory — not emitted here
        # quantity: injected by group_by_fingerprint downstream
    }


# ---------------------------------------------------------------------------
# Plan 02-03 — D-3 stub literals for the networking[] and drives[] sublists.
#
# These intentionally bypass the NetworkPort / DriveInstance StrictModel
# constructors: those models enforce enum membership and `min_length=1`
# constraints which fail on empty strings, but empty-string stubs ARE the
# desired emit shape — the schema_validator failures at submission time are
# the SER-02 "submitter has work to do" UX, not a bug.
#
# Field-name parity with NetworkPort.model_fields / DriveInstance.model_fields
# is asserted at TEST time by
# tests/unit/test_auto_generator.py::test_stub_keys_match_pydantic_fields.
# Any future schema change failing that test forces the stub to be updated.
# ---------------------------------------------------------------------------
_NETWORKING_STUB: Final[dict] = {
    "unit_count": "",   # NetworkPort.unit_count: int(ge=1) — '' fails validation
    "type":       "",   # NetworkPort.type: NetworkType enum — '' fails enum check
    "state":      "",   # NEW (Phase 3 D-20): parity with NetworkPort.model_fields; D-3 option (a) — '' means "collector blind", distinct from real "down"
    "speed":      "",   # NetworkPort.speed: Optional[int](ge=1) — '' fails int coercion
    "traffic":    [],   # NetworkPort.traffic: Optional[List[TrafficType]] — [] rejected by _require_speed_and_traffic_when_up when state would be "up"
}

_DRIVE_STUB: Final[dict] = {
    "unit_count":     "",   # DriveInstance.unit_count: int(ge=1)
    "vendor_name":    "",
    "model_name":     "",
    "interface":      "",   # DriveInstance.interface: DriveInterface enum
    "media_type":     "",   # DriveInstance.media_type: DriveMediaType enum
    "capacity_in_GB": "",   # DriveInstance.capacity_in_GB: int(ge=1)
    # performance: OMITTED per D-2 row 4 — optional + non-derivable spec-sheet fact
}


def _splice_stub_lists(dump: dict) -> dict:
    """Splice stub networking[] and drives[] entries into every client.

    D-3 (CONTEXT.md): stub entries intentionally bypass the StrictModel
    Pydantic classes — empty-string fields fail enum / min=1 / list-non-empty
    checks at construction time but ARE the desired emit shape (visible
    to-do reminders for the submitter; SER-02). Each stub is a fresh dict
    (`dict(_NETWORKING_STUB)`) so callers can safely mutate without
    aliasing the module-level constant.

    D-17 (Phase 3): when the client already has real networking entries
    (provided by Plan 03-05's `node_dict_from_host` wiring), the helper
    iterates the entries and sets `entry['traffic'] = []` on every entry
    whose `state == 'up'`. This is the post-Pydantic splice seam: the
    schema validator at submission time then surfaces the empty-list
    `traffic` field as the SER-02 "submitter must fill the traffic role"
    UX. `NetworkPort` is never constructed with `traffic=[]` (the
    `_require_speed_and_traffic_when_up` validator would crash); the
    splice mutates the dumped dict directly.

    Down entries are NOT mutated — Plan 03-03's emit shape for down NICs
    omits both `speed` and `traffic` keys, and `model_dump(exclude_none=True)`
    drops the optional fields on the Pydantic round-trip, so down entries
    serialize cleanly without a splice.

    When the client has NO networking entries (or an empty list), the
    helper falls back to the Phase 2 behavior: splice in a single
    `[dict(_NETWORKING_STUB)]`. This is the "we collected nothing" path —
    the universal-rule blank stub.

    Idempotent: re-running on the same dict REPLACES the spliced entries
    in the fallback branch and re-sets `traffic = []` on up entries in
    the real-data branch (no append). Plan 02-04 relies on this so callers
    can chain `_splice_stub_lists(_build_outer_dict(stanzas))` even after
    re-grouping.

    Defensive on shape: if `dump` has no `system_under_test` or no
    `clients`, the function returns the dump unchanged (no `KeyError`).

    Returns the input dict (mutated in place). Callers can write
    `dump = _splice_stub_lists(dump)` for clarity even though the return
    value is identity.
    """
    clients = dump.get("system_under_test", {}).get("clients", [])
    for client in clients:
        existing_net = client.get("networking")
        if existing_net:
            # D-17: real networking from node_dict_from_host (Plan 03-05).
            # Splice traffic=[] on every up entry so the resulting YAML
            # fails schema_validator.validate_file on the visible blank —
            # that IS the SER-02 "submitter must fill the traffic role" UX.
            for entry in existing_net:
                if entry.get("state") == "up":
                    entry["traffic"] = []
        else:
            # Phase 2 fallback: no real networking collected → emit the
            # blank stub so schema validation surfaces the universal-rule
            # blanks.
            client["networking"] = [dict(_NETWORKING_STUB)]
        client["drives"] = [dict(_DRIVE_STUB)]
    return dump


def _build_outer_dict(stanzas: list[dict]) -> dict:
    """Construct {system_under_test: {clients: [...]}} per D-14.

    Top-level optional and required blocks are OMITTED:
      - solution         (required at schema level; submitter fills via D-14)
      - deployment       (required at schema level; submitter fills via D-14)
      - product_nodes    (optional)
      - product_switches (optional)
      - total_rack_units (optional)
      - rack_power_supplies (optional)

    Rationale (Pitfall 1 / D-14): `Solution.architecture.storage_location` is
    an enum and `""` is not a legal enum value. `Architecture.check_na_pairing`
    and `Capabilities.check_remap_time` are `model_validator`s that fire at
    construction time. Whole-file `schema_validator.validate_file()` will fail
    on the missing required fields — that IS the intended "submitter has work
    to do" UX (SER-02). Stubbing these blocks would just shift the failure
    from "missing block" to "invalid enum value", which is a worse signal.
    """
    return {
        "system_under_test": {
            # solution: OMITTED per D-14 (submitter fills)
            # deployment: OMITTED per D-14 (submitter fills)
            # product_nodes: OMITTED (optional)
            # product_switches: OMITTED (optional)
            # total_rack_units: OMITTED (optional)
            # rack_power_supplies: OMITTED (optional)
            "clients": stanzas,
        }
    }


# ---------------------------------------------------------------------------
# Plan 02-04 — Filesystem write orchestrator (LIFE-01).
#
# Composes 02-02 (adapter + grouping) + 02-03 (stub splice + outer dict) + the
# D-7 sort + atomic O_CREAT|O_EXCL|O_WRONLY write into a single callable that
# materializes systemname.yaml on disk at the canonical Rules.md §2.1.8 path.
# The atomic-write recipe mirrors results_dir/sentinel.py:113-134 verbatim
# (same flags, same mode, same `os.fdopen`+`yaml.safe_dump` pattern) — Phase 2
# only deviates from that analog in three places:
#
# 1. FileExistsError → return None + logger.debug (Phase 5 will replace this
#    branch with diff-and-fail); the sentinel raises DoubleInitError.
# 2. yaml.safe_dump adds default_style='"' + explicit_start=True per D-10;
#    the sentinel uses defaults that emit unquoted plain scalars.
# 3. systemname.yaml's parent (<mode>/<orgname>/systems/) is created on
#    demand via mkdir(parents=True, exist_ok=True); the sentinel relies on
#    the init command having created its parent up front.
# ---------------------------------------------------------------------------

# File mode for the emitted systemname.yaml. World-readable on purpose
# (LAY-03 parity with the sentinel — every command must be able to read).
_SYSTEMNAME_YAML_MODE: Final[int] = 0o644


def _resolve_host_info_list(cluster_info) -> list:
    """Return the list of HostInfo to feed into the adapter.

    D-8: when the caller has a populated `cluster_info.host_info_list`, return
    that list as-is. Otherwise fall back to the single-host local collector —
    this covers the dev-iteration and CI-smoke-test paths where `--hosts` was
    not supplied and `cluster_info` is either None or carries an empty
    `host_info_list`.

    The fallback reuses `collect_local_system_info` + `HostInfo.from_collected_data`
    so the produced HostInfo has the same dict-derived shape as the MPI path —
    the adapter cannot tell the two paths apart.
    """
    if cluster_info is not None and getattr(cluster_info, "host_info_list", None):
        return cluster_info.host_info_list
    # D-8 single-host fallback.
    local_data = collect_local_system_info()
    return [HostInfo.from_collected_data(local_data)]


def write_systemname_yaml(args, cluster_info, logger) -> Optional[str]:
    """Materialize systemname.yaml at the Rules.md §2.1.8 canonical path.

    Composes the full Phase 02 pipeline:
      - D-12 writer-side gate: only `args.command == 'run'` writes; any other
        command (datagen, configview, datasize, validate, history, reportgen)
        returns None without touching the filesystem.
      - D-8 empty-fleet fallback: if `cluster_info` is None or has an empty
        `host_info_list`, falls back to local-only collection via
        `_resolve_host_info_list`.
      - Adapter + grouping: each HostInfo → node-shaped dict (02-02's
        `node_dict_from_host`), then quantity-collapsed via
        `group_by_fingerprint`.
      - D-7 sort: stanzas sorted by `(-quantity, chassis.cpu_model)` —
        largest quantity first, alphabetical cpu_model on ties.
      - D-14 outer dict + D-3 stub splice (02-03's helpers).
      - D-11 path derivation:
        `<results_dir>/<mode>/<orgname>/systems/<systemname>.yaml`. Parent
        directories are created on demand (mkdir parents=True, exist_ok=True).
      - D-9 atomic exclusive create: `os.open(..., O_CREAT|O_EXCL|O_WRONLY,
        0o644)`. Mirrors `results_dir/sentinel.py:113-134`. On FileExistsError
        (T-2-01 race loser OR pre-existing file OR T-2-08 symlink at target),
        returns None and emits a logger.debug — does NOT raise. Any OTHER
        filesystem error (EACCES, ENOSPC, …) propagates per D-9: the writer
        does NOT swallow non-FileExistsError exceptions.
      - D-10 emit: `yaml.safe_dump(..., default_flow_style=False,
        default_style='"', explicit_start=True, sort_keys=False)`.

    NOT done here (deferred):
      - Validation (`schema_validator.validate_file`): D-15 — never called
        from the writer. The schema_validator is the user-facing check at
        submission time, surfacing the SER-02 blanks as "submitter has work
        to do". Calling it inside the writer would couple the two lifecycles
        and turn a known-incomplete YAML emit into a hard error.
      - Diff-and-fail on existing file: Phase 5 LIFE-02 territory; Phase 2's
        FileExistsError branch is a deliberate no-op.
      - `args.systemname` syntactic validation: Pitfall 10 — Phase 1's
        `generate_output_location` already enforces non-empty +
        well-formed systemname via the upstream `_reserve_run_directory` call
        in `Benchmark.__init__`. Phase 2's writer relies on that upstream
        contract rather than adding a redundant guard.

    Args:
        args: namespace with `command`, `results_dir`, `mode`, `orgname`,
            `systemname` attributes (e.g. argparse Namespace).
        cluster_info: optional object with `.host_info_list: list[HostInfo]`.
            None or empty `host_info_list` triggers D-8 fallback.
        logger: standard Python logger (used for .debug on no-op-if-exists
            and .info on successful write).

    Returns:
        str path to the written file on success, None if D-12 gated the write
        or if D-9 FileExistsError fired (already exists / race loser / symlink
        at target).

    Raises:
        OSError / PermissionError / etc. for filesystem errors OTHER than
        FileExistsError (D-9: the universal collection-failure rule applies
        to collector failures, NOT filesystem failures).
    """
    # D-12: writer-side gate. Defensive getattr in case `args` is a MagicMock
    # without `command` set explicitly — yields None, which != 'run'.
    if getattr(args, "command", None) != "run":
        return None

    # D-8: resolve hosts (cluster fleet OR local-only fallback).
    hosts = _resolve_host_info_list(cluster_info)

    # 02-02 adapter + grouping.
    node_items = [node_dict_from_host(h) for h in hosts]
    stanzas = group_by_fingerprint(node_items, _FINGERPRINT_KEYS, "quantity")

    # D-7: largest quantity first, alphabetical cpu_model on ties.
    stanzas.sort(key=lambda n: (-n["quantity"], n["chassis"]["cpu_model"]))

    # 02-03 outer dict + stub splice.
    dump = _build_outer_dict(stanzas)
    dump = _splice_stub_lists(dump)

    # D-11: canonical path.
    systemname_path = (
        Path(args.results_dir)
        / args.mode
        / args.orgname
        / "systems"
        / f"{args.systemname}.yaml"
    )
    systemname_path.parent.mkdir(parents=True, exist_ok=True)

    # D-9: atomic exclusive create. Kernel-level race-free. POSIX guarantees
    # O_EXCL fails (EEXIST → FileExistsError) if the path resolves to anything
    # pre-existing including a symlink — that covers T-2-01 (race) AND T-2-08
    # (symlink-at-target) in one syscall.
    try:
        fd = os.open(
            str(systemname_path),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            _SYSTEMNAME_YAML_MODE,
        )
    except FileExistsError:
        logger.debug(
            f"systemname.yaml already exists at {systemname_path}; no-op "
            f"(Phase 5 will own diff-and-fail)"
        )
        return None

    # `os.fdopen` adopts the fd; closing the file object closes the fd.
    # D-10 emit kwargs locked here.
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        yaml.safe_dump(
            dump,
            fh,
            default_flow_style=False,
            default_style='"',
            explicit_start=True,
            sort_keys=False,
        )

    logger.info(f"Wrote {systemname_path}")
    return str(systemname_path)
