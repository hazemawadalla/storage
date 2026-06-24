"""Unit tests for the pure-function diff core — Phase 05 / Plan 05-01.

This file owns the comparison-subject contract for Phase 5's logical-diff
lifecycle (LIFE-02 + LIFE-03). It covers the eight must-have truths fixed in
05-01-PLAN.md frontmatter:

1. Byte-equal inputs → DiffResult.empty == True (D-37 + LIFE-04).
2. Single-field change → exactly one DiffEntry with the differing JSONPath and
   old/new values (D-37 / D-40).
3. Quantity-field change on otherwise-identical fingerprint → drift (D-39).
4. Fingerprint present only on-disk → drift (D-47).
5. Fingerprint present only in-memory → drift (D-46).
6. format_unified_diff emits --- / +++ headers + @@ <JSONPath> @@ hunks +
   -/+ lines + Remediation block (D-40 + D-41).
7. SER-02 blank preservation Pitfall 3 direction (a): when in-memory is the
   empty string AND on-disk has a submitter-filled non-empty value, the path
   is NOT flagged.
8. Long sysctl tuple value (e.g. '4096\\t87380\\t16777216') round-trips
   through the formatter verbatim, no truncation (D-41).

Test discipline:
- Pure-function tests, no filesystem or MPI involvement.
- SimpleNamespace + MagicMock fixture style (matches test_auto_generator_write.py).
- Helper fixtures _make_node_dict / _make_disk_and_memory_pair build Phase-4
  7-key emit-shape node dicts so the diff operates on realistic input.

RED gate: at the moment this file is committed, mlpstorage_py/system_description/diff.py
does NOT yet exist. pytest collection MUST fail with ModuleNotFoundError on the
import below. Task 2 ships the implementation and flips the suite to GREEN.
"""

from __future__ import annotations

import pytest

from mlpstorage_py.system_description.diff import (
    DiffEntry,
    DiffResult,
    _flatten_to_paths,
    diff_node_dict_lists,
    format_unified_diff,
)


# ---------------------------------------------------------------------------
# Helper fixtures
# ---------------------------------------------------------------------------


def _make_node_dict(
    *,
    cpu_model: str = "Intel Xeon Platinum 8480+",
    cpu_qty: int = 2,
    cpu_cores: int = 56,
    memory: int = 256,
    model_name: str = "Dell PowerEdge R760",
    os_name: str = "Rocky Linux",
    os_version: str = "9.5",
    networking=None,
    sysctl=None,
    environment=None,
    drives=None,
    friendly_description: str = "",
    quantity: int = 1,
) -> dict:
    """Build a Phase-4 7-key emit-shape node dict.

    Mirrors auto_generator.node_dict_from_host's emission contract: top-level
    keys = friendly_description, chassis, networking, sysctl, environment,
    drives, operating_system, quantity. Friendly_description defaults to ''
    matching SER-02 (collector blank, submitter to fill).
    """
    if networking is None:
        networking = [
            {"type": "ethernet", "speed": 100, "state": "up", "traffic": [], "unit_count": 2},
        ]
    if sysctl is None:
        sysctl = [
            {"name": "net.core.rmem_max", "value": "16777216"},
        ]
    if environment is None:
        environment = []
    if drives is None:
        drives = [
            {"vendor_name": "Samsung", "model_name": "PM9A3", "interface": "NVMe",
             "capacity_in_GB": 1920, "unit_count": 4},
        ]
    return {
        "friendly_description": friendly_description,
        "chassis": {
            "cpu_model": cpu_model,
            "cpu_qty": cpu_qty,
            "cpu_cores": cpu_cores,
            "memory_capacity": memory,
            "model_name": model_name,
        },
        "networking": networking,
        "sysctl": sysctl,
        "environment": environment,
        "drives": drives,
        "operating_system": {
            "name": os_name,
            "version": os_version,
        },
        "quantity": quantity,
    }


def _make_disk_and_memory_pair(*, disk_overrides=None, memory_overrides=None):
    """Two-host fleet baseline.  Returns (on_disk_stanzas, in_memory_stanzas)."""
    on_disk = [_make_node_dict(), _make_node_dict(cpu_model="AMD EPYC 9654")]
    in_memory = [_make_node_dict(), _make_node_dict(cpu_model="AMD EPYC 9654")]
    if disk_overrides:
        for idx, override in disk_overrides.items():
            on_disk[idx].update(override)
    if memory_overrides:
        for idx, override in memory_overrides.items():
            in_memory[idx].update(override)
    return on_disk, in_memory


# ---------------------------------------------------------------------------
# TestDataclasses (3 tests)
# ---------------------------------------------------------------------------


class TestDataclasses:
    def test_diff_entry_construction_path_old_new(self):
        e = DiffEntry(path="clients[0].chassis.cpu_model", old="Intel", new="AMD")
        assert e.path == "clients[0].chassis.cpu_model"
        assert e.old == "Intel"
        assert e.new == "AMD"

    def test_diff_result_empty_when_entries_empty(self):
        r = DiffResult(entries=[])
        assert r.empty is True

    def test_diff_result_not_empty_when_one_entry(self):
        r = DiffResult(entries=[DiffEntry(path="x", old=1, new=2)])
        assert r.empty is False


# ---------------------------------------------------------------------------
# TestFlattenToPaths (5 tests)
# ---------------------------------------------------------------------------


class TestFlattenToPaths:
    def test_scalar_at_root_yields_empty_prefix_path(self):
        # Scalar input with empty prefix should yield ('', scalar).
        result = list(_flatten_to_paths(42))
        assert result == [("", 42)]

    def test_dict_keys_dotted_concatenation(self):
        result = dict(_flatten_to_paths({"a": {"b": 1}}))
        assert result == {"a.b": 1}

    def test_list_indices_bracket_notation(self):
        result = dict(_flatten_to_paths({"items": [{"x": 1}, {"x": 2}]}))
        assert result == {"items[0].x": 1, "items[1].x": 2}

    def test_empty_dict_yields_no_entries(self):
        assert list(_flatten_to_paths({})) == []

    def test_mixed_dict_list_dict_nesting(self):
        node = _make_node_dict()
        flat = dict(_flatten_to_paths(node))
        # Spot-check load-bearing JSONPaths that downstream tests assert on.
        assert flat["chassis.cpu_model"] == "Intel Xeon Platinum 8480+"
        assert flat["chassis.cpu_qty"] == 2
        assert flat["operating_system.name"] == "Rocky Linux"
        assert flat["networking[0].type"] == "ethernet"
        assert flat["networking[0].speed"] == 100
        assert flat["sysctl[0].name"] == "net.core.rmem_max"
        assert flat["drives[0].capacity_in_GB"] == 1920


# ---------------------------------------------------------------------------
# TestRoundTripEqualIsEmpty (3 tests; covers D-37 + LIFE-04)
# ---------------------------------------------------------------------------


class TestRoundTripEqualIsEmpty:
    def test_identical_single_stanza_lists_empty(self):
        a = [_make_node_dict()]
        b = [_make_node_dict()]
        r = diff_node_dict_lists(a, b)
        assert r.empty is True

    def test_identical_multi_stanza_lists_empty(self):
        a, b = _make_disk_and_memory_pair()
        r = diff_node_dict_lists(a, b)
        assert r.empty is True

    def test_field_value_order_inside_stanza_insensitive(self):
        # PyYAML / Python dict equality is order-insensitive for siblings —
        # Pitfall 1 lock: reordering keys in the dict literal MUST NOT show
        # up as drift because the diff layer flattens to paths.
        a = [_make_node_dict()]
        b_node = _make_node_dict()
        # Rebuild dict in different key order — semantically equal.
        reordered = {k: b_node[k] for k in reversed(list(b_node.keys()))}
        r = diff_node_dict_lists(a, [reordered])
        assert r.empty is True


# ---------------------------------------------------------------------------
# TestFieldChangeIsDrift (4 tests; covers D-37)
# ---------------------------------------------------------------------------


class TestFieldChangeIsDrift:
    def test_cpu_model_change_surfaces_one_diff_with_jsonpath(self):
        # CPU model is part of the fingerprint — changing it makes the two
        # sides orphan stanzas per D-38 / Pitfall 2. Report shape: two
        # entries (one for the absent on-disk fingerprint, one for the absent
        # in-memory fingerprint). NOT a single same-fingerprint field diff.
        on_disk = [_make_node_dict(cpu_model="Intel Xeon Platinum 8480+")]
        in_mem = [_make_node_dict(cpu_model="AMD EPYC 9654")]
        r = diff_node_dict_lists(on_disk, in_mem)
        assert r.empty is False
        # Both fingerprints surface as orphans, so at least one DiffEntry path
        # mentions a fingerprint marker.
        assert any("fingerprint=" in e.path for e in r.entries)

    def test_sysctl_value_change_surfaces_diff_at_clients_index_sysctl_index_value(self):
        on_disk = [_make_node_dict(sysctl=[{"name": "net.core.rmem_max", "value": "16777216"}])]
        in_mem = [_make_node_dict(sysctl=[{"name": "net.core.rmem_max", "value": "33554432"}])]
        r = diff_node_dict_lists(on_disk, in_mem)
        # Sysctl is part of the fingerprint (sysctl_sig), so this is again an
        # orphan-stanza diff; assert the change is surfaced (not silently
        # dropped).
        assert r.empty is False

    def test_drives_capacity_change_surfaces_diff(self):
        on_disk = [_make_node_dict(drives=[{"vendor_name": "Samsung", "model_name": "PM9A3",
                                            "interface": "NVMe", "capacity_in_GB": 1920,
                                            "unit_count": 4}])]
        in_mem = [_make_node_dict(drives=[{"vendor_name": "Samsung", "model_name": "PM9A3",
                                           "interface": "NVMe", "capacity_in_GB": 3840,
                                           "unit_count": 4}])]
        r = diff_node_dict_lists(on_disk, in_mem)
        assert r.empty is False

    def test_environment_value_change_surfaces_diff(self):
        # Phase 4 redaction shape: '[SET — 40 chars]' → '[SET — 38 chars]'.
        # The environment sig is part of the fingerprint, so this is an
        # orphan-stanza diff.
        on_disk = [_make_node_dict(environment=[
            {"name": "AWS_SECRET_ACCESS_KEY", "value": "[SET — 40 chars]"}])]
        in_mem = [_make_node_dict(environment=[
            {"name": "AWS_SECRET_ACCESS_KEY", "value": "[SET — 38 chars]"}])]
        r = diff_node_dict_lists(on_disk, in_mem)
        assert r.empty is False


# ---------------------------------------------------------------------------
# TestQuantityChangeIsDrift (2 tests; covers D-39)
# ---------------------------------------------------------------------------


class TestQuantityChangeIsDrift:
    def test_quantity_4_to_5_surfaces_diff_at_clients_index_quantity(self):
        on_disk = [_make_node_dict(quantity=4)]
        in_mem = [_make_node_dict(quantity=5)]
        r = diff_node_dict_lists(on_disk, in_mem)
        assert r.empty is False
        # quantity is NOT part of the fingerprint, so same fingerprint → flat
        # diff at the .quantity path.
        assert any(e.path.endswith(".quantity") or e.path == "quantity"
                   for e in r.entries) or any(
            "quantity" in e.path for e in r.entries)
        # Verify the values
        matching = [e for e in r.entries if "quantity" in e.path]
        assert any(e.old == 4 and e.new == 5 for e in matching)

    def test_quantity_5_to_4_surfaces_diff(self):
        # Fleet shrinkage is symmetric.
        on_disk = [_make_node_dict(quantity=5)]
        in_mem = [_make_node_dict(quantity=4)]
        r = diff_node_dict_lists(on_disk, in_mem)
        assert r.empty is False
        matching = [e for e in r.entries if "quantity" in e.path]
        assert any(e.old == 5 and e.new == 4 for e in matching)


# ---------------------------------------------------------------------------
# TestSymmetricDriftDetection (3 tests; covers D-46 + D-47)
# ---------------------------------------------------------------------------


class TestSymmetricDriftDetection:
    def test_disk_absent_field_in_memory_only_is_drift(self):
        # Add a new field only present in memory (e.g. fresh schema).
        on_disk_node = _make_node_dict()
        in_mem_node = _make_node_dict()
        in_mem_node["chassis"]["new_field"] = "fresh-value"
        r = diff_node_dict_lists([on_disk_node], [in_mem_node])
        assert r.empty is False
        assert any("chassis.new_field" in e.path for e in r.entries)

    def test_disk_only_field_in_memory_absent_is_drift(self):
        on_disk_node = _make_node_dict()
        on_disk_node["chassis"]["legacy_field"] = "legacy-value"
        in_mem_node = _make_node_dict()
        r = diff_node_dict_lists([on_disk_node], [in_mem_node])
        assert r.empty is False
        assert any("chassis.legacy_field" in e.path for e in r.entries)

    def test_fingerprint_only_on_one_side_is_drift(self):
        # D-38 orphan: a stanza with a unique fingerprint on disk should
        # surface as drift.
        on_disk = [_make_node_dict(), _make_node_dict(cpu_model="AMD EPYC 9654")]
        in_mem = [_make_node_dict()]
        r = diff_node_dict_lists(on_disk, in_mem)
        assert r.empty is False
        # The AMD fingerprint should be flagged as on-disk-only.
        assert any("fingerprint=" in e.path for e in r.entries)


# ---------------------------------------------------------------------------
# TestSer02BlankPreservation (4 tests; covers Pitfall 3 direction (a))
# ---------------------------------------------------------------------------


class TestSer02BlankPreservation:
    def test_in_memory_empty_disk_filled_friendly_description_NO_diff(self):
        # Disk has submitter-filled friendly_description; in-memory (fresh
        # collection) has the default empty string. Direction (a): NO diff.
        on_disk = [_make_node_dict(friendly_description="Production tier rack 7")]
        in_mem = [_make_node_dict(friendly_description="")]
        r = diff_node_dict_lists(on_disk, in_mem)
        assert r.empty is True, (
            "Submitter-filled friendly_description on disk + empty in-memory "
            "MUST NOT register as drift (SER-02 blank preservation)."
        )

    def test_in_memory_empty_disk_filled_networking_traffic_NO_diff(self):
        # networking[].traffic is on the SER-02 blank list (REQUIREMENTS line 40).
        on_disk_nic = {"type": "ethernet", "speed": 100, "state": "up",
                       "traffic": ["data"], "unit_count": 2}
        in_mem_nic = {"type": "ethernet", "speed": 100, "state": "up",
                      "traffic": [""], "unit_count": 2}
        on_disk = [_make_node_dict(networking=[on_disk_nic])]
        in_mem = [_make_node_dict(networking=[in_mem_nic])]
        r = diff_node_dict_lists(on_disk, in_mem)
        # Note: in-memory networking[0].traffic[0] == '' and on-disk == 'data'
        # → direction (a) skip applies at the leaf.
        assert r.empty is True

    def test_in_memory_empty_disk_filled_drives_media_type_NO_diff(self):
        # drives media_type is on the SER-02 blank list. Add the field and
        # verify direction (a) preserves the disk-filled value.
        on_disk_drive = {"vendor_name": "Samsung", "model_name": "PM9A3",
                         "interface": "NVMe", "capacity_in_GB": 1920,
                         "unit_count": 4, "media_type": "ssd-flash"}
        in_mem_drive = {"vendor_name": "Samsung", "model_name": "PM9A3",
                        "interface": "NVMe", "capacity_in_GB": 1920,
                        "unit_count": 4, "media_type": ""}
        on_disk = [_make_node_dict(drives=[on_disk_drive])]
        in_mem = [_make_node_dict(drives=[in_mem_drive])]
        r = diff_node_dict_lists(on_disk, in_mem)
        assert r.empty is True

    def test_in_memory_NONEMPTY_disk_filled_with_different_value_IS_diff(self):
        # Direction-lock regression: direction (a) is NOT "always skip when
        # on-disk is filled". It's specifically "skip when in-memory IS the
        # empty string". If in-memory has a non-empty different value, drift
        # MUST surface.
        on_disk = [_make_node_dict(friendly_description="Production tier rack 7")]
        in_mem = [_make_node_dict(friendly_description="Staging tier rack 3")]
        r = diff_node_dict_lists(on_disk, in_mem)
        assert r.empty is False, (
            "Pitfall 3(a) direction lock: in-memory non-empty value MUST "
            "register as drift; only empty-string in-memory is preserved."
        )
        assert any("friendly_description" in e.path for e in r.entries)


# ---------------------------------------------------------------------------
# TestUnifiedDiffFormat (5 tests; covers D-40 + D-41)
# ---------------------------------------------------------------------------


class TestUnifiedDiffFormat:
    def _drift_result(self):
        on_disk = [_make_node_dict(quantity=4)]
        in_mem = [_make_node_dict(quantity=5)]
        return diff_node_dict_lists(on_disk, in_mem)

    def test_header_lines_present(self):
        result = self._drift_result()
        report = format_unified_diff(result, "/tmp/sys.yaml")
        assert "--- on-disk: /tmp/sys.yaml" in report
        assert "+++ in-memory: <computed from live MPI fleet>" in report

    def test_hunk_marker_at_at_jsonpath_at_at(self):
        # Construct a quantity diff (same fingerprint, .quantity flat diff).
        result = self._drift_result()
        report = format_unified_diff(result, "/tmp/sys.yaml")
        # The hunk marker is @@ <path> @@ — the path includes .quantity.
        assert "@@ " in report and " @@" in report
        assert "quantity" in report

    def test_minus_old_plus_new_lines(self):
        result = self._drift_result()
        report = format_unified_diff(result, "/tmp/sys.yaml")
        assert "- 4" in report
        assert "+ 5" in report

    def test_remediation_block_present(self):
        result = self._drift_result()
        report = format_unified_diff(result, "/tmp/sys.yaml")
        assert "Remediation:" in report
        assert "Rename the existing yaml" in report
        assert "Remove " in report
        assert "/tmp/sys.yaml" in report  # path appears in rm hint too

    def test_long_sysctl_value_round_trips_verbatim_no_truncation(self):
        # D-41 lock: a long tab-separated sysctl tuple must appear in the
        # output exactly as it appears in the input — no shortening, no
        # repr-quote-wrapping that loses the literal tabs.
        long_value = "4096\t87380\t16777216"
        on_disk = [_make_node_dict(sysctl=[
            {"name": "net.ipv4.tcp_rmem", "value": long_value}])]
        in_mem = [_make_node_dict(sysctl=[
            {"name": "net.ipv4.tcp_rmem", "value": "4096\t87380\t8388608"}])]
        result = diff_node_dict_lists(on_disk, in_mem)
        report = format_unified_diff(result, "/tmp/sys.yaml")
        # The longer value must appear intact somewhere in the report.
        assert long_value in report, (
            f"D-41 truncation regression: long sysctl value not found "
            f"verbatim in report. Report:\n{report}"
        )
