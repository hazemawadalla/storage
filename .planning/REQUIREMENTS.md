# Requirements: Client System Information Auto-Collection

**Defined:** 2026-06-18
**Core Value:** A storage submitter can take a benchmark result directory, hand it to the MLCommons submission checker, and have it pass — without having to hand-tune the submission package against a moving target.

**Milestone Core Value:** First-run `run` writes a `systemname.yaml` to the canonical Rules.md-shaped path containing every `clients[]` field that can be proven from `/proc`, `/sys`, `lsblk`, `os.environ`, and `os.statvfs` — and on subsequent runs, *diffs* the in-memory image against the on-disk version and refuses to overwrite drift, so a submitter can't accidentally mix results from one client fleet into another fleet's system description.

## v1 Requirements

### Layout & Initialization (LAY)

The submission package's directory shape per Rules.md §2.1.5-2.1.8 is `<results-dir>/<mode>/<orgname>/{code|results|systems}/...`. mlpstorage today does NOT emit this shape — `generate_output_location()` produces `<results-dir>/<benchmark>/<model>/<command>/<datetime>/` without the `<mode>/<orgname>/results/<systemname>/` prefix. Phase 1 closes that gap and introduces a results-dir bootstrap subcommand that pins the orgname to the directory at creation time.

- [x] **LAY-01**: `mlpstorage init <orgname> <path>` subcommand creates the results-dir bootstrap. Refuses if `<path>` is a non-empty directory unless it was previously initialized by the same tool (in which case the existing `mlperf-results.yaml` is read and either confirmed or rejected with a clear message). No `--force` flag in v1.
- [x] **LAY-02**: `mlpstorage init` writes `<path>/mlperf-results.yaml` containing `mlperf_results_version: 1`, `orgname` (as supplied), `initialized_at` (ISO-8601 timestamp), and `initialized_by` (the running mlpstorage version). The file validates against a ships-with-mlpstorage schema.
- [x] **LAY-03**: Every mlpstorage command that takes `--results-dir` (datagen, run, configview, datasize, reportgen, validate, history, etc.) reads orgname exclusively from `<results-dir>/mlperf-results.yaml`. There is NO `--orgname` CLI flag and NO `MLPERF_ORGNAME` env var consulted by these commands. If `mlperf-results.yaml` is missing or unparseable, the command fails before any work begins with: "results-dir `<path>` has not been initialized. Run `mlpstorage init <orgname> <path>` first."
- [x] **LAY-04**: `mlpstorage` accepts `--systemname` CLI flag for `run` (and other commands that need it), with default from `MLPERF_SYSTEMNAME` env var. A single results-dir can legitimately hold multiple system-name subtrees per Rules.md §2.1.8; systemname is therefore per-run, not pinned at init.
- [x] **LAY-05**: `generate_output_location()` in `mlpstorage_py/rules/utils.py` produces `<results-dir>/<mode>/<orgname>/results/<systemname>/<benchmark>/<model>/<command>/<datetime>/` for every command that emits results. `<mode>` is `closed`/`open`/`whatif`. `<orgname>` comes from the sentinel. `<systemname>` comes from the CLI flag / env var.
- [x] **LAY-06**: Code-image capture policy per the three modes — `closed` captures one code image per submitter (under `<results-dir>/closed/<orgname>/code/`), `open` captures one code image per (benchmark, command) tuple, `whatif` captures no code image at all. The capture mechanism honors Rules.md §2.1.6 for closed.
- [x] **LAY-07**: `whatif` mode produces the same directory shape (`<results-dir>/whatif/<orgname>/results/<systemname>/...`) for self-consistency, even though whatif is non-submittable. The submission checker will continue to reject `whatif/` at the top level — that's correct.
- [x] **LAY-08**: Existing test fixtures and submission-checker tests that hard-code the old layout (`tests/unit/test_loader_metadata_refresh.py`, `tests/unit/test_definition_of_done.py`, `tests/unit/test_checkpointing_check_retrofit.py`, `mlpstorage_py/tests/conftest.py`, and others) are updated to expect the new layout. The submission-checker's existing layout checks continue to pass on the new output shape.

### Collection (COLL)

The collector extracts these fields from the per-host data the MPI cluster collector already gathers, or by extending the collector to read additional sysfs / tool output.

**Universal failure rule:** Any collection failure — a missing file, a missing sysfs node, a missing field within a parsed structure, a tool not on PATH, a parse error — returns an empty string for that single data point. The collector never errors out the benchmark over a collection failure. Any field unfilled by the collector is left blank for the submitter to fill in by hand.

- [x] **COLL-01**: Collector exposes `clients[].chassis.cpu_model`, `cpu_qty`, `cpu_cores`, `memory_capacity` (GiB) for every host, sourced from the existing `/proc/cpuinfo` + `/proc/meminfo` data already gathered by `mlpstorage_py/cluster_collector.py:collect_local_system_info()` and `summarize_cpuinfo()`. Per the universal rule, any unreadable source yields empty strings for the affected fields.
- [x] **COLL-02**: Collector exposes `clients[].operating_system.name` and `operating_system.version` for every host, sourced from the `/etc/os-release` data already gathered. Per the universal rule.
- [x] **COLL-03**: Collector exposes `clients[].chassis.model_name` from `/sys/class/dmi/id/product_name`. Per the universal rule, unreadable yields empty string.
- [x] **COLL-04**: Collector exposes `clients[].networking[]` entries with `type` (`ethernet` / `infiniband` / `other`), `speed` (Gbps), and `unit_count`, sourced from `/sys/class/net/<iface>/{type,speed}` and the presence of `/sys/class/infiniband/`. Virtual interfaces (`lo`, `docker*`, `virbr*`, `veth*`, `bond` slaves) are filtered out. Interfaces in the `down` state with `speed: -1` are reported with a recognizable sentinel rather than omitted. Per the universal rule for any unreadable per-interface field. (End-to-end complete in 03-05: collector 03-03 + transform 03-04 + HostInfo dataclass + node_dict_from_host emit-side wire-through.)
- [ ] **COLL-05**: Collector exposes `clients[].sysctl[]` as a snapshot of a curated MLPerf-relevant key allowlist read from `/proc/sys/*`. Initial allowlist: `vm.dirty_*`, `net.core.*`, `net.ipv4.tcp_*`, `kernel.numa_balancing`. Allowlist is data-driven and extensible. Per the universal rule for any unreadable key.
- [ ] **COLL-06**: Collector exposes `clients[].environment[]` as a snapshot of `os.environ` filtered to the prefix allowlist `AWS_*`, `BUCKET`, `STORAGE_*`, `OMPI_*`, `UCX_*`, `NCCL_*`. Credential values (`AWS_SECRET_ACCESS_KEY`, `AWS_ACCESS_KEY_ID`) are redacted per the policy already established in `mlpstorage_py/storage_config.py`.
- [ ] **COLL-07**: Collector exposes `clients[].drives[]` populated from `lsblk -J -d -o NAME,MODEL,VENDOR,SIZE,ROTA,TRAN,RM`: `vendor_name`, `model_name`, `capacity_in_GB` (base 10), and `interface` (nvme/sata/sas/other). `media_type` (HDD/TLC/QLC), `form_factor`, and `performance` are NOT auto-filled — they're vendor-published spec sheet facts. Per the universal rule, missing `lsblk` or any per-device field yields empty strings.

### Serialization (SER)

- [x] **SER-01**: Per-host collected data is **quantity-grouped** before serialization: hosts that share the same canonical chassis fingerprint (the entire set of fields the collector currently fills — extensible) collapse into one `clients[]` stanza with `quantity: N` matching the group size. Heterogeneous fleets produce multiple stanzas.
- [x] **SER-02**: Schema fields that cannot be auto-derived from the client (`friendly_description`, `chassis.rack_units`, `networking[].traffic` role, drive `media_type`/`form_factor`/`performance`, `chassis.power`/`psus_configured`) are emitted as blank/missing in the auto-generated YAML so that a downstream schema-validation pass naturally flags "submitter has fields to fill in."
- [x] **SER-03**: The serialized YAML's filled fields validate against `mlpstorage_py/system_description/schema.yaml` using the existing `schema_validator.py`. Whole-file schema validation may still fail on blanks left by SER-02 — that's the intended UX.

### Lifecycle (LIFE)

- [x] **LIFE-01**: At the start of `mlpstorage <mode> <benchmark> [model] run` (and only `run` — not `datagen`, since the datagen client fleet may differ from the run fleet), the benchmark builds the in-memory `systemname.yaml` representation from the MPI-collected data. The target path is `<results-dir>/<mode>/<orgname>/systems/<systemname>.yaml` where `<orgname>` comes from `<results-dir>/mlperf-results.yaml` and `<systemname>` comes from `--systemname` / `MLPERF_SYSTEMNAME`. If the file does not exist, the in-memory image is written before the benchmark proceeds.
- [x] **LIFE-02**: When `<results-dir>/<mode>/<orgname>/systems/<systemname>.yaml` already exists, the on-disk version is loaded and a *logical* diff (YAML tree diff after canonicalization, not a text diff) is computed against the in-memory image, comparing **only fields the auto-collector is responsible for filling**. User-supplied fields (the blanks from SER-02) are not part of the diff — submitters can fill them in without re-triggering drift detection. The diff is per-mode: each mode (`closed`/`open`/`whatif`) owns its own systemname.yaml at its own path, generated and checked independently. The same fleet under different modes can legitimately produce different content (e.g., environment vars filtered to mode-specific allowlists).
- [x] **LIFE-03**: When the LIFE-02 diff is non-empty, the benchmark fails before any DLIO/MPI execution begins, with an error that (a) lists each differing field by JSONPath-style path, (b) shows the on-disk value and the in-memory value side-by-side, and (c) instructs the submitter to either rename the existing `<systemname>.yaml` (and re-run with a different `--systemname`, generating a fresh one) or remove it and re-run.
- [x] **LIFE-04**: When the LIFE-02 diff is empty, the benchmark proceeds without touching the on-disk file; whatever the submitter has filled into the blanks survives across runs.

### Capacity (CAP)

- [x] **CAP-01**: At `datagen` startup, after computing the dataset size in bytes, the benchmark calls `os.statvfs()` on the **dataset destination directory** (`--data-dir` for training and checkpointing, the engine-specific data path for vectordb / kvcache — *not* `--results-dir`, which lives off the system-under-test and only holds logs and metadata) and compares free space against the computed size. If free space < computed size, the benchmark fails before generation begins with a message naming the path, the available bytes, the required bytes, and the deficit. Per-node check on multi-node runs (each rank checks its own destination so a single starved node fails fast).
- [x] **CAP-02**: At `datagen` or `run` startup on multi-host operations, the benchmark verifies that every participating host sees the **same shared filesystem** at the dataset destination directory. The check collects a filesystem identifier (e.g., `stat -f -c '%i' <data-dir>` on each host, or the Python equivalent `os.statvfs(<data-dir>).f_fsid`) from every rank and compares values. If the set of returned IDs has cardinality > 1, the benchmark fails before any work begins with a message listing each host and the filesystem ID it reported, plus a one-line explanation that this typically means one or more hosts have a local-disk path where a shared mount was expected. On single-host runs (`--hosts` defaults to None or has length 1), CAP-02 is a no-op. Implementation note for Phase 5 discuss/plan: tool choice (`stat -f` vs. `os.statvfs().f_fsid` vs. write-a-sentinel-and-read-on-peer) should be decided during planning; the `fsid` approach is simple but has known edge cases with bind mounts and FUSE that may warrant a sentinel-file fallback.

## v2 Requirements

Deferred — not in current milestone, but flagged for future consideration.

### Schema Validation Gate (SCH)

- **SCH-01**: A `mlpstorage validate` extension that runs `schema_validator.validate_file()` against `systemname.yaml` and reports blank required fields as actionable "submitter work to do" rather than generic schema errors.

### Submission Bundle (BUN)

- **BUN-01**: `mlpstorage reports reportgen` includes the auto-generated `systemname.yaml` in the submission bundle automatically.

### Init Adoption (ADP)

- **ADP-01**: `mlpstorage init --adopt <orgname> <path>` for migrating an existing non-initialized results-dir to the new layout. Deliberately deferred — v1 forces clean starts.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Auto-generate `<systemname>.pdf` | PDF generation is its own can of worms; submission checker fails until submitter uploads one (existing responsibility). |
| `--force` flag on `mlpstorage init` | Safer default for v1; easy to add later if real need surfaces. |
| Auto-fill `product_nodes` | Submitters don't have programmatic access to the storage product's internals. |
| Auto-fill `product_switches` | Same — no access to network switch configuration. |
| Auto-fill drive `media_type` (TLC vs QLC), `form_factor`, vendor-published `performance` numbers | Vendor spec-sheet facts, not derivable from `/sys` or `lsblk`. |
| Auto-fill `power_device` / `psus_configured` | Requires `dmidecode -t 39` with root; nameplate watts live on physical labels; unreliable. |
| Auto-fill `friendly_description`, `networking[].traffic` role, `chassis.rack_units` | Semantic submitter declarations, not observable facts. |
| Fold the historical loose planning docs into context | Predate hundreds of commits of refactoring; assertions are stale. |
| macOS/Windows clients | MLPerf Storage clients are Linux-only; collector is `/proc`+`/sys`. |
| Run any collector probe as root | The benchmark runs as an unprivileged user; root-required sources are not in scope. |

## Traceability

Each v1 requirement maps to exactly one phase. See `.planning/ROADMAP.md` for full phase definitions and success criteria.

| Requirement | Phase | Status |
|-------------|-------|--------|
| LAY-01  | Phase 1 | Complete (01-02) |
| LAY-02  | Phase 1 | Complete (01-01) |
| LAY-03  | Phase 1 | Complete (01-04) |
| LAY-04  | Phase 1 | Complete (01-03) |
| LAY-05  | Phase 1 | Complete (01-03) |
| LAY-06  | Phase 1 | Complete (01-05) |
| LAY-07  | Phase 1 | Complete (01-05) |
| LAY-08  | Phase 1 | Complete (01-05) |
| COLL-01 | Phase 2 | Complete |
| COLL-02 | Phase 2 | Complete |
| COLL-03 | Phase 3 | Complete (03-05) — end-to-end; collector (03-02) + HostInfo wiring + node_dict_from_host emit (03-05) |
| COLL-04 | Phase 3 | Complete (03-05) — end-to-end; collector (03-03) + transform (03-04) + HostInfo wiring + node_dict_from_host emit (03-05) |
| COLL-05 | Phase 4 | Pending |
| COLL-06 | Phase 4 | Pending |
| COLL-07 | Phase 4 | Pending |
| SER-01  | Phase 2 | Complete |
| SER-02  | Phase 2 | Complete |
| SER-03  | Phase 2 | Complete |
| LIFE-01 | Phase 2 | Complete |
| LIFE-02 | Phase 5 | Complete |
| LIFE-03 | Phase 5 | Complete |
| LIFE-04 | Phase 5 | Complete |
| CAP-01  | Phase 5 / Plan 05-03 + 05-05 | Complete |
| CAP-02  | Phase 5 / Plan 05-04 | Complete |

**Coverage:**

- v1 requirements: 24 total
- Mapped to phases: 24
- Unmapped: 0

**Per-phase totals:**

- Phase 1 (Canonical Layout & Init): 8 requirements — LAY-01..08
- Phase 2 (First-Run Write of Partial systemname.yaml): 6 requirements — COLL-01, COLL-02, SER-01, SER-02, SER-03, LIFE-01
- Phase 3 (Chassis Model + Networking Coverage): 2 requirements — COLL-03, COLL-04
- Phase 4 (Sysctl, Environment, and Drives Coverage): 3 requirements — COLL-05, COLL-06, COLL-07
- Phase 5 (Logical Diff Lifecycle + Capacity Gate): 5 requirements — LIFE-02, LIFE-03, LIFE-04, CAP-01, CAP-02

---
*Requirements defined: 2026-06-18*
*Last updated: 2026-06-22 after Phase 2 UAT — added CAP-02 (shared-filesystem verification) to Phase 5 scope per submitter clarification on multi-host datagen / run gating.*
