# Roadmap: Client System Information Auto-Collection

## Overview

This milestone delivers auto-population of the `clients[]` section of `systemname.yaml` from data observable on the benchmark client systems, plus a drift-detection lifecycle and a startup capacity gate. Phase 1 is core infrastructure (canonical Rules.md §2.1-shaped directory layout, plus the `mlpstorage init` bootstrap that pins orgname to a results-dir via a `mlperf-results.yaml` sentinel) — laid down first so the later phases build on solid ground. Phase 2 ships the smallest user-visible artifact (a partial YAML built from data the existing MPI collector already gathers). Phases 3 and 4 widen field coverage. Phase 5 closes the loop with cross-run drift detection and a destination-free-space check.

## Phases

**Phase Numbering:**

- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Canonical Layout & Init** — Add `mlpstorage init <orgname> <path>` plus the `mlperf-results.yaml` sentinel; refactor `generate_output_location()` to emit the Rules.md §2.1-shaped tree; add `--systemname` CLI flag + `MLPERF_SYSTEMNAME` env-var default; update affected tests. (Completed 2026-06-19 — LAY-01..LAY-08 all green.)
- [x] **Phase 2: First-Run Write of Partial systemname.yaml** — On first `run`, write a quantity-grouped `systemname.yaml` containing CPU, memory, and OS for every client; leave non-derivable fields blank; no-op if the file already exists. (Completed 2026-06-19 — LIFE-01, SER-01..03, COLL-01..02 all green; 257-test verification suite passes.)
- [ ] **Phase 3: Chassis Model + Networking Coverage** — Extend the auto-filled YAML with DMI chassis `model_name` and a `networking[]` block sourced from sysfs.
- [ ] **Phase 4: Sysctl, Environment, and Drives Coverage** — Extend the auto-filled YAML with curated sysctl snapshot, redacted environment variables, and `lsblk`-sourced drive entries.
- [ ] **Phase 5: Logical Diff Lifecycle + Capacity Gate** — On re-runs, diff the in-memory image against the on-disk YAML for collector-owned fields and fail on drift; preserve user-filled blanks when unchanged; refuse to start `datagen` if the dataset destination directory lacks free space.

## Phase Details

### Phase 1: Canonical Layout & Init

**Goal:** After this phase, every `mlpstorage` command that emits results writes into a Rules.md §2.1-shaped tree under `<results-dir>/<mode>/<orgname>/results/<systemname>/...`, and orgname is pinned to the results-dir at creation time via a `mlperf-results.yaml` sentinel that every non-init command reads as authoritative. A submitter who runs `mlpstorage init Acme /path/to/results` can then run any benchmark without worrying about orgname drift, casing mismatches, or env-var-forgetting silent failures.
**Mode:** mvp
**Depends on:** Nothing (first phase)
**Requirements:** LAY-01, LAY-02, LAY-03, LAY-04, LAY-05, LAY-06, LAY-07, LAY-08
**Success Criteria** (what must be TRUE):

  1. `mlpstorage init Acme /tmp/r1` creates `/tmp/r1/mlperf-results.yaml` containing `orgname: Acme` (plus version, timestamp, mlpstorage version); a second `mlpstorage init Other /tmp/r1` fails before writing, with a clear message identifying the existing initialized orgname.
  2. Any command that takes `--results-dir` (e.g., `mlpstorage closed training datagen file --results-dir /tmp/uninit ...`) fails before any work if `<results-dir>/mlperf-results.yaml` is missing, with the actionable message "results-dir `<path>` has not been initialized. Run `mlpstorage init <orgname> <path>` first."
  3. `mlpstorage closed training unet3d run file --results-dir /tmp/r1 --systemname sys-v1 ...` writes its run output to `/tmp/r1/closed/Acme/results/sys-v1/training/unet3d/run/<timestamp>/`, not the legacy `/tmp/r1/training/unet3d/run/<timestamp>/`.
  4. The same submitter running `--mode open` and `--mode whatif` against `/tmp/r1` writes to `/tmp/r1/open/Acme/results/sys-v1/...` and `/tmp/r1/whatif/Acme/results/sys-v1/...` respectively; code-image capture under `code/` follows the per-mode policy (closed=one total, open=per-(benchmark,command), whatif=none).
  5. The full unit test suite passes after fixtures are updated for the new layout, and the submission checker's existing layout checks (`mlpstorage_py/submission_checker/checks/directory_checks.py`) pass on output produced by the new generator.

**Plans:** 5/5 plans complete
**Wave 1**

- [x] 01-01-PLAN.md — Slice 1: Sentinel infrastructure (Pydantic schema + atomic YAML I/O + domain errors) (LAY-02)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 01-02-PLAN.md — Slice 2: `mlpstorage init` subcommand wiring (LAY-01 + D-09 + D-11)
- [x] 01-03-PLAN.md — Slice 3: `generate_output_location()` rewrite + `--systemname`/`MLPERF_SYSTEMNAME` plumbing (LAY-04, LAY-05)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 01-04-PLAN.md — Slice 4: Orgname resolution gate in main._main_impl + banner (LAY-03 + D-12)

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 01-05-PLAN.md — Slice 5: Per-mode code-image capture + end-to-end integration tests (LAY-06, LAY-07, LAY-08)

### Phase 2: First-Run Write of Partial systemname.yaml

**Goal:** A submitter who has run `mlpstorage init <orgname> <results-dir>` (per Phase 1) and then runs `mlpstorage <mode> <benchmark> <model> run --results-dir <results-dir> --systemname <sys> ...` for the first time finds a quantity-grouped `<results-dir>/<mode>/<orgname>/systems/<sys>.yaml`, populated with the CPU, memory, and OS fields the existing MPI collector already gathers — with any blanks visibly waiting for them.
**Mode:** mvp
**Depends on:** Phase 1
**Requirements:** COLL-01, COLL-02, SER-01, SER-02, SER-03, LIFE-01
**Success Criteria** (what must be TRUE):

  1. After a fresh `mlpstorage closed training unet3d run file --results-dir /tmp/r1 --systemname sys-v1 ...` (with `/tmp/r1` already `init`'d as Acme), `/tmp/r1/closed/Acme/systems/sys-v1.yaml` exists and is non-empty.
  2. Opening that file shows `system_under_test.clients[]` entries with `chassis.cpu_model`, `chassis.cpu_qty`, `chassis.cpu_cores`, `chassis.memory_capacity`, `operating_system.name`, and `operating_system.version` filled in from the MPI-collected data, and shows non-derivable fields (`friendly_description`, `chassis.rack_units`, `networking[].traffic`, drive `media_type`/`form_factor`/`performance`, `chassis.power`/`psus_configured`) blank or absent.
  3. On a homogeneous fleet of N hosts, the file contains exactly one `clients[]` stanza with `quantity: N`; on a fleet where one host differs in CPU/memory/OS, the file contains two stanzas whose `quantity` values sum to the fleet size.
  4. Running `schema_validator.validate_file()` against the filled fields passes (whole-file validation may still report the intentional blanks from SER-02 — that's expected).
  5. Re-running the same command against the same results-dir does not overwrite or modify the existing `systemname.yaml` (Phase 2 ships the trivial "exists → don't touch" branch; the diff-and-fail behavior lands in Phase 5).
  6. The `datagen` command does NOT touch the systemname.yaml — neither writes nor diffs (datagen client fleet may legitimately differ from the run fleet).
  7. Per the universal collection-failure rule, any unreadable source (e.g., the test environment's `/proc/cpuinfo` is mocked to return a parse error) yields empty strings for the affected fields; `datagen` / `run` still completes.

**Plans:** 5/5 plans complete
**Wave 1**

- [x] 02-01-PLAN.md — Slice 1: HostCPUInfo.num_sockets data-model extension (D-16; COLL-01 prep)

**Wave 2** *(blocked on Wave 1)*

- [x] 02-02-PLAN.md — Slice 2: node_dict_from_host adapter + group_by_fingerprint helper (COLL-01, COLL-02, SER-01; D-4, D-5, D-6)

**Wave 3** *(blocked on Wave 2; same-file sequencing with 02-02)*

- [x] 02-03-PLAN.md — Slice 3: stub literals + _splice_stub_lists + _build_outer_dict (SER-02; D-3, D-14)

**Wave 4** *(blocked on Waves 2 + 3)*

- [x] 02-04-PLAN.md — Slice 4: write_systemname_yaml atomic orchestrator (LIFE-01, SER-01..03; D-7..D-12, T-2-01/04/08)

**Wave 5** *(blocked on Wave 4)*

- [x] 02-05-PLAN.md — Slice 5: Benchmark.run() hook + integration tests + kvcache/vectordb regression (LIFE-01 end-to-end)

### Phase 3: Chassis Model + Networking Coverage

**Goal:** The auto-generated `systemname.yaml` also reports the DMI chassis model name and a per-host networking inventory, so the submitter sees real hardware coverage instead of blank fields.
**Mode:** mvp
**Depends on:** Phase 2
**Requirements:** COLL-03, COLL-04
**Success Criteria** (what must be TRUE):

  1. On a host where `/sys/class/dmi/id/product_name` is readable, the generated `clients[].chassis.model_name` matches the file's contents; on a host where it is unreadable (restricted container), that field is an empty string and `run` still completes without error.
  2. The generated `clients[].networking[]` contains one entry per `(type, speed)` group of real interfaces, with `unit_count` equal to the number of interfaces in that group, and `lo`, `docker*`, `virbr*`, `veth*`, and bond-slave interfaces absent from the list.
  3. An interface in the `down` state (sysfs `speed: -1`) appears in `networking[]` with a recognizable sentinel value rather than being silently dropped or causing `run` to fail.
  4. On a host with at least one InfiniBand HCA present under `/sys/class/infiniband/`, at least one networking entry has `type: infiniband`.
  5. Quantity-grouping still collapses hosts that match on the new chassis/networking fingerprint into one `clients[]` stanza, and splits hosts that differ on `chassis.model_name` or networking signature into separate stanzas.

**Plans:** TBD

### Phase 4: Sysctl, Environment, and Drives Coverage

**Goal:** The auto-generated `systemname.yaml` also reports a curated sysctl snapshot, the relevant filtered environment, and an `lsblk`-sourced drive inventory — so a submitter who looks at the generated YAML sees a near-complete client description, with only the truly non-derivable fields left to fill in.
**Mode:** mvp
**Depends on:** Phase 3
**Requirements:** COLL-05, COLL-06, COLL-07
**Success Criteria** (what must be TRUE):

  1. The generated `clients[].sysctl[]` contains one `{name, value}` entry per `/proc/sys` key that matches the data-driven allowlist (`vm.dirty_*`, `net.core.*`, `net.ipv4.tcp_*`, `kernel.numa_balancing`); adding a new pattern to the allowlist file causes that key to appear in the next run's output without code changes.
  2. The generated `clients[].environment[]` contains the `AWS_*`, `BUCKET`, `STORAGE_*`, `OMPI_*`, `UCX_*`, and `NCCL_*` variables that are set at run time, with `AWS_SECRET_ACCESS_KEY` redacted as a length+sha256 fingerprint and `AWS_ACCESS_KEY_ID` rendered as a first-4/last-4 mask matching the policy in `storage_config.py`.
  3. On a host where `lsblk` is installed and reports at least one device, the generated `clients[].drives[]` contains one entry per `(vendor_name, model_name, interface, capacity_in_GB)` group with `unit_count` set to the group size, `capacity_in_GB` in base 10, and `interface` set to `nvme`/`sata`/`sas`/`other`.
  4. The generated drive entries do NOT contain `media_type`, `form_factor`, or `performance` (those remain blank for the submitter to fill from spec sheets per SER-02).
  5. On a host where `lsblk` is not installed or returns no devices, `clients[].drives` is omitted from the YAML and `run` still completes without error.

**Plans:** TBD

### Phase 5: Logical Diff Lifecycle + Capacity Gate

**Goal:** A submitter who re-runs the benchmark against an existing results-dir gets a hard failure if the client fleet has drifted from the previously recorded `systemname.yaml`, but their hand-filled blanks survive unchanged when nothing has drifted — and `datagen` refuses to start if the dataset destination doesn't have room.
**Mode:** mvp
**Depends on:** Phase 4
**Requirements:** LIFE-02, LIFE-03, LIFE-04, CAP-01
**Success Criteria** (what must be TRUE):

  1. After Phase 2-4 has written `<results-dir>/<mode>/<orgname>/systems/<systemname>.yaml` and a submitter has filled in `friendly_description`, `networking[].traffic`, and drive `media_type`/`form_factor`/`performance`, re-running the same `run` command against the same fleet completes without modifying the file and without raising drift errors — the submitter's hand-filled values survive.
  2. Re-running against a fleet where any collector-owned field has changed (e.g., a host swapped CPU SKU, network speed renegotiated, a sysctl key changed value) causes `run` to fail **before** DLIO/MPI launch with an error that lists each differing field by JSONPath-style path and shows the on-disk value alongside the in-memory value.
  3. The same drift-failure error message names the two remediation options for the submitter (rename to a new `--systemname` and re-run, generating a fresh one; or remove the existing yaml and re-run).
  4. The diff is per-mode: changes to the `closed` file do not trigger drift errors on a subsequent `open` run, and vice-versa.
  5. At `datagen` startup, when the computed dataset size exceeds the free space reported by `os.statvfs()` on the dataset destination directory (`--data-dir` for training, `--checkpoint-folder` for checkpointing, engine path for vectordb/kvcache), the benchmark fails before any data is written with a message stating the destination path, available bytes, required bytes, and the deficit; on multi-node runs, each rank checks its own destination so a single starved node fails fast.
  6. When free space is sufficient, `datagen` proceeds without printing or logging anything misleading about capacity — the gate is silent on the happy path.

**Plans:** TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4 → 5

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Canonical Layout & Init | 5/5 | Complete    | 2026-06-20 |
| 2. First-Run Write of Partial systemname.yaml | 4/5 | In progress | - |
| 3. Chassis Model + Networking Coverage | 0/TBD | Not started | - |
| 4. Sysctl, Environment, and Drives Coverage | 0/TBD | Not started | - |
| 5. Logical Diff Lifecycle + Capacity Gate | 0/TBD | Not started | - |
