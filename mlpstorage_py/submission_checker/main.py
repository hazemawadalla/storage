import argparse
import logging
import os
import sys

# Constants
from .constants import *

# Import config
from .configuration.configuration import Config

# Import loader
from .loader import Loader

# Import checkers
from .checks.checkpointing_checks import CheckpointingCheck
from .checks.directory_checks import DirectoryCheck
from .checks.kvcache_checks import KVCacheCheck
from .checks.submission_structure_checks import SubmissionStructureCheck
from .checks.system_yaml_schema_checks import SystemYamlSchemaCheck
from .checks.training_checks import TrainingCheck
from .checks.vdb_checks import VdbCheck


# Import result exporter
from .results import ResultExporter

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s %(filename)s:%(lineno)d %(levelname)s] %(message)s"
)
log = logging.getLogger("main")

def get_args():
    """Parse command-line arguments for the submission checker.

    Sets up an ArgumentParser with options for input directory, version,
    filtering, output files, and various skip flags for different checks.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="submission directory")
    parser.add_argument("--submitters", help="Comma separated submitters to run the checker")
    parser.add_argument(
        "--version",
        default="v5.1",
        choices=list(VERSIONS),
        help="mlperf version",
    )
    parser.add_argument(
        "--csv",
        default="summary.csv",
        help="csv file with results")
    parser.add_argument(
        "--skip-output-file",
        action="store_true",
        help="Skip check output file"
    )
    parser.add_argument(
        "--reference-checksum",
        default=None,
        help="MD5 checksum for the code/ tree (overrides REFERENCE_CHECKSUMS)",
    )
    args = parser.parse_args()
    return args

def main():
    """Run the MLPerf submission checker on the provided directory.

    Parses arguments, initializes configuration and loader, iterates
    through all submissions, runs validation checks (performance,
    accuracy, system, measurements, power), collects results, and
    exports summaries. Logs pass/fail status and statistics.

    Returns:
        int: 0 if all submissions pass checks, 1 if any errors found.
    """
    args = get_args()

    # When --submitters is not supplied, pass None (not ["None"]) to Config so
    # Config.check_submitter returns True for every submitter (the documented
    # "match all" default). The previous str(None).split(",") produced ["None"]
    # which silently filtered out every real submitter, leaving the loader loop
    # empty — a pre-existing bug surfaced by the Phase-3 Definition-of-Done test.
    #
    # Per WR-06 + WR-07 (review 2026-06-10): strip whitespace from each CSV
    # token and drop empty-after-strip entries. `--submitters "Acme, BetaCo"`
    # previously produced `["Acme", " BetaCo"]` and the leading space silently
    # filtered out BetaCo. Likewise `--submitters ""` and `--submitters " "`
    # now route to None (match-all) consistently rather than producing `[""]`
    # which `Config.check_submitter` would reject for every submitter.
    if args.submitters:
        submitters = [s.strip() for s in args.submitters.split(",") if s.strip()]
        if not submitters:
            submitters = None
    else:
        submitters = None
    config = Config(
        version=args.version,
        submitters=submitters,
        skip_output_file=args.skip_output_file,
        reference_checksum_override=args.reference_checksum,
    )

    loader = Loader(args.input, args.version, config)
    exporter = ResultExporter(args.csv, config)


    results = {}
    systems = {}
    errors = []

    # Per CR-02 (review 2026-06-10): the checker list is mode-routed so that
    # each per-mode Check class only runs against submissions whose loader
    # mode matches. The previous flat list ran every checker against every
    # submission. That was benign while VdbCheck/KVCacheCheck were no-op
    # stubs, but locked in a regression for the next contributor — the first
    # real @rule-decorated method added to VdbCheck would fire against
    # training/checkpointing submissions and emit a §5 rule_id bound to the
    # wrong submission. Gating at the loop keeps the stubs (and any future
    # real checks) on the right submissions without each rule method needing
    # to guard with `if self.mode != ...`.
    MODE_TO_CHECKERS = {
        "training":      [DirectoryCheck, TrainingCheck],
        "checkpointing": [DirectoryCheck, CheckpointingCheck],
        "vectordb":      [VdbCheck],
        "kvcache":       [KVCacheCheck],
    }

    # Per PLAN.md 01-03 D-02: run structural hierarchy checks ONCE before the
    # per-benchmark loader loop. Failures are accumulated into `errors` but do
    # NOT short-circuit the loop — every benchmark still gets its own checks.
    structure_check = SubmissionStructureCheck(log, config, args.input)
    if not structure_check():
        errors.append(args.input)

    # Per Phase 2 D-A1: schema-validate every systems/<name>.yaml ONCE before
    # the per-benchmark loader loop. Runs after SubmissionStructureCheck and
    # before the for-loop so schema errors surface once per YAML (not once per
    # workload). Failures accumulated into `errors` but do NOT abort the loop.
    schema_check = SystemYamlSchemaCheck(log, config, args.input)
    if not schema_check():
        errors.append(args.input)

    # Main loop over all the submissions
    for logs in loader.load():
        mode = getattr(logs.loader_metadata, "mode", None)
        checkers = MODE_TO_CHECKERS.get(mode, [])
        if not checkers:
            log.warning(
                "No checkers registered for mode=%r at %s; skipping",
                mode, logs.loader_metadata.folder,
            )
        valid = True
        for checker in checkers:
            valid &= checker(log, config, logs)()

        # TODO: Add results to summary
        if valid:
            exporter.add_result(logs)
        else:
            errors.append(logs.loader_metadata.folder)
    
    # Export results
    exporter.export()

    if len(errors) > 0:
        log.error("SUMMARY: submission has errors")
        return 1
    else:
        log.info("SUMMARY: submission looks OK")
        return 0

if __name__ == "__main__":
    sys.exit(main())