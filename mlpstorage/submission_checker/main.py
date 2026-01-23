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
from checks.base import BaseCheck

# Import result exporter
from .results import ResultExporter

logging.basicConfig(level=logging.INFO)
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
    parser.add_argument(
        "--version",
        default="v5.1",
        choices=list(VERSIONS),
        help="mlperf version",
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

    config = Config()
    
    loader = Loader(args.input, args.version)
    exporter = ResultExporter(args.csv, config)

    results = {}
    systems = {}
    # Main loop over all the submissions
    for logs in loader.load():
        # TODO: Initialize checkers

        # TODO: Run checks
        valid = True

        # TODO: Add results to summary
        if valid:
            exporter.add_result(logs)
    
    # Export results
    exporter.export()

    # TODO: Output result summary to console


