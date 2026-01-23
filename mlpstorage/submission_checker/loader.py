
import os
from typing import Generator, Literal
from .utils import *
from .constants import *
import logging
from dataclasses import dataclass

@dataclass
class SubmissionLogs:
    """Container for parsed submission log artifacts and metadata.

    The `SubmissionLogs` class holds references to parsed log files and
    associated metadata for a single submission. It serves as a data
    transfer object passed between loading and validation phases.
    """
    datagen_files: dict
    run_files: dict
    system_file: dict

class Loader:
    """Loads and parses submission artifacts from the filesystem.

    The `Loader` class traverses the submission directory structure,
    identifies valid submissions, and parses their log files and metadata.
    It yields `SubmissionLogs` objects for each valid submission found,
    handling version-specific path formats and optional artifacts.
    """
    def __init__(self, root, version) -> None:
        """Initialize the submission loader.

        Sets up path templates based on the MLPerf version and root
        directory.

        Args:
            root (str): Root directory containing submissions.
            version (str): MLPerf version for path resolution.
        """
        self.root = root
        self.version = version
        self.logger = logging.getLogger("Loader")
        self.system_log_path = os.path.join(
            self.root, SYSTEM_PATH.get(
                version, SYSTEM_PATH["default"]))

    def load_single_log(self, path, log_type):
        pass

    def load_datagen_files(self):
        pass

    def load_run_files(self):
        pass

    def load(self) -> Generator[SubmissionLogs, None, None]:
        # Iterate over submission folder.
        # Division -> submitter -> system -> benchmark -> runs
        for division in list_dir(self.root):
            if division not in VALID_DIVISIONS:
                continue
            division_path = os.path.join(self.root, division)
            for submitter in list_dir(division_path):
                results_path = os.path.join(
                    division_path, submitter, "results")
                for system in list_dir(results_path):
                    system_path = os.path.join(results_path, system)
                    system_file_path = self.system_log_path.format(division = division, submitter = submitter, system = system)
                    system_file = self.load_single_log(system_file_path, "System")
                    for benchmark in list_dir(system_path):
                        datagen_path = os.path.join(system_path, "datagen")
                        run_path = os.path.join(system_path, "run")
                        datagen_files_agg = {}
                        run_files_agg = {}
                        for timestamp in datagen_path:
                            timestamp_path = os.path.join(datagen_path, timestamp)
                            datagen_files = self.load_datagen_files()

                        for timestamp in run_path:
                            run_path = os.path.join(datagen_path, timestamp)
                            run_files = self.load_run_files()

                        yield SubmissionLogs(datagen_files_agg, run_files_agg, system_file)
                        



