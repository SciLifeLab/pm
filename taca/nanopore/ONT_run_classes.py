import csv
import glob
import json
import logging
import os
import re
import shutil
import subprocess
from datetime import datetime
from typing import Union

import pandas as pd

from taca.utils.config import CONFIG
from taca.utils.statusdb import NanoporeRunsConnection
from taca.utils.transfer import RsyncAgent, RsyncError

logger = logging.getLogger(__name__)

ONT_RUN_PATTERN = re.compile(
    r"^(\d{8})_(\d{4})_([0-9a-zA-Z]+)_([0-9a-zA-Z]+)_([0-9a-zA-Z]+)$"
)


class ONT_run:
    """General Nanopore run.

    Expects instantiation from absolute path of run directory on preprocessing server.
    """

    def __init__(self, run_abspath: str):
        # Get paths and names of MinKNOW experiment, sample and run
        self.run_name = os.path.basename(run_abspath)
        self.run_abspath = run_abspath

        self.run_type: str | None = (
            None  # This will be defined upon instantiation of a child class
        )

        assert re.match(
            ONT_RUN_PATTERN, self.run_name
        ), f"Run {self.run_name} doesn't look like a run dir"

        # Parse MinKNOW sample and experiment name
        with open(self.get_file("/run_path.txt")) as stream:
            self.experiment_name, self.sample_name, _ = stream.read().split("/")

        # Get info from run name
        (
            self.date,
            self.time,
            self.position,
            self.flowcell_id,
            self.run_hash,
        ) = self.run_name.split("_")

        # Get instrument
        # - For PromethION, the run position will be one of "1A", "2A", ..., "3G".
        # - For MinION, the position will be the instrument ID e.g. "MN19414".
        self.instrument = "promethion" if len(self.position) == 2 else "minion"

        # Get attributes from config
        self.minknow_reports_dir = CONFIG["nanopore_analysis"]["minknow_reports_dir"]
        self.toulligqc_reports_dir = CONFIG["nanopore_analysis"][
            "toulligqc_reports_dir"
        ]
        self.toulligqc_executable = CONFIG["nanopore_analysis"]["toulligqc_executable"]
        self.analysis_server = CONFIG["nanopore_analysis"].get("analysis_server", None)
        self.rsync_options = CONFIG["nanopore_analysis"]["rsync_options"]
        for k, v in self.rsync_options.items():
            if v == "None":
                self.rsync_options[k] = None

        # Get DB
        self.db = NanoporeRunsConnection(CONFIG["statusdb"], dbname="nanopore_runs")

    # Looking for files within the run dir

    def has_file(self, content_pattern: str) -> bool:
        """Checks within run dir for pattern, e.g. '/report*.json', returns bool."""
        query_path = self.run_abspath + content_pattern
        query_glob = glob.glob(query_path)

        if len(query_glob) > 0:
            return True
        else:
            return False

    def get_file(self, content_pattern) -> str:
        """Checks within run dir for pattern, e.g. '/report*.json', returns file abspath as string."""
        query_path = self.run_abspath + content_pattern
        query_glob = glob.glob(query_path)

        if len(query_glob) == 1:
            return query_glob[0]
        elif len(query_glob) == 0:
            raise AssertionError(f"Could not find {query_path}")
        else:
            raise AssertionError(f"Found multiple instances of {query_path}")

    # Evaluating run status

    def is_synced(self) -> bool:
        return self.has_file("/.sync_finished")

    def assert_contents(self):
        """Checklist function to assure run has all files necessary to proceed with processing"""

        # Completion indicators
        assert self.has_file("/.sync_finished")
        assert self.has_file("/final_summary*.txt")

        # Raw seq files
        assert any(
            [
                dir in os.listdir(self.run_abspath)
                for dir in ["pod5", "pod5_pass", "fast5", "fast5_pass"]
            ]
        )

        # NGI files from instrument
        assert self.has_file("/pore_count_history.csv")
        assert self.has_file("/run_path.txt")

        # MinKNOW reports
        assert self.has_file("/report_*.json")
        assert self.has_file("/report_*.html")

        # MinKNOW auxillary files
        assert self.has_file("/pore_activity*.csv")

    def touch_db_entry(self):
        """Check run vs statusdb. Create entry if there is none."""

        if not self.db.check_run_exists(self):
            logger.info(
                f"{self.run_name}: Run does not exist in the database, creating entry for ongoing run."
            )

            run_path_file = os.path.join(self.run_abspath, "run_path.txt")
            assert os.path.isfile(run_path_file), f"Couldn't find {run_path_file}"

            pore_count_history_file = os.path.join(
                self.run_abspath, "pore_count_history.csv"
            )
            assert os.path.isfile(
                pore_count_history_file
            ), f"Couldn't find {pore_count_history_file}"

            self.db.create_ongoing_run(self, run_path_file, pore_count_history_file)
            logger.info(
                f"{self.run_name}: Successfully created database entry for ongoing run."
            )
        else:
            logger.info(f"{self.run_name}: Database entry already exists, skipping.")

    def update_db_entry(self, force_update=False):
        """Check run vs statusdb. Create or update run entry."""

        # If no run document exists in the database, create an ongoing run document
        self.touch_db_entry()

        # If the run document is marked as "ongoing" or database is being manually updated
        if self.db.check_run_status(self) == "ongoing" or force_update is True:
            logger.info(
                f"{self.run_name}: Run exists in the database with run status: {self.db.check_run_status(self)}."
            )

            logger.info(f"{self.run_name}: Updating...")

            # Instantiate json (dict) to update the db with
            db_update = {}

            # Parse run path
            db_update["run_path"] = (
                open(f"{self.run_abspath}/run_path.txt").read().strip()
            )

            # Parse pore counts
            pore_counts = []
            with open(f"{self.run_abspath}/pore_count_history.csv") as stream:
                for line in csv.DictReader(stream):
                    pore_counts.append(line)
            db_update["pore_count_history"] = pore_counts

            # Parse report_*.json
            self.parse_minknow_json(db_update)

            # Parse pore_activity_*.csv
            self.parse_pore_activity(db_update)

            # Update the DB entry
            self.db.finish_ongoing_run(self, db_update)

        # If the run document is marked as "finished"
        elif self.db.check_run_status(self) == "finished":
            logger.info(
                f"Run {self.run_name} exists in the database as an finished run, do nothing."
            )

    def parse_pore_activity(self, db_update):
        logger.info(f"{self.run_name}: Parsing pore activity...")

        pore_activity = {}

        # Use pandas to pivot the data into a more manipulable dataframe
        df = pd.read_csv(self.get_file("/pore_activity_*.csv"))
        df.sort_values(by="Experiment Time (minutes)", inplace=True)
        df = df.pivot_table(
            "State Time (samples)", "Experiment Time (minutes)", "Channel State"
        )

        # Use pore counts to calculate new metrics
        df["all"] = df.sum(axis=1)
        df["healthy"] = df.strand + df.adapter + df.pore
        df["productive"] = df.strand + df.adapter

        df["health"] = df["healthy"] / df["all"]
        df["efficacy"] = df["productive"] / df["healthy"]

        # Look at peaks within 1st hour of the run and define some metrics
        df_h1 = df[0:60]
        pore_activity["peak_pore_health_pc"] = round(
            100
            * float(df_h1.loc[df_h1.health == df_h1.health.max(), "health"].values[0]),
            2,
        )
        if not all(df["efficacy"].isna()):
            pore_activity["peak_pore_efficacy_pc"] = round(
                100
                * float(
                    df_h1.loc[
                        df_h1.efficacy == df_h1.efficacy.max(), "efficacy"
                    ].values[0]
                ),
                2,
            )
        else:
            pore_activity["peak_pore_efficacy_pc"] = None

        # Calculate the T90
        # -- Get the cumulative sum of all productive pores
        df["cum_productive"] = df["productive"].cumsum()
        # -- Find the timepoint (h) at which the cumulative sum >= 90% of the absolute sum
        if not df["productive"].sum() == 0:
            t90_min = df[df["cum_productive"] >= 0.9 * df["productive"].sum()].index[0]
            pore_activity["t90_h"] = round(t90_min / 60, 1)
        else:
            pore_activity["t90_h"] = None

        # Add to the db update
        db_update["pore_activity"] = pore_activity

    def parse_minknow_json(self, db_update):
        """Parse useful stuff from the MinKNOW .json report to add to CouchDB"""

        logger.info(f"{self.run_name}: Parsing report JSON...")

        dict_json_report = json.load(open(self.get_file("/report*.json")))

        # Initialize return dict
        parsed_data = {}

        # These sections of the .json can be added as they are
        for section in [
            "host",
            "protocol_run_info",
            "user_messages",
        ]:
            parsed_data[section] = dict_json_report[section]

        # Only parse the last acquisition section, which contains the actual sequencing data
        seq_metadata = dict_json_report["acquisitions"][-1]
        seq_metadata_trimmed = {}

        # -- Run info subsection
        seq_metadata_trimmed["acquisition_run_info"] = {}

        seq_metadata_trimmed["acquisition_run_info"]["yield_summary"] = seq_metadata[
            "acquisition_run_info"
        ]["yield_summary"]

        # -- Run output subsection
        seq_metadata_trimmed["acquisition_output"] = []
        for section in seq_metadata["acquisition_output"]:
            if "type" not in section.keys() or section["type"] in [
                "AllData",
                "SplitByBarcode",
            ]:
                seq_metadata_trimmed["acquisition_output"].append(section)

        # -- Read length subseqtion
        seq_metadata_trimmed["read_length_histogram"] = seq_metadata[
            "read_length_histogram"
        ]

        # Add the trimmed acquisition section to the parsed data
        parsed_data["acquisitions"] = []
        parsed_data["acquisitions"].append(seq_metadata_trimmed)

        # Add the parsed data to the db update
        db_update.update(parsed_data)

    # Transferring metadata

    def copy_metadata(self):
        """Copies run dir (excluding seq data) to metadata dir"""

        exclude_patterns = [
            # Main seq dirs
            "**/bam*/***",
            "**/fast5*/***",
            "**/fastq*/***",
            "**/pod5*/***",
            # Any files found elsewhere
            "*.bam*",
            "*.bai*",
            "*.fast5*",
            "*.fastq*",
            "*.pod5*",
        ]

        exclude_patterns_quoted = ["'" + pattern + "'" for pattern in exclude_patterns]

        src = self.run_abspath
        dst = self.transfer_details["metadata_dir"]

        os.system(
            f"rsync -rv --exclude={{{','.join(exclude_patterns_quoted)}}} {src} {dst}"
        )

    def copy_html_report(self):
        logger.info(f"{self.run_name}: Transferring .html report to ngi-internal...")

        # Transfer the MinKNOW .html report file to ngi-internal, renaming it to the full run ID. Requires password-free SSH access.
        report_src_path = self.get_file("/report*.html")
        report_dest_path = os.path.join(
            self.minknow_reports_dir,
            f"report_{self.run_name}.html",
        )
        transfer_object = RsyncAgent(
            src_path=report_src_path,
            dest_path=report_dest_path,
            validate=False,
        )
        try:
            transfer_object.transfer()
        except RsyncError:
            msg = f"{self.run_name}: An error occurred while attempting to transfer the report {report_src_path} to {report_dest_path}."
            logger.error(msg)
            raise RsyncError(msg)

    def toulligqc_report(self):
        """Generate a QC report for the run using ToulligQC and publish it to GenStat."""

        report_dir_name = "toulligqc_report"

        # Do not run this function if it's output dir exists in the run dir
        if os.path.exists(f"{self.run_abspath}/{report_dir_name}"):
            logging.info(
                f"{self.run_name}: ToulligQC report dir already exists, skipping."
            )
            return None

        # Get sequencing summary file
        glob_summary = glob.glob(f"{self.run_abspath}/sequencing_summary*.txt")
        assert len(glob_summary) == 1, f"Found {len(glob_summary)} summary files"
        summary = glob_summary[0]

        # Determine the format of the raw sequencing data, sorted by preference
        raw_data_dir_options = [
            "pod5_pass",
            "pod5",
            "fast5_pass",
            "fast5",
        ]
        raw_data_dir = None
        for raw_data_dir_option in raw_data_dir_options:
            if os.path.exists(f"{self.run_abspath}/{raw_data_dir_option}"):
                raw_data_dir = raw_data_dir_option
                break
        if raw_data_dir is None:
            raise AssertionError(f"No seq data found in {self.run_abspath}")
        raw_data_format = "pod5" if "pod5" in raw_data_dir else "fast5"

        # Load samplesheet, if any
        ss_glob = glob.glob(f"{self.run_abspath}/sample_sheet*.csv")
        if len(ss_glob) == 0:
            samplesheet = None
        elif len(ss_glob) > 1:
            # If multiple samplesheet, use latest one
            samplesheet = ss_glob.sort()[-1]
        else:
            samplesheet = ss_glob[0]

        # Run has barcode subdirs
        barcode_dirs_glob = glob.glob(f"{self.run_abspath}/{raw_data_dir}/barcode*")
        if len(barcode_dirs_glob) > 0:
            barcode_dirs = True
            raw_data_path = barcode_dirs_glob[0]
        else:
            barcode_dirs = False
            raw_data_path = f"{self.run_abspath}/{raw_data_dir}"

        # Determine barcodes
        if samplesheet:
            ss_df = pd.read_csv(samplesheet)
            if "barcode" in ss_df.columns:
                ss_barcodes = list(ss_df["barcode"].unique())
                ss_barcodes.sort()
                barcode_nums = [int(bc[-2:]) for bc in ss_barcodes]
                # If barcodes are numbered sequentially, write arg as range
                if barcode_nums == list(range(1, len(barcode_nums) + 1)):
                    barcodes_arg = f"{ss_barcodes[0]}:{ss_barcodes[-1]}"
                else:
                    barcodes_arg = ":".join(ss_barcodes)
            else:
                ss_barcodes = None

        command_args = {
            "--sequencing-summary-source": summary,
            f"--{raw_data_format}-source": raw_data_path,
            "--output-directory": self.run_abspath,
            "--report-name": report_dir_name,
        }
        if barcode_dirs:
            command_args["--barcoding"] = ""
            if samplesheet and ss_barcodes:
                command_args["--samplesheet"] = samplesheet
                command_args["--barcodes"] = barcodes_arg

        # Build command list
        command_list = [self.toulligqc_executable]
        for k, v in command_args.items():
            command_list.append(k)
            if v:
                command_list.append(v)

        # Run the command
        # Small enough to wait for, should be done in 1-5 minutes
        process = subprocess.run(command_list)

        # Check if the command was successful
        if process.returncode == 0:
            logging.info(f"{self.run_name}: ToulligQC report generated successfully.")
        else:
            raise subprocess.CalledProcessError(process.returncode, command_list)

        # Copy the report to GenStat

        logger.info(
            f"{self.run_name}: Transferring ToulligQC report to ngi-internal..."
        )
        # Transfer the ToulligQC .html report file to ngi-internal, renaming it to the full run ID. Requires password-free SSH access.
        report_src_path = self.get_file(f"/{report_dir_name}/report.html")
        report_dest_path = os.path.join(
            self.toulligqc_reports_dir,
            f"report_{self.run_name}.html",
        )
        transfer_object = RsyncAgent(
            src_path=report_src_path,
            dest_path=report_dest_path,
            validate=False,
        )
        try:
            transfer_object.transfer()
        except RsyncError:
            msg = f"{self.run_name}: An error occurred while attempting to transfer the report {report_src_path} to {report_dest_path}."
            logger.error(msg)
            raise RsyncError(msg)

    # Transfer run

    def transfer_run(self):
        """Transfer dir to destination specified in config file via rsync"""
        destination = self.transfer_details["destination"]

        logger.info(
            f"{self.run_name}: Transferring to {self.analysis_server['host'] if self.analysis_server else destination}..."
        )

        transfer_object = RsyncAgent(
            self.run_abspath,
            dest_path=destination,
            remote_host=self.analysis_server["host"] if self.analysis_server else None,
            remote_user=self.analysis_server["user"] if self.analysis_server else None,
            validate=False,
            opts=self.rsync_options,
        )

        try:
            transfer_object.transfer()
        except RsyncError:
            msg = f"{self.run_name}: An error occurred while transferring to the analysis server."
            logger.error(msg)
            raise RsyncError(msg)

    def update_transfer_log(self):
        """Update transfer log with run id and date."""
        try:
            with open(self.transfer_details["transfer_log"], "a") as f:
                tsv_writer = csv.writer(f, delimiter="\t")
                tsv_writer.writerow([self.run_name, str(datetime.now())])
        except OSError:
            msg = f"{self.run_name}: Could not update the transfer logfile {self.transfer_details['transfer_log']}"
            logger.error(msg)
            raise OSError(msg)

    # Archive run

    def archive_run(self):
        """Move directory to nosync."""
        src = self.run_abspath
        dst = os.path.join(self.run_abspath, os.pardir, "nosync")

        shutil.move(src, dst)


class ONT_user_run(ONT_run):
    """ONT user run, has class methods and attributes specific to user runs."""

    def __init__(self, run_abspath: str):
        super().__init__(run_abspath)
        self.run_type = "user_run"
        self.transfer_details = CONFIG["nanopore_analysis"]["run_types"][self.run_type][
            "instruments"
        ][self.instrument]

    def is_transferred(self) -> bool:
        """Return True if run ID in transfer.tsv, else False."""
        with open(self.transfer_details["transfer_log"]) as f:
            return self.run_name in f.read()


class ONT_qc_run(ONT_run):
    """ONT QC run, has class methods and attributes specific to QC runs"""

    def __init__(self, run_abspath: str):
        super().__init__(run_abspath)
        self.run_type = "qc_run"
        self.transfer_details = CONFIG["nanopore_analysis"]["run_types"][self.run_type][
            "instruments"
        ][self.instrument]

        # Get Anglerfish attributes from run
        self.anglerfish_done_abspath = f"{self.run_abspath}/.anglerfish_done"
        self.anglerfish_ongoing_abspath = f"{self.run_abspath}/.anglerfish_ongoing"

        # Get Anglerfish attributes from config
        self.anglerfish_config = CONFIG["nanopore_analysis"]["run_types"][
            self.run_type
        ]["anglerfish"]

        self.anglerfish_samplesheets_dir = self.anglerfish_config[
            "anglerfish_samplesheets_dir"
        ]
        self.anglerfish_path = self.anglerfish_config["anglerfish_path"]

    def is_transferred(self) -> bool:
        """Return True if run ID in transfer.tsv, else False."""
        with open(self.transfer_details["transfer_log"]) as f:
            return self.run_name in f.read()

    # QC methods

    def get_anglerfish_exit_code(self) -> Union[int, None]:
        """Check whether Anglerfish has finished.

        Return exit code or None.
        """
        if os.path.exists(self.anglerfish_done_abspath):
            return int(open(self.anglerfish_done_abspath).read())
        else:
            return None

    def get_anglerfish_pid(self) -> Union[str, None]:
        """Check whether Anglerfish is ongoing.

        Return process ID or None."""
        if os.path.exists(self.anglerfish_ongoing_abspath):
            return str(open(self.anglerfish_ongoing_abspath).read())
        else:
            return None

    def fetch_anglerfish_samplesheet(self) -> bool:
        """Fetch Anglerfish samplesheet belonging to the run from where it was
        dumped by LIMS and put it in the run directory.

        a) If file(s) is available, copy to run folder. On success, return True and
        add new samplesheet abspath as run object attribute.

        b) If the file is not yet available, return False.
        """

        # Define query pattern
        expected_file_pattern = f"*{self.run_name}*.csv"
        pattern_abspath = os.path.join(
            self.anglerfish_samplesheets_dir, "*", expected_file_pattern
        )

        glob_results = glob.glob(pattern_abspath)

        if len(glob_results) == 0:
            return False

        else:
            # Sort by ascending date
            glob_results.sort()

            # Grab abspath of latest samplesheet
            src = glob_results[-1]
            dst = os.path.join(self.run_abspath, os.path.basename(src))

            # Copy into run directory
            if os.system(f"rsync -v {src} {dst}") == 0:
                self.anglerfish_samplesheet = dst
                return True
            else:
                raise RsyncError(
                    f"{self.run_name}: Error occurred when copying anglerfish samplesheet to run dir."
                )

    def has_fastq_output(self) -> bool:
        """Check whether run has fastq output."""

        reads_dir = os.path.join(self.run_abspath, "fastq_pass")

        return os.path.exists(reads_dir)

    def has_raw_seq_output(self) -> bool:
        """Check whether run has sequencing data output."""

        raw_seq_dirs = ["pod5", "pod5_pass", "fast5", "fast5_pass"]

        for dir in raw_seq_dirs:
            if os.path.exists(os.path.join(self.run_abspath, dir)):
                return True

        return False

    def has_barcode_dirs(self) -> bool:
        barcode_dir_pattern = r"barcode\d{2}"

        for dir in os.listdir(os.path.join(self.run_abspath, "fastq_pass")):
            if re.search(barcode_dir_pattern, dir):
                return True

        return False

    def run_anglerfish(self):
        """Run Anglerfish as subprocess within it's own Conda environment.
        Dump files to indicate ongoing and finished processes.
        """

        timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")

        # "anglerfish_run*" is the dir pattern recognized by the LIMS script parsing the results
        anglerfish_run_name = "anglerfish_run"

        n_threads = 2  # This could possibly be changed

        anglerfish_command = [
            self.anglerfish_path,
            "run",
            f"--samplesheet {self.anglerfish_samplesheet}",
            f"--out_fastq {self.run_abspath}",
            f"--run_name {anglerfish_run_name}",
            f"--threads {n_threads}",
            "--max_distance 1",
            "--lenient",
            "--skip_demux",
        ]
        if self.has_barcode_dirs():
            anglerfish_command.append("--barcoding")

        # Create dir to trace TACA executing Anglerfish as a subprocess
        taca_anglerfish_run_dir = f"taca_anglerfish_run_{timestamp}"
        taca_anglerfish_run_dir_abspath = (
            f"{self.run_abspath}/{taca_anglerfish_run_dir}"
        )
        os.mkdir(taca_anglerfish_run_dir_abspath)
        # Copy samplesheet used for traceability
        shutil.copy(self.anglerfish_samplesheet, taca_anglerfish_run_dir_abspath)
        # Create files to dump subprocess std
        stderr_abspath = f"{taca_anglerfish_run_dir_abspath}/stderr.txt"

        full_command = [
            # Dump subprocess PID into 'run-ongoing'-indicator file.
            f"echo $$ > {self.anglerfish_ongoing_abspath}",
            # Run Anglerfish in its own environment
            "conda run -n anglerfish " + " ".join(anglerfish_command),
            # Dump Anglerfish exit code into file
            f"echo $? > {self.anglerfish_done_abspath}",
            # Move run to subdir
            #  1) Find the latest Anglerfish run dir (younger than the 'run-ongoing' file)
            f'find {self.run_abspath} -name "anglerfish_run*" -type d -newer {self.run_abspath}/.anglerfish_ongoing '
            #  2) Move the Anglerfish run dir into the TACA Anglerfish run dir
            + r"-exec mv \{\} "
            + rf"{self.run_abspath}/{taca_anglerfish_run_dir}/ \; "
            #  3) Only do this once
            + "-quit",
            # Remove 'run-ongoing' file.
            f"rm {self.anglerfish_ongoing_abspath}",
        ]

        with open(f"{taca_anglerfish_run_dir_abspath}/command.sh", "w") as stream:
            stream.write("\n".join(full_command))

        # Start Anglerfish subprocess
        with open(stderr_abspath, "w") as stderr:
            process = subprocess.Popen(
                f"bash {taca_anglerfish_run_dir}/command.sh",
                shell=True,
                cwd=self.run_abspath,
                stderr=stderr,
            )
        logger.info(
            f"{self.run_name}: Anglerfish subprocess started with process ID {process.pid}."
        )
