"""This is a stand-alone script run on ONT instrument computers. It transfers new ONT runs to NAS using rsync."""

__version__ = "1.0.14"

import argparse
import logging
import os
import re
import shutil
import subprocess
from datetime import datetime as dt
from glob import glob

RUN_PATTERN = re.compile(
    # Run folder name expected as yyyymmdd_HHMM_positionOrInstrument_flowCellId_randomHash
    # Flow cell names starting with "CTC" are configuration test cells and should not be included
    r"^\d{8}_\d{4}_(([1-3][A-H])|(MN19414))_(?!CTC)[A-Za-z0-9]+_[A-Za-z0-9]+$"
)


def main(args):
    """Find ONT runs and transfer them to storage.
    Archives the run when the transfer is complete."""

    logging.basicConfig(
        filename=args.log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logging.info("Starting script...")

    rsync_log = os.path.join(args.source_dir, "rsync_log.txt")

    logging.info("Parsing instrument position logs...")
    position_logs = parse_position_logs(args.minknow_logs_dir)
    logging.info("Subsetting QC and MUX metrics...")
    pore_counts = get_pore_counts(position_logs)

    handle_runs(pore_counts, args, rsync_log)
    delete_archived_runs(args)


def handle_runs(pore_counts, args, rsync_log):
    logging.info("Finding runs...")
    # Look for dirs matching run pattern 3 levels deep from source, excluding certain dirs
    exclude_dirs = ["nosync", "keep_data", "cg_data"]
    run_paths = [
        path
        for path in glob(os.path.join(args.source_dir, "*", "*", "*"), recursive=True)
        if re.match(RUN_PATTERN, os.path.basename(path))
        and path.split(os.sep)[-3] not in exclude_dirs
    ]
    logging.info(f"Found {len(run_paths)} runs...")

    # Iterate over runs
    for run_path in run_paths:
        logging.info(f"Handling {run_path}...")

        experiment_name = run_path.split(os.sep)[-3]
        sample_name = run_path.split(os.sep)[-2]
        if sample_name[0:3] == "QC_" or experiment_name[0:3] == "QC_":
            logging.info("Run categorized as QC.")
            rsync_dest = args.dest_dir_qc
        else:
            rsync_dest = args.dest_dir

        logging.info("Dumping run path...")
        dump_path(run_path)
        logging.info("Dumping QC and MUX history...")
        dump_pore_count_history(run_path, pore_counts)

        if not sequencing_finished(run_path):
            sync_to_storage(run_path, rsync_dest, rsync_log)
        else:
            final_sync_to_storage(run_path, rsync_dest, args.archive_dir, rsync_log)


def delete_archived_runs(args):
    logging.info("Finding locally archived runs...")
    # Look for dirs matching run pattern inside archive dir
    run_paths = [
        path
        for path in glob(os.path.join(args.archive_dir, "*", "*", "*"), recursive=True)
        if re.match(RUN_PATTERN, os.path.basename(path))
    ]
    logging.info(f"Found {len(run_paths)} locally archived runs...")

    preproc_archive_contents = set(
        os.listdir(os.path.join(args.dest_dir, "nosync"))
        + os.listdir(os.path.join(args.dest_dir, "nosync", "archived"))
    )
    # Iterate over runs
    for run_path in run_paths:
        logging.info(f"Handling locally archived run {run_path}...")
        run_name = os.path.basename(run_path)

        if run_name in preproc_archive_contents:
            logging.info(
                f"Locally archived run {run_path} was found in the preproc archive. Deleting..."
            )
            shutil.rmtree(run_path)
        else:
            logging.info(
                f"Locally archived run {run_path} was not found in the preproc archive. Skipping..."
            )
            continue

    # Remove empty dirs
    sample_dirs = set([os.path.dirname(run_path) for run_path in run_paths])
    experiment_dirs = set([os.path.dirname(sample_dir) for sample_dir in sample_dirs])

    for sample_dir in sample_dirs:
        if not os.listdir(sample_dir):
            logging.info(f"Removing empty dir '{sample_dir}'.")
            os.rmdir(sample_dir)

    # Remove empty experiment dirs
    for experiment_dir in experiment_dirs:
        if not os.listdir(experiment_dir):
            logging.info(f"Removing empty dir '{experiment_dir}'.")
            os.rmdir(experiment_dir)


def sequencing_finished(run_path: str) -> bool:
    sequencing_finished_indicator = "final_summary"
    run_dir_content = os.listdir(run_path)
    for item in run_dir_content:
        if sequencing_finished_indicator in item:
            return True
    return False


def dump_path(run_path: str):
    """Dump path <minknow_experiment_id>/<minknow_sample_id>/<minknow_run_id>
    to a file. Used for transferring info on ongoing runs to StatusDB."""
    new_file = os.path.join(run_path, "run_path.txt")
    proj, sample, run = run_path.split(os.sep)[-3:]
    path_to_write = os.path.join(proj, sample, run)
    with open(new_file, "w") as f:
        f.write(path_to_write)
    return path_to_write


def write_finished_indicator(run_path):
    """Write a hidden file to indicate
    when the finial rsync is finished."""
    finished_indicator = ".sync_finished"
    new_file_path = os.path.join(run_path, finished_indicator)
    open(new_file_path, "w").close()
    return new_file_path


def sync_to_storage(run_dir: str, destination: str, rsync_log: str):
    """Sync the run to storage using rsync.
    Skip if rsync is already running on the run."""

    command = [
        "run-one",
        "rsync",
        "-rvu",
        "--log-file=" + rsync_log,
        run_dir,
        destination,
    ]

    p = subprocess.Popen(command)
    logging.info(
        f"Initiated rsync with PID {p.pid} and the following command: {command}"
    )


def final_sync_to_storage(
    run_dir: str, destination: str, archive_dir: str, rsync_log: str
):
    """Do a final sync of the run to storage, then archive it.
    Skip if rsync is already running on the run."""

    logging.info(f"Performing a final sync of {run_dir} to storage")

    command = [
        "run-one",
        "rsync",
        "-rvu",
        "--log-file=" + rsync_log,
        run_dir,
        destination,
    ]

    p = subprocess.run(command)

    if p.returncode == 0:
        finished_indicator_path = write_finished_indicator(run_dir)
        dest = os.path.join(destination, os.path.basename(run_dir))
        sync_finished_indicator_command = ["rsync", finished_indicator_path, dest]
        p = subprocess.run(sync_finished_indicator_command)
        archive_finished_run(run_dir, archive_dir)
    else:
        logging.info(
            f"Previous rsync might be running still. Skipping {run_dir} for now."
        )
        return


def archive_finished_run(run_dir: str, archive_dir: str):
    """Move finished run to archive (nosync)."""

    logging.info(f"Archiving {run_dir}.")

    sample_dir = os.path.dirname(run_dir)
    exp_dir = os.path.dirname(sample_dir)

    os.path.basename(run_dir)
    sample_name = os.path.basename(sample_dir)
    exp_name = os.path.basename(exp_dir)

    # Create archive experiment group dir, if none
    if not os.path.exists(os.path.join(archive_dir, exp_name)):
        logging.info(f"Creating {os.path.join(archive_dir, exp_name)}.")
        os.mkdir(os.path.join(archive_dir, exp_name))
    # Create archive sample dir, if none
    if not os.path.exists(os.path.join(archive_dir, exp_name, sample_name)):
        logging.info(f"Creating {os.path.join(archive_dir, exp_name, sample_name)}.")
        os.mkdir(os.path.join(archive_dir, exp_name, sample_name))

    # Archive run
    logging.info(
        f"Archiving {run_dir} to {os.path.join(archive_dir, exp_name, sample_name)}."
    )
    shutil.move(run_dir, os.path.join(archive_dir, exp_name, sample_name))

    # Remove sample dir, if empty
    if not os.listdir(sample_dir):
        logging.info(f"Sample folder {sample_dir} is empty. Removing it.")
        os.rmdir(sample_dir)
    else:
        logging.info(
            f"Sample folder {sample_dir} is not empty ({os.listdir(sample_dir)}), leaving it."
        )
    # Remove experiment group dir, if empty
    if not os.listdir(exp_dir):
        logging.info(f"Experiment group folder {exp_dir} is empty. Removing it.")
        os.rmdir(exp_dir)
    else:
        logging.info(
            f"Experiment group folder {exp_dir} is not empty ({os.listdir(exp_dir)}), leaving it."
        )


def parse_position_logs(minknow_logs_dir: str) -> list:
    """Look through all position logs and boil down into a structured list of dicts

    Example output:
    [{
        "timestamp": "2023-07-10 15:44:31.481512",
        "category": "INFO: platform_qc.report (user_messages)",
        "body": {
            "flow_cell_id": "PAO33763"
            "num_pores": "8378"
        }
    } ... ]

    """

    # MinION
    positions = ["MN19414"]
    # PromethION
    for col in "123":
        for row in "ABCDEFGH":
            positions.append(col + row)

    headers = []
    header: dict | None = None
    for position in positions:
        log_files = glob(
            os.path.join(minknow_logs_dir, position, "control_server_log-*.txt")
        )

        if log_files:
            log_files.sort()

            for log_file in log_files:
                with open(log_file) as stream:
                    lines = stream.readlines()

                    # Iterate across log lines
                    for line in lines:
                        if not line[0:4] == "    ":
                            # Line is log header
                            split_header = line.split(" ")
                            timestamp = " ".join(split_header[0:2])
                            category = " ".join(split_header[2:])

                            header = {
                                "position": position,
                                "timestamp": timestamp.strip(),
                                "category": category.strip(),
                            }
                            headers.append(header)

                        elif header:
                            # Line is log body
                            if "body" not in header.keys():
                                body: dict = {}
                                header["body"] = body
                            key = line.split(": ")[0].strip()
                            val = ": ".join(line.split(": ")[1:]).strip()
                            header["body"][key] = val

    headers.sort(key=lambda x: x["timestamp"])
    logging.info(f"Parsed {len(headers)} log entries.")

    return headers


def get_pore_counts(position_logs: list) -> list:
    """Take the flowcell log list output by parse_position_logs() and subset to contain only QC and MUX info."""

    pore_counts = []
    for entry in position_logs:
        if "INFO: platform_qc.report (user_messages)" in entry["category"]:
            type = "qc"
        elif "INFO: mux_scan_result (user_messages)" in entry["category"]:
            type = "mux"
        else:
            type = "other"

        if type in ["qc", "mux"]:
            new_entry = {
                "flow_cell_id": entry["body"]["flow_cell_id"],
                "timestamp": entry["timestamp"],
                "position": entry["position"],
                "type": type,
                "num_pores": entry["body"]["num_pores"],
            }

            new_entry["total_pores"] = (
                entry["body"]["num_pores"]
                if type == "qc"
                else entry["body"]["total_pores"]
            )

            pore_counts.append(new_entry)

    logging.info(f"Subset {len(pore_counts)} QC and MUX log entries.")

    return pore_counts


def dump_pore_count_history(run: str, pore_counts: list) -> str:
    """For a recently started run, dump all QC and MUX events that the instrument remembers
    for the flow cell as a file in the run dir."""

    flowcell_id = os.path.basename(run).split("_")[-2]
    run_start_time = dt.strptime(os.path.basename(run)[0:13], "%Y%m%d_%H%M")
    log_time_pattern = "%Y-%m-%d %H:%M:%S.%f"

    new_file_path = os.path.join(run, "pore_count_history.csv")

    flowcell_pore_counts = [
        log_entry
        for log_entry in pore_counts
        if (
            log_entry["flow_cell_id"] == flowcell_id
            and dt.strptime(log_entry["timestamp"], log_time_pattern) <= run_start_time
        )
    ]

    if flowcell_pore_counts:
        flowcell_pore_counts_sorted = sorted(
            flowcell_pore_counts, key=lambda x: x["timestamp"], reverse=True
        )

        header = flowcell_pore_counts_sorted[0].keys()
        rows = [e.values() for e in flowcell_pore_counts_sorted]

        with open(new_file_path, "w") as f:
            f.write(",".join(header) + "\n")
            for row in rows:
                f.write(",".join(row) + "\n")
    else:
        # Create an empty file if there is not one already
        if not os.path.exists(new_file_path):
            open(new_file_path, "w").close()

    return new_file_path


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        dest="source_dir",
        help="Full path to directory containing runs to be synced.",
    )
    parser.add_argument(
        "--dest",
        dest="dest_dir",
        help="Full path to destination directory to sync default runs to.",
    )
    parser.add_argument(
        "--dest_qc",
        dest="dest_dir_qc",
        help="Full path to destination directory to sync QC runs to.",
    )
    parser.add_argument(
        "--archive",
        dest="archive_dir",
        help="Full path to directory containing runs to be synced.",
    )
    parser.add_argument(
        "--minknow_logs",
        dest="minknow_logs_dir",
        help="Full path to the directory containing the MinKNOW position logs.",
    )
    parser.add_argument(
        "--log",
        dest="log_path",
        help="Full path to the script log file.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    args = parser.parse_args()

    main(args)
