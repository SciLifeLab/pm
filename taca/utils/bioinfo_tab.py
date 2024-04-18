import datetime
import glob
import logging
import os
import re
from collections import OrderedDict, defaultdict

from flowcell_parser.classes import RunParametersParser, SampleSheetParser

from taca.utils import statusdb
from taca.utils.config import CONFIG
from taca.utils.misc import send_mail

logger = logging.getLogger(__name__)


class Tree(defaultdict):
    """Constructor for a search tree."""

    def __init__(self, value=None):
        super().__init__(Tree)
        self.value = value


def collect_runs():
    """Update command."""
    found_runs = []
    # Pattern explained:
    # 6-8Digits_(maybe ST-)AnythingLetterornumberNumber_Number_AorBLetterornumberordash
    rundir_re = re.compile("\d{6,8}_[ST-]*\w+\d+_\d+_[AB]?[A-Z0-9\-]+")
    for data_dir in CONFIG["bioinfo_tab"]["data_dirs"]:
        if os.path.exists(data_dir):
            potential_run_dirs = glob.glob(os.path.join(data_dir, "*"))
            for run_dir in potential_run_dirs:
                if rundir_re.match(
                    os.path.basename(os.path.abspath(run_dir))
                ) and os.path.isdir(run_dir):
                    found_runs.append(os.path.basename(run_dir))
                    logger.info(f"Working on {run_dir}")
                    update_statusdb(run_dir)
        nosync_data_dir = os.path.join(data_dir, "nosync")
        potential_nosync_run_dirs = glob.glob(os.path.join(nosync_data_dir, "*"))
        for run_dir in potential_nosync_run_dirs:
            if rundir_re.match(
                os.path.basename(os.path.abspath(run_dir))
            ) and os.path.isdir(run_dir):
                update_statusdb(run_dir)


def update_statusdb(run_dir):
    """Gets status for a project."""
    # Fetch individual fields
    project_info = get_ss_projects(run_dir)
    run_id = os.path.basename(os.path.abspath(run_dir))
    statusdb_conf = CONFIG.get("statusdb")
    couch_connection = statusdb.StatusdbSession(statusdb_conf).connection
    valueskey = datetime.datetime.now().isoformat()
    bioinfo_db = couch_connection["bioinfo_analysis"]
    view = bioinfo_db.view("latest_data/sample_id")
    # Construction and sending of individual records, if samplesheet is incorrectly formatted the loop is skipped
    if project_info:
        for flowcell in project_info:
            for lane in project_info[flowcell]:
                for sample in project_info[flowcell][lane]:
                    for project in project_info[flowcell][lane][sample]:
                        project_info[flowcell][lane][sample].value = get_status(run_dir)
                        sample_status = project_info[flowcell][lane][sample].value
                        obj = {
                            "run_id": run_id,
                            "project_id": project,
                            "flowcell": flowcell,
                            "lane": lane,
                            "sample": sample,
                            "status": sample_status,
                            "values": {
                                valueskey: {
                                    "user": "taca",
                                    "sample_status": sample_status,
                                }
                            },
                        }
                        # If entry exists, append to existing
                        # Special if case to handle lanes written as int, can be safely removed when old lanes
                        # is no longer stored as int
                        if len(view[[project, run_id, int(lane), sample]].rows) >= 1:
                            lane = int(lane)
                        if len(view[[project, run_id, lane, sample]].rows) >= 1:
                            remote_id = view[[project, run_id, lane, sample]].rows[0].id
                            lane = str(lane)
                            remote_doc = bioinfo_db[remote_id]["values"]
                            remote_status = bioinfo_db[remote_id]["status"]
                            # Only updates the listed statuses
                            if (
                                remote_status
                                in ["New", "ERROR", "Sequencing", "Demultiplexing"]
                                and sample_status != remote_status
                            ):
                                # Appends old entry to new. Essentially merges the two
                                for k, v in remote_doc.items():
                                    obj["values"][k] = v
                                logger.info(
                                    f"Updating {run_id} {project} {flowcell} {lane} {sample} as {sample_status}"
                                )
                                # Sorts timestamps
                                obj["values"] = OrderedDict(
                                    sorted(
                                        obj["values"].items(),
                                        key=lambda k_v: k_v[0],
                                        reverse=True,
                                    )
                                )
                                # Update record cluster
                                obj["_rev"] = bioinfo_db[remote_id].rev
                                obj["_id"] = remote_id
                                bioinfo_db.save(obj)
                        # Creates new entry
                        else:
                            logger.info(
                                f"Creating {run_id} {project} {flowcell} {lane} {sample} as {sample_status}"
                            )
                            # Creates record
                            bioinfo_db.save(obj)
                        # Sets FC error flag
                        if project_info[flowcell].value is not None:
                            if (
                                "Failed" in project_info[flowcell].value
                                and "Failed" not in sample_status
                            ) or (
                                "Failed" in sample_status
                                and "Failed" not in project_info[flowcell].value
                            ):
                                project_info[flowcell].value = "Ambiguous"
                            else:
                                project_info[flowcell].value = sample_status
            # Checks if a flowcell needs partial re-doing
            # Email error per flowcell
            if project_info[flowcell].value is not None:
                if "Ambiguous" in project_info[flowcell].value:
                    error_emailer("failed_run", run_id)


def get_status(run_dir):
    """Gets status of a sample run, based on flowcell info (folder structure)."""
    # Default state, should never occur
    status = "ERROR"
    xten_dmux_folder = os.path.join(run_dir, "Demultiplexing")
    unaligned_folder = glob.glob(os.path.join(run_dir, "Unaligned_*"))
    nosync_pattern = re.compile("nosync")

    # If we're in a nosync folder
    if nosync_pattern.search(run_dir):
        status = "New"
    # If demux folder exist (or similar)
    elif os.path.exists(xten_dmux_folder) or unaligned_folder:
        status = "Demultiplexing"
    # If RTAcomplete doesn't exist
    elif not (os.path.exists(os.path.join(run_dir, "RTAComplete.txt"))):
        status = "Sequencing"
    return status


def get_ss_projects(run_dir):
    """Fetches project, FC, lane & sample (sample-run) status for a given folder."""
    proj_tree = Tree()
    lane_pattern = re.compile("^([1-8]{1,2})$")
    sample_proj_pattern = re.compile("^((P[0-9]{3,5})_[0-9]{3,5})")
    run_name = os.path.basename(os.path.abspath(run_dir))
    run_date = run_name.split("_")[0]
    if len(run_date) == 6:
        current_year = "20" + run_date[0:2]
    elif len(run_name.split("_")[0]) == 8:  # NovaSeqXPlus case
        current_year = run_date[0:4]
    run_name_components = run_name.split("_")
    if "VH" in run_name_components[1]:
        FCID = run_name_components[3]
    else:
        FCID = run_name_components[3][1:]
    miseq = False
    # FIXME: this check breaks if the system is case insensitive
    if os.path.exists(os.path.join(run_dir, "runParameters.xml")):
        run_parameters_file = "runParameters.xml"
    elif os.path.exists(os.path.join(run_dir, "RunParameters.xml")):
        run_parameters_file = "RunParameters.xml"
    else:
        logger.error(
            f"Cannot find RunParameters.xml or runParameters.xml in the run folder for run {run_dir}"
        )
        return []
    rp = RunParametersParser(os.path.join(run_dir, run_parameters_file))
    if "Setup" in rp.data["RunParameters"]:
        runtype = rp.data["RunParameters"]["Setup"].get("Flowcell", "")
        if not runtype:
            logger.warn(
                "Parsing runParameters to fetch instrument type, "
                "not found Flowcell information in it. Using ApplicationName"
            )
            runtype = rp.data["RunParameters"]["Setup"].get("ApplicationName", "")
    elif "InstrumentType" in rp.data["RunParameters"]:
        runtype = rp.data["RunParameters"].get("InstrumentType")
    else:
        runtype = rp.data["RunParameters"].get("Application")
        if not runtype:
            logger.warn(
                "Couldn't find 'Application', could be NextSeq. Trying 'ApplicationName'"
            )
            runtype = rp.data["RunParameters"].get("ApplicationName", "")

    # Miseq case
    if "MiSeq" in runtype:
        if os.path.exists(
            os.path.join(run_dir, "Data", "Intensities", "BaseCalls", "SampleSheet.csv")
        ):
            FCID_samplesheet_origin = os.path.join(
                run_dir, "Data", "Intensities", "BaseCalls", "SampleSheet.csv"
            )
        elif os.path.exists(os.path.join(run_dir, "SampleSheet.csv")):
            FCID_samplesheet_origin = os.path.join(run_dir, "SampleSheet.csv")
        else:
            logger.warn(f"No samplesheet found for {run_dir}")
        miseq = True
        lanes = str(1)
        # Pattern is a bit more rigid since we're no longer also checking for lanes
        sample_proj_pattern = re.compile("^((P[0-9]{3,5})_[0-9]{3,5})$")
        data = parse_samplesheet(FCID_samplesheet_origin, run_dir, is_miseq=True)
    # HiSeq X case
    elif "HiSeq X" in runtype:
        FCID_samplesheet_origin = os.path.join(
            CONFIG["bioinfo_tab"]["xten_samplesheets"], current_year, f"{FCID}.csv"
        )
        data = parse_samplesheet(FCID_samplesheet_origin, run_dir)
    # HiSeq 2500 case
    elif "HiSeq" in runtype or "TruSeq" in runtype:
        FCID_samplesheet_origin = os.path.join(
            CONFIG["bioinfo_tab"]["hiseq_samplesheets"], current_year, f"{FCID}.csv"
        )
        data = parse_samplesheet(FCID_samplesheet_origin, run_dir)
    elif "NovaSeqXPlus" in runtype:
        FCID_samplesheet_origin = os.path.join(
            CONFIG["bioinfo_tab"]["novaseqxplus_samplesheets"],
            current_year,
            f"{FCID}.csv",
        )
        data = parse_samplesheet(FCID_samplesheet_origin, run_dir)
    # NovaSeq 6000 case
    elif "NovaSeq" in runtype:
        FCID_samplesheet_origin = os.path.join(
            CONFIG["bioinfo_tab"]["novaseq_samplesheets"], current_year, f"{FCID}.csv"
        )
        data = parse_samplesheet(FCID_samplesheet_origin, run_dir)
    # NextSeq Case
    elif "NextSeq" in runtype:
        FCID_samplesheet_origin = os.path.join(
            CONFIG["bioinfo_tab"]["nextseq_samplesheets"], current_year, f"{FCID}.csv"
        )
        data = parse_samplesheet(FCID_samplesheet_origin, run_dir)
    else:
        logger.warn(f"Cannot locate the samplesheet for run {run_dir}")
        return []

    # If samplesheet is empty, don't bother going through it
    if data == []:
        return data

    proj_n_sample = False
    lane = False
    for d in data:
        for v in d.values():
            # If sample is found
            if sample_proj_pattern.search(v):
                samples = sample_proj_pattern.search(v).group(1)
                # Project is also found
                projects = sample_proj_pattern.search(v).group(2)
                proj_n_sample = True

            # If a lane is found
            elif not miseq and lane_pattern.search(v):
                # In miseq case, FC only has 1 lane
                lanes = lane_pattern.search(v).group(1)
                lane = True

        # Populates structure
        if proj_n_sample and lane or proj_n_sample and miseq:
            proj_tree[FCID][lanes][samples][projects]
            proj_n_sample = False
            lane = False

    if list(proj_tree.keys()) == []:
        logger.info(f"INCORRECTLY FORMATTED SAMPLESHEET, CHECK {run_name}")
    return proj_tree


def parse_samplesheet(FCID_samplesheet_origin, run_dir, is_miseq=False):
    """Parses a samplesheet with SampleSheetParser
    :param FCID_samplesheet_origin sample sheet path
    """
    data = []
    try:
        ss_reader = SampleSheetParser(FCID_samplesheet_origin)
        data = ss_reader.data
    except:
        logger.warn(
            f"Cannot initialize SampleSheetParser for {run_dir}. Most likely due to poor comma separation"
        )
        return []

    if is_miseq:
        if "Description" not in ss_reader.header or not (
            "Production" in ss_reader.header["Description"]
            or "Application" in ss_reader.header["Description"]
        ):
            logger.warn(
                f"Run {run_dir} not labelled as production or application. Disregarding it."
            )
            # Skip this run
            return []
    return data


def error_emailer(flag, info):
    """Sends a custom error e-mail
    :param flag e-mail state
    :param info variable that describes the record in some way
    """
    recipients = CONFIG["mail"]["recipients"]

    # Failed_run: Samplesheet for a given project couldn't be found

    body = "TACA has encountered an issue that might be worth investigating\n"
    body += "The offending entry is: "
    body += info
    body += "\n\nSincerely, TACA"

    if flag == "no_samplesheet":
        subject = "ERROR, Samplesheet error"
    elif flag == "failed_run":
        subject = "WARNING, Reinitialization of partially failed FC"
    elif flag == "weird_samplesheet":
        subject = "ERROR, Incorrectly formatted samplesheet"

    hour_now = datetime.datetime.now().hour
    if hour_now == 7 or hour_now == 12 or hour_now == 16:
        send_mail(subject, body, recipients)


def fail_run(runid, project):
    """Updates status of specified run or project-run to Failed."""
    statusdb_conf = CONFIG.get("statusdb")
    logger.info("Connecting to status db: {}".format(statusdb_conf.get("url")))
    try:
        status_db = statusdb.StatusdbSession(statusdb_conf).connection
    except Exception as e:
        logger.error(
            "Can not connect to status_db: https://{}:*****@{}".format(
                statusdb_conf.get("username"), statusdb_conf.get("url")
            )
        )
        logger.error(e)
        raise e
    bioinfo_db = status_db["bioinfo_analysis"]
    if project is not None:
        view = bioinfo_db.view("full_doc/pj_run_to_doc")
        rows = view[[project, runid]].rows
        logger.info(
            f"Updating status of {len(rows)} objects with flowcell_id: {runid} and project_id {project}"
        )
    else:
        view = bioinfo_db.view("full_doc/run_id_to_doc")
        rows = view[[runid]].rows
        logger.info(f"Updating status of {len(rows)} objects with flowcell_id: {runid}")

    new_timestamp = datetime.datetime.now().isoformat()
    updated = 0
    for row in rows:
        if row.value["status"] != "Failed":
            row.value["values"][new_timestamp] = {
                "sample_status": "Failed",
                "user": "taca",
            }
            row.value["status"] = "Failed"
        try:
            bioinfo_db.save(row.value)
            updated += 1
        except Exception as e:
            logger.error(
                "Cannot update object project-sample-run-lane: {}-{}-{}-{}".format(
                    row.value.get("project_id"),
                    row.value.get("sample"),
                    row.value.get("run_id"),
                    row.value.get("lane"),
                )
            )
            logger.error(e)
            raise e
    logger.info(f"Successfully updated {updated} objects")
