"""Backup methods and utilities."""

import csv
import logging
import os
import re
import shutil
import subprocess as sp
import time
from datetime import datetime

from taca.utils import filesystem, misc, statusdb
from taca.utils.config import CONFIG

logger = logging.getLogger(__name__)


class run_vars:
    """A simple variable storage class."""

    def __init__(self, run, archive_path):
        self.abs_path = os.path.abspath(run)
        self.path, self.name = os.path.split(self.abs_path)
        self.name = self.name.split(".", 1)[0]
        self.zip = os.path.join(archive_path, f"{self.name}.tar.gz")
        self.key = f"{self.name}.key"
        self.key_encrypted = f"{self.name}.key.gpg"
        self.zip_encrypted = os.path.join(archive_path, f"{self.name}.tar.gz.gpg")


class backup_utils:
    """A class object with main utility methods related to backing up."""

    def __init__(self, run=None):
        self.run = run
        self.fetch_config_info()
        self.host_name = os.getenv("HOSTNAME", os.uname()[1]).split(".", 1)[0]

    def fetch_config_info(self):
        """Try to fecth required info from the config file. Log and exit if any neccesary info is missing."""
        try:
            self.data_dirs = CONFIG["backup"]["data_dirs"]
            self.archive_dirs = CONFIG["backup"]["archive_dirs"]
            self.archived_dirs = CONFIG["backup"]["archived_dirs"]
            self.exclude_list = CONFIG["backup"]["exclude_list"]
            self.keys_path = CONFIG["backup"]["keys_path"]
            self.gpg_receiver = CONFIG["backup"]["gpg_receiver"]
            self.mail_recipients = CONFIG["mail"]["recipients"]
            self.check_demux = CONFIG.get("backup", {}).get("check_demux", False)
            self.couch_info = CONFIG.get("statusdb")
            self.finished_run_indicator = CONFIG.get("storage", {}).get(
                "finished_run_indicator", "RTAComplete.txt"
            )
            self.copy_complete_indicator = CONFIG.get("storage", {}).get(
                "copy_complete_indicator", "CopyComplete.txt"
            )
            self.archive_log_location = CONFIG["backup"]["archive_log"]
        except KeyError as e:
            logger.error(
                f"Config file is missing the key {str(e)}, make sure it have all required information"
            )
            raise SystemExit

    def collect_runs(self, ext=None, filter_by_ext=False):
        """Collect runs from archive directories."""
        self.runs = []
        if self.run:
            run_type = self._get_run_type(self.run)
            archive_path = self.archive_dirs[run_type]
            run = run_vars(self.run, archive_path)
            if not (
                re.match(filesystem.RUN_RE, run.name)
                or re.match(filesystem.RUN_RE_ONT, run.name)
            ):
                logger.error(f"Given run {self.run} did not match a FC pattern")
                raise SystemExit
            if self._is_ready_to_archive(run, ext):
                self.runs.append(run)
        else:
            for adir in self.archive_dirs.values():
                if not os.path.isdir(adir):
                    logger.warn(f"Path {adir} does not exist or it is not a directory")
                    continue
                for item in os.listdir(adir):
                    if filter_by_ext and not item.endswith(ext):
                        continue
                    elif item.endswith(ext):
                        item = item.replace(ext, "")
                    elif not os.path.isdir(os.path.join(adir, item)):
                        continue
                    if (
                        re.match(filesystem.RUN_RE, item)
                        or re.match(filesystem.RUN_RE_ONT, item)
                    ) and item not in self.runs:
                        run_type = self._get_run_type(item)
                        archive_path = self.archive_dirs[run_type]
                        run = run_vars(os.path.join(adir, item), archive_path)
                        if self._is_ready_to_archive(run, ext):
                            self.runs.append(run)

    def avail_disk_space(self, path, run):
        """Check the space on file system based on parent directory of the run."""
        # not able to fetch runtype use the max size as precaution, size units in GB
        illumina_run_sizes = {
            "novaseq": 1800,
            "miseq": 20,
            "nextseq": 250,
            "NovaSeqXPlus": 3600,
            "promethion": 3000,
            "minion": 1000,
        }
        required_size = illumina_run_sizes.get(self._get_run_type(run), 900) * 2
        # check for any ongoing runs and add up the required size accrdingly
        for ddir in self.data_dirs.values():
            if not os.path.isdir(ddir):
                continue
            for item in os.listdir(ddir):
                if not re.match(filesystem.RUN_RE, item):
                    continue
                if not os.path.exists(os.path.join(ddir, item, "RTAComplete.txt")):
                    required_size += illumina_run_sizes.get(
                        self._get_run_type(run), 900
                    )
        # get available free space from the file system
        try:
            df_proc = sp.Popen(["df", path], stdout=sp.PIPE, stderr=sp.PIPE)
            df_out, df_err = df_proc.communicate()
            available_size = (
                int(df_out.strip().decode("utf-8").split("\n")[-1].strip().split()[3])
                / 1024
                / 1024
            )
        except Exception as e:
            logger.error(f"Evaluation of disk space failed with error {e}")
            raise SystemExit
        if available_size < required_size:
            e_msg = f"Required space for encryption is {required_size}GB, but only {available_size}GB available"
            subjt = f"Low space for encryption - {self.host_name}"
            logger.error(e_msg)
            misc.send_mail(subjt, e_msg, self.mail_recipients)
            raise SystemExit

    def file_in_pdc(self, src_file, silent=True):
        """Check if the given files exist in PDC."""
        # dsmc will return zero/True only when file exists, it returns
        # non-zero/False though cmd is execudted but file not found
        src_file_abs = os.path.abspath(src_file)
        try:
            sp.check_call(
                ["dsmc", "query", "archive", src_file_abs],
                stdout=sp.PIPE,
                stderr=sp.PIPE,
            )
            value = True
        except sp.CalledProcessError:
            value = False
        if not silent:
            msg = "File {} {} in PDC".format(
                src_file_abs, "exist" if value else "do not exist"
            )
            logger.info(msg)
        return value

    def _get_run_type(self, run):
        """Returns run type based on the flowcell name."""
        run_type = ""
        try:
            if "_A0" in run:
                run_type = "novaseq"
            elif "-" in run.split("_")[-1]:
                run_type = "miseq"
            elif "_NS" in run or "_VH" in run:
                run_type = "nextseq"
            elif "_LH" in run:
                run_type = "NovaSeqXPlus"
            elif "_MN" in run:
                run_type = "minion"
            elif re.match(
                "^(\d{8})_(\d{4})_([1-3][A-H])_([0-9a-zA-Z]+)_([0-9a-zA-Z]+)$", run
            ):
                run_type = "promethion"
            else:
                run_type = ""
        except:
            logger.warn(f"Could not fetch run type for run {run}")
        return run_type

    def _call_commands(
        self,
        cmd1,
        cmd2=None,
        out_file=None,
        return_out=False,
        mail_failed=False,
        tmp_files=[],
    ):
        """Call an external command(s) with atmost two commands per function call.
        Given 'out_file' is always used for the later cmd and also stdout can be return
        for the later cmd. In case of failure, the 'tmp_files' are removed"""
        if out_file:
            if not cmd2:
                stdout1 = open(out_file, "w")
            else:
                stdout1 = sp.PIPE
                stdout2 = open(out_file, "w")
        else:
            stdout1 = sp.PIPE
            stdout2 = sp.PIPE
        # calling the commands
        try:
            cmd1 = cmd1.split()
            p1 = sp.Popen(cmd1, stdout=stdout1, stderr=sp.PIPE)
            if cmd2:
                cmd2 = cmd2.split()
                p2 = sp.Popen(cmd2, stdin=p1.stdout, stdout=stdout2, stderr=sp.PIPE)
                p2_stat = p2.wait()
                p2_out, p2_err = p2.communicate()
                if not self._check_status(
                    cmd2, p2_stat, p2_err, mail_failed, tmp_files
                ):
                    return (False, p2_err) if return_out else False
            p1_stat = p1.wait()
            p1_out, p1_err = p1.communicate()
            if not self._check_status(cmd1, p1_stat, p1_err, mail_failed, tmp_files):
                return (False, p1_err) if return_out else False
            if return_out:
                return (True, p2_out) if cmd2 else (True, p1_out)
            return True
        except Exception as e:
            raise e
        finally:
            if out_file:
                if not cmd2:
                    stdout1.close()
                else:
                    stdout2.close()

    def _check_status(self, cmd, status, err_msg, mail_failed, files_to_remove=[]):
        """Check if a subprocess status is success and log error if failed."""
        if status != 0:
            self._clean_tmp_files(files_to_remove)
            if mail_failed:
                subjt = f"Command call failed - {self.host_name}"
                e_msg = "Called cmd: {}\n\nError msg: {}".format(" ".join(cmd), err_msg)
                misc.send_mail(subjt, e_msg, self.mail_recipients)
            logger.error(
                'Command "{}" failed with the error "{}"'.format(" ".join(cmd), err_msg)
            )
            return False
        return True

    def _clean_tmp_files(self, files):
        """Remove the file is exist."""
        for fl in files:
            if os.path.exists(fl):
                os.remove(fl)

    def _log_pdc_statusdb(self, run):
        """Log the time stamp in statusDB if a file is succussfully sent to PDC."""
        try:
            run_vals = run.split("_")
            if len(run_vals[0]) == 8:
                run_date = run_vals[0][2:]
            else:
                run_date = run_vals[0]
            run_fc = f"{run_date}_{run_vals[-1]}"
            couch_connection = statusdb.StatusdbSession(self.couch_info).connection
            x_flowcells_db = couch_connection[self.couch_info["db"]]
            fc_names = {
                e.key: e.id for e in x_flowcells_db.view("names/name", reduce=False)
            }
            d_id = fc_names[run_fc]
            doc = x_flowcells_db.get(d_id)
            doc["pdc_archived"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            x_flowcells_db.save(doc)
            logger.info(
                f'Logged "pdc_archived" timestamp for fc {run} in statusdb doc "{d_id}"'
            )
        except:
            logger.warn(f'Not able to log "pdc_archived" timestamp for run {run}')

    def _is_ready_to_archive(self, run, ext):
        """Check if the run to be encrypted has finished sequencing and has been copied completely to nas"""
        archive_ready = False
        run_path = run.abs_path
        rta_file = os.path.join(run_path, self.finished_run_indicator)
        cp_file = os.path.join(run_path, self.copy_complete_indicator)
        if (
            os.path.exists(rta_file)
            and os.path.exists(cp_file)
            and (not self.file_in_pdc(run.zip_encrypted))
        ) or (
            self._get_run_type(run.name) in ["promethion", "minion"]
            and os.path.exists(os.path.join(run_path, ".sync_finished"))
        ):
            # Case for encrypting
            # Run has NOT been encrypted (run.tar.gz.gpg not exists)
            if ext == ".tar.gz" and (not os.path.exists(run.zip_encrypted)):
                logger.info(
                    f"Sequencing has finished and copying completed for run {os.path.basename(run_path)} and is ready for archiving"
                )
                archive_ready = True
            # Case for putting data to PDC
            # Run has already been encrypted (run.tar.gz.gpg exists)
            elif ext == ".tar.gz.gpg" and os.path.exists(run.zip_encrypted):
                logger.info(
                    f"Sequencing has finished and copying completed for run {os.path.basename(run_path)} and is ready for sending to PDC"
                )
                archive_ready = True

        return archive_ready

    def log_archived_run(self, file_name):
        """Write files archived to PDC to log file"""
        with open(self.archive_log_location, "a") as archive_file:
            tsv_writer = csv.writer(archive_file, delimiter="\t")
            tsv_writer.writerow([file_name, str(datetime.now())])

    def _move_run_to_archived(self, run):
        """Move a run folder from nosync to archived"""
        run_type = self._get_run_type(run.name)
        archived_path = self.archived_dirs[run_type]
        if os.path.isdir(archived_path):
            logger.info(f"Moving run {run.name} to the archived folder")
            shutil.move(run.name, archived_path)
        else:
            logger.warning("Cannot move run to archived, destination does not exist")

    @classmethod
    def encrypt_runs(cls, run, force):
        """Encrypt the runs that have been collected."""
        bk = cls(run)
        bk.collect_runs(ext=".tar.gz")
        logger.info(f"In total, found {len(bk.runs)} run(s) to be encrypted")
        for run in bk.runs:
            run.flag = f"{run.name}.encrypting"
            run.dst_key_encrypted = os.path.join(bk.keys_path, run.key_encrypted)
            tmp_files = [run.zip_encrypted, run.key_encrypted, run.key, run.flag]
            logger.info(f"Encryption of run {run.name} is now started")
            # Check if there is enough space and exit if not
            bk.avail_disk_space(run.path, run.name)
            # Check if the run in demultiplexed
            if not force and bk.check_demux:
                if not misc.run_is_demuxed(
                    run, bk.couch_info, bk._get_run_type(run.name)
                ):
                    logger.warn(
                        f"Run {run.name} is not demultiplexed yet, so skipping it"
                    )
                    continue
                logger.info(
                    f"Run {run.name} is demultiplexed and proceeding with encryption"
                )
            with filesystem.chdir(run.path):
                # skip run if already ongoing
                if os.path.exists(run.flag):
                    logger.warn(
                        f"Run {run.name} is already being encrypted, so skipping now"
                    )
                    continue
                open(run.flag, "w").close()
                # zip the run directory
                if os.path.exists(run.zip):
                    if os.path.isdir(run.name):
                        logger.warn(
                            f"Both run source and zipped archive exist for run {run.name}, skipping run as precaution"
                        )
                        bk._clean_tmp_files([run.flag])
                        continue
                    logger.info(
                        f"Zipped archive already exist for run {run.name}, so using it for encryption"
                    )
                else:
                    exclude_files = " ".join(
                        [f"--exclude {x}" for x in bk.exclude_list]
                    )
                    logger.info(f"Creating zipped archive for run {run.name}")
                    if bk._call_commands(
                        cmd1=f"tar {exclude_files} -cf - {run.name}",
                        cmd2="pigz --fast -c -",
                        out_file=run.zip,
                        mail_failed=True,
                        tmp_files=[run.zip, run.flag],
                    ):
                        logger.info(
                            f"Run {run.name} was successfully compressed and transferred to {run.zip}"
                        )
                    else:
                        logger.warn(f"Skipping run {run.name} and moving on")
                        continue
                # Remove encrypted file if already exists
                if os.path.exists(run.zip_encrypted):
                    logger.warn(
                        f"Removing already existing encrypted file for run {run.name}, this is a precaution "
                        "to make sure the file was encrypted with correct key file"
                    )
                    bk._clean_tmp_files(
                        [
                            run.zip_encrypted,
                            run.key,
                            run.key_encrypted,
                            run.dst_key_encrypted,
                        ]
                    )
                # Generate random key to use as pasphrase
                if not bk._call_commands(
                    cmd1="gpg --gen-random 1 256", out_file=run.key, tmp_files=tmp_files
                ):
                    logger.warn(f"Skipping run {run.name} and moving on")
                    continue
                logger.info(f"Generated random phrase key for run {run.name}")
                # Calculate md5 sum pre encryption
                if not force:
                    logger.info("Calculating md5sum before encryption")
                    md5_call, md5_out = bk._call_commands(
                        cmd1=f"md5sum {run.zip}", return_out=True, tmp_files=tmp_files
                    )
                    if not md5_call:
                        logger.warn(f"Skipping run {run.name} and moving on")
                        continue
                    md5_pre_encrypt = md5_out.split()[0]
                # Encrypt the zipped run file
                logger.info("Encrypting the zipped run file")
                if not bk._call_commands(
                    cmd1=(
                        f"gpg --symmetric --cipher-algo aes256 --passphrase-file {run.key} --batch --compress-algo "
                        f"none -o {run.zip_encrypted} {run.zip}"
                    ),
                    tmp_files=tmp_files,
                ):
                    logger.warn(f"Skipping run {run.name} and moving on")
                    continue
                # Decrypt and check for md5
                if not force:
                    logger.info("Calculating md5sum after encryption")
                    md5_call, md5_out = bk._call_commands(
                        cmd1=f"gpg --decrypt --cipher-algo aes256 --passphrase-file {run.key} --batch {run.zip_encrypted}",
                        cmd2="md5sum",
                        return_out=True,
                        tmp_files=tmp_files,
                    )
                    if not md5_call:
                        logger.warn(f"Skipping run {run.name} and moving on")
                        continue
                    md5_post_encrypt = md5_out.split()[0]
                    if md5_pre_encrypt != md5_post_encrypt:
                        logger.error(
                            f"md5sum did not match before {md5_pre_encrypt} and after {md5_post_encrypt} encryption. Will remove temp files and move on"
                        )
                        bk._clean_tmp_files(tmp_files)
                        continue
                    logger.info("Md5sum matches before and after encryption")
                # Encrypt and move the key file
                if bk._call_commands(
                    cmd1=f"gpg -e -r {bk.gpg_receiver} -o {run.key_encrypted} {run.key}",
                    tmp_files=tmp_files,
                ):
                    shutil.move(run.key_encrypted, run.dst_key_encrypted)
                else:
                    logger.error("Encryption of key file failed, skipping run")
                    continue
                bk._clean_tmp_files([run.zip, run.key, run.flag])
                logger.info(
                    f"Encryption of run {run.name} is successfully done, removing zipped run file"
                )

    @classmethod
    def pdc_put(cls, run):
        """Archive the collected runs to PDC."""
        bk = cls(run)
        bk.collect_runs(ext=".tar.gz.gpg", filter_by_ext=True)
        logger.info(f"In total, found {len(bk.runs)} run(s) to send PDC")
        for run in bk.runs:
            run.flag = f"{run.name}.archiving"
            run.dst_key_encrypted = os.path.join(bk.keys_path, run.key_encrypted)
            if run.path not in bk.archive_dirs.values():
                logger.error(
                    "Given run is not in one of the archive directories {}. Kindly move the run {} to appropriate "
                    "archive dir before sending it to PDC".format(
                        ",".join(list(bk.archive_dirs.values())), run.name
                    )
                )
                continue
            if not os.path.exists(run.dst_key_encrypted):
                logger.error(
                    f"Encrypted key file {run.dst_key_encrypted} is not found for file {run.zip_encrypted}, skipping it"
                )
                continue
            with filesystem.chdir(run.path):
                # skip run if being encrypted
                if os.path.exists(f"{run.name}.encrypting"):
                    logger.warn(
                        f"Run {run.name} is currently being encrypted, so skipping now"
                    )
                    continue
                # skip run if already ongoing
                if os.path.exists(run.flag):
                    logger.warn(
                        f"Run {run.name} is already being archived, so skipping now"
                    )
                    continue
                if bk.file_in_pdc(run.zip_encrypted, silent=False) or bk.file_in_pdc(
                    run.dst_key_encrypted, silent=False
                ):
                    logger.warn(
                        f"Seems like files related to run {run.name} already exist in PDC, check and cleanup"
                    )
                    continue
                open(run.flag, "w").close()
                logger.info(f"Sending file {run.zip_encrypted} to PDC")
                if bk._call_commands(
                    cmd1=f"dsmc archive {run.zip_encrypted}", tmp_files=[run.flag]
                ):
                    time.sleep(15)  # give some time just in case 'dsmc' needs to settle
                    if bk._call_commands(
                        cmd1=f"dsmc archive {run.dst_key_encrypted}",
                        tmp_files=[run.flag],
                    ):
                        time.sleep(
                            5
                        )  # give some time just in case 'dsmc' needs to settle
                        if bk.file_in_pdc(run.zip_encrypted) and bk.file_in_pdc(
                            run.dst_key_encrypted
                        ):
                            logger.info(
                                f"Successfully sent file {run.zip_encrypted} to PDC, moving file locally from {run.path} to archived folder"
                            )
                            bk.log_archived_run(run.zip_encrypted)
                            if bk.couch_info:
                                bk._log_pdc_statusdb(run.name)
                            bk._clean_tmp_files(
                                [run.zip_encrypted, run.dst_key_encrypted, run.flag]
                            )
                            bk._move_run_to_archived(run)
                        continue
                logger.warn(f"Sending file {run.zip_encrypted} to PDC failed")
