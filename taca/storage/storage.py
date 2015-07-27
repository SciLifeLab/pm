"""Storage methods and utilities"""
import getpass
import os
import logging
import re
import shutil
import time

from datetime import datetime
from multiprocessing import Pool

from statusdb.db import connections as statusdb
from taca.utils.config import CONFIG
from taca.utils import filesystem, misc

logger = logging.getLogger(__name__)

# This is used by many of the functions in this module
finished_run_indicator = CONFIG.get('storage', {}).get('finished_run_indicator',
                                                   'RTAComplete.txt')

def cleanup_nas(days):
    """Will move the finished runs in NASes to nosync directory.

    :param int days: Number of days to consider a run to be old
    """
    for data_dir in CONFIG.get('storage').get('data_dirs'):
        logger.info('Moving old runs in {}'.format(data_dir))
        with filesystem.chdir(data_dir):
            for run in [r for r in os.listdir(data_dir) if re.match(filesystem.RUN_RE, r)]:
                rta_file = os.path.join(run, finished_run_indicator)
                if os.path.exists(rta_file):
                    # 1 day == 60*60*24 seconds --> 86400
                    if os.stat(rta_file).st_mtime < time.time() - (86400 * days):
                        logger.info('Moving run {} to nosync directory'
                                    .format(os.path.basename(run)))
                        shutil.move(run, 'nosync')
                    else:
                        logger.info('{} file exists but is not older than {} day(s), skipping run {}'.format(
                                    finished_run_indicator, str(days), run))


def cleanup_processing(days):
    """Cleanup runs in processing server.

    :param int days: Number of days to consider a run to be old
    """
    transfer_file = os.path.join(CONFIG.get('preprocessing', {}).get('status_dir'), 'transfer.tsv')
    if not days:
        days = CONFIG.get('cleanup', {}).get('processing-server', {}).get('days', 10)
    try:
        #Move finished runs to nosync
        for data_dir in CONFIG.get('storage').get('data_dirs'):
            logger.info('Moving old runs in {}'.format(data_dir))
            with filesystem.chdir(data_dir):
                for run in [r for r in os.listdir(data_dir) if re.match(filesystem.RUN_RE, r)]:
                    if filesystem.is_in_file(transfer_file, run):
                        logger.info('Moving run {} to nosync directory'
                                    .format(os.path.basename(run)))
                        shutil.move(run, 'nosync')
                    else:
                        logger.info(("Run {} has not been transferred to the analysis "
                            "server yet, not archiving".format(run)))
        #Remove old runs from archiving dirs
        for archive_dir in CONFIG.get('storage').get('archive_dirs').values():
            logger.info('Removing old runs in {}'.format(archive_dir))
            with filesystem.chdir(archive_dir):
                for run in [r for r in os.listdir(archive_dir) if re.match(filesystem.RUN_RE, r)]:
                    rta_file = os.path.join(run, finished_run_indicator)
                    if os.path.exists(rta_file):
                        # 1 day == 60*60*24 seconds --> 86400
                        if os.stat(rta_file).st_mtime < time.time() - (86400 * days) and \
                                filesystem.is_in_swestore("{}.tar.bz2".format(run)):
                            logger.info('Removing run {} to nosync directory'
                                        .format(os.path.basename(run)))
                            shutil.rmtree(run)
                        else:
                            logger.info('{} file exists but is not older than {} day(s), skipping run {}'.format(
                                        finished_run_indicator, str(days), run))

    except IOError:
        sbj = "Cannot archive old runs in processing server"
        msg = ("Could not find transfer.tsv file, so I cannot decide if I should "
               "archive any run or not.")
        cnt = CONFIG.get('contact', None)
        if not cnt:
            cnt = "{}@localhost".format(getpass.getuser())
        logger.error(msg)
        misc.send_mail(sbj, msg, cnt)


def archive_to_swestore(days, run=None, max_runs=None, force=False, compress_only=False):
    """Send runs (as archives) in NAS nosync to swestore for backup

    :param int days: number fo days to check threshold
    :param str run: specific run to send swestore
    :param int max_runs: number of runs to be processed simultaneously
    :param bool force: Force the archiving even if the run is not complete
    :param bool compress_only: Compress the run without sending it to swestore
    """
    # If the run is specified in the command line, check that exists and archive
    if run:
        run = os.path.basename(run)
        base_dir = os.path.dirname(run)
        if re.match(filesystem.RUN_RE, run):
            # If the parameter is not an absolute path, find the run in the archive_dirs
            if not base_dir:
                for archive_dir in CONFIG.get('storage').get('archive_dirs'):
                    if os.path.exists(os.path.join(archive_dir, run)):
                        base_dir = archive_dir
            if not os.path.exists(os.path.join(base_dir, run)):
                logger.error(("Run {} not found. Please make sure to specify "
                              "the absolute path or relative path being in "
                              "the correct directory.".format(run)))
            else:
                with filesystem.chdir(base_dir):
                    _archive_run((run, days, force, compress_only))
        else:
            logger.error("The name {} doesn't look like an Illumina run"
                         .format(os.path.basename(run)))
    # Otherwise find all runs in every data dir on the nosync partition
    else:
        logger.info("Archiving old runs to SWESTORE")
        for to_send_dir in CONFIG.get('storage').get('archive_dirs'):
            logger.info('Checking {} directory'.format(to_send_dir))
            with filesystem.chdir(to_send_dir):
                to_be_archived = [r for r in os.listdir(to_send_dir)
                                  if re.match(filesystem.RUN_RE, r)
                                  and not os.path.exists("{}.archiving".format(r.split('.')[0]))]
                if to_be_archived:
                    pool = Pool(processes=len(to_be_archived) if not max_runs else max_runs)
                    pool.map_async(_archive_run, ((run, days, force, compress_only) for run in to_be_archived))
                    pool.close()
                    pool.join()
                else:
                    logger.info('No old runs to be archived')


def cleanup_swestore(days, dry_run=False):
    """Remove archived runs from swestore

    :param int days: Threshold days to check and remove
    """
    days = check_days('swestore', days, config)
    if not days:
        return
    runs = filesystem.list_runs_in_swestore(path=CONFIG.get('cleanup').get('swestore').get('root'))
    for run in runs:
        date = run.split('_')[0]
        if misc.days_old(date) > days:
            if dry_run:
                logger.info('Will remove file {} from swestore'.format(run))
                continue
            misc.call_external_command('irm -f {}'.format(run))
            logger.info('Removed file {} from swestore'.format(run))


def cleanup_uppmax(site, days, dry_run=False):
    """Remove project/run that have been closed more than 'days'
    from the given 'site' on uppmax

    :param str site: site where the cleanup should be performed
    :param int days: number of days to check for closed projects
    """
    days = check_days(site, days, config)
    if not days:
        return
    root_dir = CONFIG.get('cleanup').get(site).get('root')
    deleted_log = CONFIG.get('cleanup').get('deleted_log')
    assert os.path.exists(os.path.join(root_dir,deleted_log)), "Log directory {} doesn't exist in {}".format(deleted_log,root_dir)
    log_file = os.path.join(root_dir,"{fl}/{fl}.log".format(fl=deleted_log))

    # make a connection for project db #
    pcon = statusdb.ProjectSummaryConnection()
    assert pcon, "Could not connect to project database in StatusDB"

    if site != "archive":
        ## work flow for cleaning up illumina/analysis ##
        projects = [ p for p in os.listdir(root_dir) if re.match(filesystem.PROJECT_RE,p) ]
        list_to_delete = get_closed_projects(projects, pcon, days)
    else:
        ##work flow for cleaning archive ##
        list_to_delete = []
        archived_in_swestore = filesystem.list_runs_in_swestore(path=CONFIG.get('cleanup').get('swestore').get('root'), no_ext=True)
        runs = [ r for r in os.listdir(root_dir) if re.match(filesystem.RUN_RE,r) ]
        with filesystem.chdir(root_dir):
            for run in runs:
                fc_date = run.split('_')[0]
                if misc.days_old(fc_date) > days:
                    if run in archived_in_swestore:
                        list_to_delete.append(run)
                    else:
                        logger.warn("Run {} is older than {} days but not in "
                                    "swestore, so SKIPPING".format(run, days))

    ## delete and log
    for item in list_to_delete:
        if dry_run:
            logger.info('Will remove {} from {}'.format(item,root_dir))
            continue
        try:
            shutil.rmtree(os.path.join(root_dir,item))
            logger.info('Removed project {} from {}'.format(item,root_dir))
            with open(log_file,'a') as to_log:
                to_log.write("{}\t{}\n".format(item,datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M')))
        except OSError:
            logger.warn("Could not remove path {} from {}"
                        .format(item,root_dir))
            continue


#############################################################
# Class helper methods, not exposed as commands/subcommands #
#############################################################
def _archive_run((run, days, force, compress_only)):
    """ Archive a specific run to swestore

    :param str run: Run directory
    :param int days: Days to consider a run old
    :param bool force: Force the archiving even if the run is not complete
    :param bool compress_only: Only compress the run without sending it to swestore
    """

    def _send_to_swestore(f, dest, remove=True):
        """ Send file to swestore checking adler32 on destination and eventually
        removing the file from disk

        :param str f: File to remove
        :param str dest: Destination directory in Swestore
        :param bool remove: If True, remove original file from source
        """
        if not filesystem.is_in_swestore(f):
            logger.info("Sending {} to swestore".format(f))
            misc.call_external_command('iput -K -P {file} {dest}'.format(file=f, dest=dest),
                    with_log_files=True)
            logger.info('Run {} sent correctly and checksum was okay.'.format(f))
            if remove:
                logger.info('Removing run'.format(f))
                os.remove(f)
        else:
            logger.warn('Run {} is already in Swestore, not sending it again nor removing from the disk'.format(f))

    # Create state file to say that the run is being archived
    open("{}.archiving".format(run.split('.')[0]), 'w').close()
    if run.endswith('bz2'):
        if os.stat(run).st_mtime < time.time() - (86400 * days):
            _send_to_swestore(run, CONFIG.get('storage').get('irods').get('irodsHome'))
        else:
            logger.info("Run {} is not {} days old yet. Not archiving".format(run, str(days)))
    else:
        rta_file = os.path.join(run, finished_run_indicator)
        if not os.path.exists(rta_file) and not force:
            logger.warn(("Run {} doesn't seem to be completed and --force option was "
                      "not enabled, not archiving the run".format(run)))
        if force or (os.path.exists(rta_file) and os.stat(rta_file).st_mtime < time.time() - (86400 * days)):
            logger.info("Compressing run {}".format(run))
            # Compress with pbzip2
            misc.call_external_command('tar --use-compress-program=pbzip2 -cf {run}.tar.bz2 {run}'.format(run=run))
            logger.info('Run {} successfully compressed! Removing from disk...'.format(run))
            shutil.rmtree(run)
            if not compress_only:
                _send_to_swestore('{}.tar.bz2'.format(run), CONFIG.get('storage').get('irods').get('irodsHome'))
        else:
            logger.info("Run {} is not completed or is not {} days old yet. Not archiving".format(run, str(days)))
    os.remove("{}.archiving".format(run.split('.')[0]))


def get_closed_projects(projs, pj_con, days):
    """Takes list of project and gives project list that are closed
    more than given check 'days'

    :param list projs: list of projects to check
    :param obj pj_con: connection object to project database
    :param int days: number of days to check
    """
    closed_projs = []
    for proj in projs:
        if proj not in pj_con.name_view.keys():
            logger.warn("Project {} is not in database, so SKIPPING it.."
                        .format(proj))
            continue
        proj_db_obj = pj_con.get_entry(proj)
        try:
            proj_close_date = proj_db_obj['close_date']
        except KeyError:
            logger.warn("Project {} is either open or too old, so SKIPPING it..".format(proj))
            continue
        if misc.days_old(proj_close_date,date_format='%Y-%m-%d') > days:
            closed_projs.append(proj)
    return closed_projs


def check_days(site, days, config):
    """Check if 'days' given while running command. If not take the default threshold
    from config file (which should exist). Also when 'days' given on the command line
    raise a check to make sure it was really meant to do so

    :param str site: site to be cleaned and relevent date to pick
    :param int days: number of days to check, will be None if '-d' not used
    :param dict config: config file parsed and saved as dictionary
    """
    try:
        default_days = config['cleanup'][site]['days']
    except KeyError:
        raise
    if not days:
        return default_days
    elif days >= default_days:
        return days
    else:
        if misc.query_yes_no("Seems like given days({}) is less than the "
                             " default({}), are you sure to proceed ?"
                             .format(days,default_days), default="no"):
            return days
        else:
            return None
