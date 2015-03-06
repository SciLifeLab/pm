"""Storage methods and utilities"""

import os
import re
import shutil
import time

from taca.log import LOG
from taca.utils.config import CONFIG as config
from taca.utils import filesystem, misc

def cleanup(days):
    for data_dir in config.get('storage').get('data_dirs'):
        with filesystem.chdir(data_dir):
            for run in [r for r in os.listdir(data_dir) if re.match(filesystem.RUN_RE, r)]:
                rta_file = os.path.join(run, 'RTAComplete.txt')
                if os.path.exists(rta_file):
                    # 1 day == 60*60*24 seconds --> 86400
                    if os.stat(rta_file).st_mtime < time.time() - (86400 * days):
                        LOG.info('Moving run {} to nosync directory'.format(os.path.basename(run)))
                        shutil.move(run, 'nosync')
                    else:
                        LOG.info('RTAComplete.txt file exists but is not older than {} day(s), skipping run {}'.format(str(days), run))


def archive_to_swestore(days, run=None):
    # If the run is specified in the command line, check that exists and archive
    if run:
        run = os.path.basename(run)
        base_dir = os.path.dirname(run)
        if re.match(filesystem.RUN_RE, run):
            # If the parameter is not an absolute path, find the run in the archive_dirs
            if not base_dir:
                for archive_dir in config.get('storage').get('archive_dirs'):
                    if os.path.exists(os.path.join(archive_dir, run)):
                        base_dir = archive_dir
            if not os.path.exists(os.path.join(base_dir, run)):
                LOG.error(("Run {} not found. Please make sure to specify "
                    "the absolute path or relative path being in the correct directory.".format(run)))
            else:
                with filesystem.chdir(base_dir):
                    _archive_run(run)
        else:
            LOG.error("The name {} doesn't look like an Illumina run".format(os.path.basename(run)))
    # Otherwise find all runs in every data dir on the nosync partition
    else:
        LOG.info("Archiving old runs to SWESTORE")
        for data_dir in config.get('storage').get('data_dirs'):
            to_send_dir = os.path.join(data_dir, 'nosync')
            LOG.info('Checking {} directory'.format(to_send_dir))
            with filesystem.chdir(to_send_dir):
                for run in [r for r in os.listdir(to_send_dir) if re.match(filesystem.RUN_RE, r)]:
                    _archive_run(config, run)


#############################################################
# Class helper methods, not exposed as commands/subcommands #
#############################################################
def _archive_run(config, run):
    """ Archive a specific run to swestore

    :param dict config: Dictionary with configurations
    :param str run: Run directory
    """
    def _send_to_swestore(f, dest, remove=True):
        """ Send file to swestore checking adler32 on destination and eventually
        removing the file from disk

        :param str f: File to remove
        :param str dest: Destination directory in Swestore
        :param bool remove: If True, remove original file from source
        """
        if not filesystem.is_in_swestore(f) and not misc.exists_process_with_text(run):
            LOG.info("Sending {} to swestore".format(f))
            misc.call_external_command_detached('iput -K -P {file} {dest}'.format(file=f, dest=dest),
                    with_log_files=True)
            LOG.info('Run {} sent correctly and checksum was okay.'.format(f))
        else:
            LOG.warn('Run {} is already in Swestore or currently being archived, not sending it again'.format(f))
        if remove and filesystem.is_in_swestore(run) and not misc.exists_process_with_text(run):
            LOG.info('Removing run'.format(f))
            os.remove(f)


    if run.endswith('bz2'):
        _send_to_swestore(run, config.get('storage').get('irods').get('irodsHome'))
    else:
        LOG.info("Compressing run {}".format(run))
        # Compress with pbzip2
        misc.call_external_command('tar --use-compress-program=pbzip2 -cf {run}.tar.bz2 {run}'.format(run=run))
        LOG.info('Run {} successfully compressed! Removing from disk...'.format(run))
        shutil.rmtree(run)
        _send_to_swestore('{}.tar.bz2'.format(run), config.get('storage').get('irods').get('irodsHome'))
