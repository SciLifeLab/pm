""" Analysis methods for TACA """
import csv
import glob
import logging
import os
import re
import subprocess
import shutil
import taca.utils.undetermined as ud
import flowcell_parser.db as fcpdb

from datetime import datetime

import requests

from taca.illumina import Run
from taca.utils.filesystem import chdir, control_fastq_filename
from taca.utils.config import CONFIG
from taca.utils import misc
from flowcell_parser.classes import XTenRunParametersParser,XTenSampleSheetParser,XTenParser

logger = logging.getLogger(__name__)


def is_transferred(run, transfer_file):
    """ Checks wether a run has been transferred to the analysis server or not.
        Returns true in the case in which the tranfer is ongoing.

    :param str run: Run directory
    :param str transfer_file: Path to file with information about transferred runs
    """
    try:
        with open(transfer_file, 'r') as file_handle:
            t_f = csv.reader(file_handle, delimiter='\t')
            for row in t_f:
                #Rows have two columns: run and transfer date
                if row[0] == os.path.basename(run):
                    return True
        if os.path.exists(os.path.join(run, 'transferring')):
            return True
        return False
    except IOError:
        return False


def transfer_run(run, analysis=True):
    """ Transfer a run to the analysis server. Will add group R/W permissions to
    the run directory in the destination server so that the run can be processed
    by any user/account in that group (i.e a functional account...). Run will be
    moved to data_dir/nosync after transferred.

    :param str run: Run directory
    :param bool analysis: Trigger analysis on remote server
    """
    command_line = ['rsync', '-av']
    # Add R/W permissions to the group
    command_line.append('--chmod=g+rw')
    # rsync works in a really funny way, if you don't understand this, refer to
    # this note: http://silentorbit.com/notes/2013/08/rsync-by-extension/
    command_line.append("--include=*/")
    for to_include in CONFIG['analysis']['analysis_server']['sync']['include']:
        command_line.append("--include={}".format(to_include))
    command_line.extend(["--exclude=*", "--prune-empty-dirs"])
    r_user = CONFIG['analysis']['analysis_server']['user']
    r_host = CONFIG['analysis']['analysis_server']['host']
    r_dir = CONFIG['analysis']['analysis_server']['sync']['data_archive']
    remote = "{}@{}:{}".format(r_user, r_host, r_dir)
    command_line.extend([run, remote])

    # Create temp file indicating that the run is being transferred
    try:
        open(os.path.join(run, 'transferring'), 'w').close()
    except IOError as e:
        logger.error("Cannot create a file in {}. Check the run name, and the permissions.".format(run))
        raise e
    started = ("Started transfer of run {} on {}"
               .format(os.path.basename(run), datetime.now()))
    logger.info(started)
    # In this particular case we want to capture the exception because we want
    # to delete the transfer file
    try:
        misc.call_external_command(command_line, with_log_files=True)
    except subprocess.CalledProcessError as exception:
        os.remove(os.path.join(run, 'transferring'))
        raise exception

    t_file = os.path.join(CONFIG['analysis']['status_dir'], 'transfer.tsv')
    logger.info('Adding run {} to {}'
                .format(os.path.basename(run), t_file))
    with open(t_file, 'a') as tranfer_file:
        tsv_writer = csv.writer(tranfer_file, delimiter='\t')
        tsv_writer.writerow([os.path.basename(run), str(datetime.now())])
    os.remove(os.path.join(run, 'transferring'))

    #Now, let's move the run to nosync
    archive_run(run)

    if analysis:
        trigger_analysis(run)

def archive_run(run):

    rppath=os.path.join(run, 'runParameters.xml')
    try:
        rp=XTenRunParametersParser(os.path.join(run, 'runParameters.xml'))
    except OSError:
        logger.warn("Cannot find the runParameters.xml file at {}. This is quite unexpected. please archive the run {} manually".format(rppath, run))
    else:
        try:
            #Works for recent control software
            runtype=rp.data["Setup"].get("Application Name")
        except KeyError :
            #should work for ancient control software
            runtype=rp.data.get("Application Name")

        if "HiSeq X" in runtype:
            destination=CONFIG['storage']['archive_dir']['HiSeqX']
        elif "MiSeq" in runtype:
            destination=CONFIG['storage']['archive_dir']['MiSeq']
        elif "HiSeq" in runtype:
            destination=CONFIG['storage']['archive_dir']['HiSeq']
        else:
            logger.warn("unrecognized runtype {}, cannot archive the run {}.".format(runtype, run))
            destination=None

        if destination:
            logger.info('archiving run {}'.format(run))
            os.move(os.path.abspath(run), os.path.join(destination, os.path.basename(os.path.abspath(run))))

def trigger_analysis(run_id):
    """ Trigger the analysis of the flowcell in the analysis sever.

    :param str run_id: run/flowcell id
    """
    if not CONFIG.get('analysis', {}).get('analysis_server', {}):
        logger.warn(("No configuration found for remote analysis server. "
                     "Not triggering analysis of {}"
                     .format(os.path.basename(run_id))))
    else:
        url = ("http://{host}:{port}/flowcell_analysis/{dir}"
               .format(host=CONFIG['analysis']['analysis_server']['host'],
                       port=CONFIG['analysis']['analysis_server']['port'],
                       dir=os.path.basename(run_id)))
        params = {'path': CONFIG['analysis']['analysis_server']['sync']['data_archive']}
        try:
            r = requests.get(url, params=params)
            if r.status_code != requests.status_codes.codes.OK:
                logger.warn(("Something went wrong when triggering the "
                             "analysis of {}. Please check the logfile "
                             "and make sure to start the analysis!"
                             .format(os.path.basename(run_id))))
            else:
                logger.info('Analysis of flowcell {} triggered in {}'
                            .format(os.path.basename(run_id),
                                    CONFIG['analysis']['analysis_server']['host']))
                a_file = os.path.join(CONFIG['analysis']['status_dir'], 'analysis.tsv')
                with open(a_file, 'a') as analysis_file:
                    tsv_writer = csv.writer(analysis_file, delimiter='\t')
                    tsv_writer.writerow([os.path.basename(run_id), str(datetime.now())])
        except requests.exceptions.ConnectionError:
            logger.warn(("Something went wrong when triggering the analysis "
                         "of {}. Please check the logfile and make sure to "
                         "start the analysis!".format(os.path.basename(run_id))))

def find_samplesheet(run):
    """Find the samplesheet for the given run

    :param taca.illumina.Run run: Run to find the SampleSheet for
    :returns str: Path to the SampleSheet
    """
    if run.run_type == 'MiSeq':
        return os.path.join(run.run_dir, 'Data', 'Intensities', 'BaseCalls', 'SampleSheet.csv')
    else:
        current_year = '20' + run.id[0:2]
        samplesheets_dir = os.path.join(CONFIG['analysis']['samplesheets_dir'],
                                        current_year)
        FC_ID = parsers.get_flowcell_id(run.run_dir)
        return os.path.join(samplesheets_dir, '{}.csv'.format(FC_ID))


def prepare_sample_sheet(run_dir, run_type, ss_origin):
    """Prepares a non-X10 samplesheet

    :param str run_dir: Directory of the run
    :param str run_type: Type of the run, either HiSeq or MiSeq
    :param str ss_origin: Path for the location of the original SampleSheet for the run
    """
    shutil.copy(ss_origin, run_dir)
    if run_type == 'MiSeq':
        with chdir(run_dir):
            miseq_ss = parsers.MiSeqSampleSheet(os.path.basename(ss_origin))
            miseq_ss.to_hiseq(parsers.get_flowcell_id(run_dir), write=True)
    return True


def prepare_x10_sample_sheet(run, ss_origin=None):
    """ This is a temporary function in order to solve the current problem with LIMS system
        not able to generate a compatible samplesheet for HiSeqX. This function needs to massage
        the sample sheet created by GenoLogics in order to correctly demultiplex HiSeqX runs.
        This function returns with success if the samplesheet is in the correct place, otherwise
        this flowcell will not be processed.

        :param str run: Run directory
        :param str ss_origin: Path to the SampleSheet for this run in the filesystem
    """
    #start by checking if samplesheet is in the correct place
    run_name = os.path.basename(run)
    FCID = run_name_componets[3][1:]

    FCID_samplesheet_dest = os.path.join(run, "SampleSheet.csv")

    ss_reader=XTenSampleSheetParser(ss_origin)
    #check that the samplesheet is not already present
    if os.path.exists(FCID_samplesheet_dest):
        logger.warn(("When trying to generate SampleSheet.csv for sample "
                     "sheet {}  looks like that SampleSheet.csv was already "
                     "present in {} !!".format(FCID, FCID_samplesheet_dest)))
        return False
    try:
        with open(FCID_samplesheet_dest, 'wb') as fcd:
            fcd.write(ss_reader.generate_clean_samplesheet(fields_to_remove=['index2'], rename_samples=True))
    except Exception as e:
        logger.error(e.text)
        return False


    # everything ended correctly
    return True



def post_qc(run, qc_file, status):
    """ Checks wether a run has passed the final qc.

    :param str run: Run directory
    :param str qc_file: Path to file with information about transferred runs
    """
    already_seen=False
    runname=os.path.basename(os.path.abspath(run))
    shortrun=runname.split('_')[0] + '_' +runname.split('_')[-1]
    with open(qc_file, 'ab+') as f:
        f.seek(0)
        for row in f:
            #Rows have two columns: run and transfer date
            if row.split('\t')[0] == runname:
                already_seen=True

        if not already_seen:
            if status:
                f.write("{}\tPASSED\n".format(runname))
            else:
                sj="{} failed QC".format(runname)
                cnt="""The run {run} has failed qc and will NOT be transfered to Nestor.

                       The run might be available at : https://genomics-status.scilifelab.se/flowcells/{shortfc}

                       To read the logs, run the following command on {server}
                       grep -A30 "Checking run {run}" {log}

                       To force the transfer :
                        taca analysis transfer {rundir} """.format(run=runname, shortfc=shortrun, log=CONFIG['log']['file'], server=os.uname()[1], rundir=run)
                rcp=CONFIG['mail']['recipients']
                misc.send_mail(sj, cnt, rcp)
                f.write("{}\tFAILED\n".format(os.path.basename(run)))


def upload_to_statusdb(run_dir):
    """
    Triggers the upload to statusdb using the dependency flowcell_parser

     :param string run_dir: the run directory to upload
    """
    couch = fcpdb.setupServer(CONFIG)
    db=couch[CONFIG['statusdb']['xten_db']]
    parser=XTenParser(run_dir)
    fcpdb.update_doc(db,parser.obj)

def run_preprocessing(run):
    """Run demultiplexing in all data directories

    :param str run: Process a particular run instead of looking for runs
    """

    def _process(run):
        """Process a run/flowcell and transfer to analysis server

        :param taca.illumina.Run run: Run to be processed and transferred
        """
        logger.info('Checking run {}'.format(run.id))
        if run.is_finished():
            if  run.status == 'TO_START':
                logger.info(("Starting BCL to FASTQ conversion and "
                             "demultiplexing for run {}".format(run.id)))
                # work around LIMS problem
                samplesheet = find_samplesheet(run)
                if (run.run_type == 'HiSeqX' and prepare_x10_sample_sheet(run.run_dir, samplesheet)) \
                        or (run.run_type != 'HiSeqX' and prepare_sample_sheet(run.run_dir, run.run_type, samplesheet)):
                    run.demultiplex()
                    # Right after demultiplexing, move data to nosync directory
                    shutil.move(run.run_dir, os.path.join(os.path.dirname(run.run_dir, 'nosync')))
            elif run.status == 'IN_PROGRESS':
                logger.info(("BCL conversion and demultiplexing process in "
                             "progress for run {}, skipping it"
                             .format(run.id)))
                if run.run_type == 'HiSeqX':
                    ud.check_undetermined_status(run.run_dir, dex_status=run.status, und_tresh=CONFIG['analysis']['undetermined']['lane_treshold'],
                        q30_tresh=CONFIG['analysis']['undetermined']['q30_treshold'], freq_tresh=CONFIG['analysis']['undetermined']['highest_freq'],
                        pooled_tresh=CONFIG['analysis']['undetermined']['pooled_und_treshold'])
            elif run.status == 'COMPLETED':
                logger.info(("Preprocessing of run {} is finished, check if "
                             "run has been transferred and transfer it "
                             "otherwise".format(run.id)))

                if run.run_type == 'HiSeqX':
                    control_fastq_filename(os.path.join(run.run_dir, CONFIG['analysis']['bcl2fastq']['options'][0]['output-dir']))
                    passed_qc=ud.check_undetermined_status(run.run_dir, dex_status=run.status, und_tresh=CONFIG['analysis']['undetermined']['lane_treshold'],
                        q30_tresh=CONFIG['analysis']['undetermined']['q30_treshold'], freq_tresh=CONFIG['analysis']['undetermined']['highest_freq'],
                        pooled_tresh=CONFIG['analysis']['undetermined']['pooled_und_treshold'])
                    qc_file = os.path.join(CONFIG['analysis']['status_dir'], 'qc.tsv')

                    post_qc(run.run_dir, qc_file, passed_qc)
                    upload_to_statusdb(run.run_dir)

                    t_file = os.path.join(CONFIG['analysis']['status_dir'], 'transfer.tsv')
                    transferred = is_transferred(run.run_dir, t_file)
                    if passed_qc:
                        if not transferred:
                            logger.info("Run {} hasn't been transferred yet."
                                        .format(run.id))
                            logger.info('Transferring run {} to {} into {}'
                                        .format(run.id,
                                CONFIG['analysis']['analysis_server']['host'],
                                CONFIG['analysis']['analysis_server']['sync']['data_archive']))
                            transfer_run(run.run_dir)
                        else:
                            logger.info('Run {} already transferred to analysis server, skipping it'.format(run.id))
                    else:
                        logger.warn('Run {} failed qc, transferring will not take place'.format(run.id))
                        r_file = os.path.join(CONFIG['analysis']['status_dir'], 'report.out')
                else:
                    t_file = os.path.join(CONFIG['analysis']['status_dir'], 'transfer.tsv')
                    if not is_transferred(run.run_dir, t_file):
                        logger.info("Run {} hasn't been transferred yet."
                                    .format(run.id))
                        logger.info('Transferring run {} to {} into {}'
                                    .format(run.id,
                            CONFIG['analysis']['analysis_server']['host'],
                            CONFIG['analysis']['analysis_server']['sync']['data_archive']))
                        transfer_run(run.run_dir)
                    else:
                        logger.info('Run {} already transferred to analysis server, skipping it'.format(run.id))

        if not run.is_finished():
            # Check status files and say i.e Run in second read, maybe something
            # even more specific like cycle or something
            logger.info('Run {} is not finished yet'.format(run.id))

    if run:
        _process(Run(run))
    else:
        data_dirs = CONFIG.get('analysis').get('data_dirs')
        for data_dir in data_dirs:
            runs = glob.glob(os.path.join(data_dir, '1*XX'))
            # Try MiSeq runs as well
            if not runs:
                runs = glob.glob(os.path.join(data_dir, '1*000000000*'))
            for _run in runs:
                _process(Run(_run))
