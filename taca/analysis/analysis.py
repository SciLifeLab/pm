"""
Analysis methods for TACA
"""
import glob
import logging
import os

from shutil import copyfile
from taca.illumina.HiSeqX_Runs import HiSeqX_Run
from taca.illumina.HiSeq_Runs import HiSeq_Run
from taca.illumina.MiSeq_Runs import MiSeq_Run
from taca.illumina.NextSeq_Runs import NextSeq_Run
from taca.illumina.NovaSeq_Runs import NovaSeq_Run
from taca.utils.config import CONFIG

import flowcell_parser.db as fcpdb
from flowcell_parser.classes import RunParametersParser

logger = logging.getLogger(__name__)


def get_runObj(run):
    """ Tries to read runParameters.xml to parse the type of sequencer
        and then return the respective Run object (MiSeq, HiSeq..)
        :param run: run name identifier
        :type run: string
        :rtype: Object
        :returns: returns the sequencer type object,
        None if the sequencer type is unknown of there was an error
    """

    if os.path.exists(os.path.join(run, 'runParameters.xml')):
        run_parameters_file = "runParameters.xml"
    elif os.path.exists(os.path.join(run, 'RunParameters.xml')):
        run_parameters_file = "RunParameters.xml"
    else:
        logger.error("Cannot find RunParameters.xml or runParameters.xml in the run folder for run {}".format(run))
        return

    rppath = os.path.join(run, run_parameters_file)
    try:
        rp = RunParametersParser(os.path.join(run, run_parameters_file))
    except OSError:
        logger.warn("Problems parsing the runParameters.xml file at {}. "
                    "This is quite unexpected. please archive the run {} manually".format(rppath, run))
    else:
        #do a case by case test becasue there are so many version of RunParameters that there is no real other way
        runtype = rp.data['RunParameters'].get("Application", "")
        if "Setup" in rp.data['RunParameters']:
            #this is the HiSeq2500, MiSeq, and HiSeqX case
            try:
                # Works for recent control software
                runtype = rp.data['RunParameters']["Setup"]["Flowcell"]
            except KeyError:
                # Use this as second resource but print a warning in the logs
                logger.warn("Parsing runParameters to fecth instrument type, "
                            "not found Flowcell information in it. Using ApplicationName")
                # here makes sense to use get with default value "" ->
                # so that it doesn't raise an exception in the next lines
                # (in case ApplicationName is not found, get returns None)
                runtype = rp.data['RunParameters']["Setup"].get("ApplicationName", "")


        if "HiSeq X" in runtype in runtype:
            return HiSeqX_Run(run, CONFIG["analysis"]["HiSeqX"])
        elif "HiSeq" in runtype or "TruSeq" in runtype:
            return HiSeq_Run(run, CONFIG["analysis"]["HiSeq"])
        elif "MiSeq" in runtype:
            return MiSeq_Run(run, CONFIG["analysis"]["MiSeq"])
        elif "NextSeq" in runtype:
            return NextSeq_Run(run, CONFIG["analysis"]["NextSeq"])
        elif "NovaSeq" in runtype:
            return NovaSeq_Run(run, CONFIG["analysis"]["NovaSeq"])
        else:
            logger.warn("Unrecognized run type {}, cannot archive the run {}. "
                        "Someone as likely bought a new sequencer without telling "
                        "it to the bioinfo team".format(runtype, run))
    # Not necessary as the function will return None at this point but
    # just for being explicit
    return None

def upload_to_statusdb(run_dir):
    """ Function to upload run_dir informations to statusDB directly from click interface
        :param run_dir: run name identifier
        :type run: string
        :rtype: None
    """
    runObj = get_runObj(run_dir)
    if runObj:
        # runObj can be None
        # Make the actual upload
        _upload_to_statusdb(runObj)

def _upload_to_statusdb(run):
    """ Triggers the upload to statusdb using the dependency flowcell_parser
        :param Run run: the object run
    """
    couch = fcpdb.setupServer(CONFIG)
    db = couch[CONFIG['statusdb']['xten_db']]
    parser = run.runParserObj
    # Check if I have NoIndex lanes
    # This could be refactored and some checks for key exceptions should be added
    for element in parser.obj['samplesheet_csv']:
        if 'NoIndex' in element['index'] or not element['index']: # NoIndex in the case of HiSeq, empty in the case of HiSeqX
            lane = element['Lane'] # this is a lane with NoIndex
            # in this case PF Cluster is the number of undetermined reads
            try:
                PFclusters = parser.obj['Undetermined'][lane]['unknown']
            except KeyError:
                logger.error("While taking extra care of lane {} of NoIndex type " \
                             "I found out that not all values were available".format(lane))
                continue
            # In Lanes_stats fix the lane yield
            parser.obj['illumina']['Demultiplex_Stats']['Lanes_stats'][int(lane) - 1]['PF Clusters'] = str(PFclusters)
            # Now fix Barcode lane stats
            updated = 0 # Check that only one update is made
            for sample in parser.obj['illumina']['Demultiplex_Stats']['Barcode_lane_statistics']:
                if lane in sample['Lane']:
                    updated +=1
                    sample['PF Clusters'] = str(PFclusters)
            if updated != 1:
                logger.error("While taking extra care of lane {} of NoIndex type "
                             "I updated more than once the barcode_lane. "
                             "This is too much to continue so I will fail.".format(lane))
                os.sys.exit()
            # If I am here it means I changed the HTML representation to something
            # else to accomodate the wired things we do
            # someone told me that in such cases it is better to put a place holder for this
            parser.obj['illumina']['Demultiplex_Stats']['NotOriginal'] = "True"
    # Update info about bcl2fastq tool
    if not parser.obj.get('DemultiplexConfig'):
        parser.obj['DemultiplexConfig'] = {'Setup': {'Software': run.CONFIG.get('bcl2fastq',{})}}
    fcpdb.update_doc( db , parser.obj, over_write_db_entry=True)

def transfer_run(run_dir, analysis):
    """ Interface for click to force a transfer a run to uppmax
        :param: string run_dir: the run to tranfer
        :param bool analysis: if trigger or not the analysis
    """
    runObj = get_runObj(run_dir)
    mail_recipients = CONFIG.get('mail', {}).get('recipients')
    if runObj is None:
        mail_recipients = CONFIG.get('mail', {}).get('recipients')
        # Maybe throw an exception if possible?
        logger.error("Trying to force a transfer of run {} but the sequencer was not recognized.".format(run_dir))
    else:
        runObj.transfer_run(os.path.join("nosync",CONFIG['analysis']['status_dir'], 'transfer.tsv'),
                            analysis, mail_recipients) # do not start analsysis automatically if I force the transfer

def run_preprocessing(run, force_trasfer=True, statusdb=True):
    """ Run demultiplexing in all data directories
        :param str run: Process a particular run instead of looking for runs
        :param bool force_tranfer: if set to True the FC is transferred also if fails QC
        :param bool statusdb: True if we want to upload info to statusdb
    """
    def _process(run, force_trasfer):
        """ Process a run/flowcell and transfer to analysis server
            :param taca.illumina.Run run: Run to be processed and transferred
        """
        logger.info('Checking run {}'.format(run.id))
        t_file = os.path.join(CONFIG['analysis']['status_dir'], 'transfer.tsv')
        if run.is_transferred(t_file):
            # In this case I am either processing a run that is in transfer
            # or that has been already transferred. Do nothing.
            # time to time this situation is due to runs that are copied back from NAS after a reboot.
            # This check avoid failures
            logger.info('Run {} already transferred to analysis server, skipping it'.format(run.id))
            return

        if run.get_run_status() == 'SEQUENCING':
            # Check status files and say i.e Run in second read, maybe something
            # even more specific like cycle or something
            logger.info('Run {} is not finished yet'.format(run.id))
        elif run.get_run_status() == 'TO_START':
            if run.get_run_type() == 'NON-NGI-RUN':
                # For now MiSeq specific case. Process only NGI-run, skip all the others (PhD student runs)
                logger.warn("Run {} marked as {}, "
                            "TACA will skip this and move the run to "
                            "no-sync directory".format(run.id, run.get_run_type()))
                # Archive the run if indicated in the config file
                if 'storage' in CONFIG:
                    run.archive_run(CONFIG['storage']['archive_dirs'][run.sequencer_type])
                return
            # Otherwise it is fine, process it
            logger.info(("Starting BCL to FASTQ conversion and demultiplexing for run {}".format(run.id)))
            run.demultiplex_run()
        elif run.get_run_status() == 'IN_PROGRESS':
            logger.info(("BCL conversion and demultiplexing process in "
                         "progress for run {}, skipping it".format(run.id)))
            #this function checks if demux is done
            run.check_run_status()

        # previous elif might change the status to COMPLETED, therefore to avoid skipping
        # a cycle take the last if out of the elif
        if run.get_run_status() == 'COMPLETED':
            logger.info(("Preprocessing of run {} is finished, transferring it".format(run.id)))
            # Upload to statusDB if applies
            if 'statusdb' in CONFIG:
                _upload_to_statusdb(run)
                #notify with a mail run completion and stats uploaded
                msg = """The run {run} has been demultiplexed.
                The Run will be transferred to Irma for further analysis.

                The run is available at : https://genomics-status.scilifelab.se/flowcells/{run}

                """.format(run=run.id)
                run.send_mail(msg, rcp=CONFIG['mail']['recipients'])

            # Copy demultiplex stats file to shared file system for LIMS purpose
            if 'mfs_path' in CONFIG['analysis']:
                try:
                    mfs_dest = os.path.join(CONFIG['analysis']['mfs_path'][run.sequencer_type.lower()],run.id)
                    logger.info('Copying demultiplex stats for run {} to {}'.format(run.id, mfs_dest))
                    if not os.path.exists(mfs_dest):
                        os.mkdir(mfs_dest)
                    demulti_stat_src = os.path.join(run.run_dir, run.demux_dir, 'Reports',
                                                    'html', run.flowcell_id, 'all', 'all', 'all', 'laneBarcode.html')
                    copyfile(demulti_stat_src, os.path.join(mfs_dest, 'laneBarcode.html'))
                except:
                    logger.warn('Could not copy demultiplex stat file for run {}'.format(run.id))

            # Transfer to analysis server if flag is True
            if run.transfer_to_analysis_server:
                mail_recipients = CONFIG.get('mail', {}).get('recipients')
                logger.info('Transferring run {} to {} into {}'
                            .format(run.id,
                                    run.CONFIG['analysis_server']['host'],
                                    run.CONFIG['analysis_server']['sync']['data_archive']))
                run.transfer_run(t_file,  False, mail_recipients) # Do not trigger analysis


            # Archive the run if indicated in the config file
            if 'storage' in CONFIG:
                run.archive_run(CONFIG['storage']['archive_dirs'][run.sequencer_type])

    if run:
        # Needs to guess what run type I have (HiSeq, MiSeq, HiSeqX, NextSeq)
        runObj = get_runObj(run)
        if not runObj:
            raise RuntimeError("Unrecognized instrument type or incorrect run folder {}".format(run))
        else:
            _process(runObj, force_trasfer)
    else:
        data_dirs = CONFIG.get('analysis').get('data_dirs')
        for data_dir in data_dirs:
            # Run folder looks like DATE_*_*_*, the last section is the FC name. See Courtesy information from illumina of 10 June 2016 (no more XX at the end of the FC)
            runs = glob.glob(os.path.join(data_dir, '[1-9]*_*_*_*'))
            for _run in runs:
                runObj = get_runObj(_run)
                if not runObj:
                    logger.warning("Unrecognized instrument type or incorrect run folder {}".format(run))
                else:
                    try:
                        _process(runObj, force_trasfer)
                    except:
                        # this function might throw and exception,
                        # it is better to continue processing other runs
                        logger.warning("There was an error processing the run {}".format(run))
                        pass




