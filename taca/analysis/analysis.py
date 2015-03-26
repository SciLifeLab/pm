""" Analysis methods for TACA """
import csv
import glob
import os
import shutil
import subprocess

import requests

from datetime import datetime
import re

from taca.illumina import Run
from taca.log import LOG
from taca.utils.filesystem import chdir
from taca.utils.config import CONFIG
from taca.utils import parsers, misc

def check_config_options(config):
    """ Check that all needed configuration sections/config are present

    :param dict config: Parsed configuration file
    """
    try:
        config['preprocessing']
        config['preprocessing']['hiseq_data']
        config['preprocessing']['mfs']
        config['preprocessing']['bcl2fastq']['path']
        config['preprocessing']['status_dir']
        config['preprocessing']['samplesheets_dir']
        config['preprocessing']['sync']
        config['preprocessing']['sync']['user']
        config['preprocessing']['sync']['host']
        config['preprocessing']['sync']['data_archive']
    except KeyError:
        raise RuntimeError(("Required configuration config not found, please "
            "refer to the README file."))


def is_transferred(run, transfer_file):
    """ Checks wether a run has been transferred to the analysis server or not.
        Returns true in the case in which the tranfer is ongoing.

    :param str run: Run directory
    :param str transfer_file: Path to file with information about transferred runs
    """
    try:
        with open(transfer_file, 'r') as f:
            t_f = csv.reader(f, delimiter='\t')
            for row in t_f:
                #Rows have two columns: run and transfer date
                if row[0] == os.path.basename(run):
                    return True
        if os.path.exists(os.path.join(run, 'transferring')):
            return True
        return False
    except IOError:
        return False


def transfer_run(run, config, analysis=True):
    """ Transfer a run to the analysis server. Will add group R/W permissions to
    the run directory in the destination server so that the run can be processed
    by any user/account in that group (i.e a functional account...)

    :param str run: Run directory
    :param dict config: Parsed configuration
    :param bool analysis: Trigger analysis on remote server
    """
    with chdir(run):
        cl = ['rsync', '-av']
        # Add R/W permissions to the group
        cl.append('--chmod=g+rw')
        # rsync works in a really funny way, if you don't understand this, refer to
        # this note: http://silentorbit.com/notes/2013/08/rsync-by-extension/
        cl.append("--include=*/")
        for to_include in config['sync']['include']:
            cl.append("--include={}".format(to_include))
        cl.extend(["--exclude=*", "--prune-empty-dirs"])
        r_user = config['sync']['user']
        r_host = config['sync']['host']
        r_dir = config['sync']['data_archive']
        remote = "{}@{}:{}".format(r_user, r_host, r_dir)
        cl.extend([run, remote])

        # Create temp file indicating that the run is being transferred
        open('transferring', 'w').close()
        started = ("Started transfer of run {} on {}".format(os.path.basename(run), datetime.now()))
        LOG.info(started)
        # In this particular case we want to capture the exception because we want
        # to delete the transfer file
        try:
            misc.call_external_command(cl, with_log_files=True)
        except subprocess.CalledProcessError as e:
            os.remove('transferring')
            raise e

        t_file = os.path.join(config['status_dir'], 'transfer.tsv')
        LOG.info('Adding run {} to {}'.format(os.path.basename(run), t_file))
        with open(t_file, 'a') as tf:
            tsv_writer = csv.writer(tf, delimiter='\t')
            tsv_writer.writerow([os.path.basename(run), str(datetime.now())])
        os.remove('transferring')

        if analysis:
            trigger_analysis(run, config)


def trigger_analysis(run, config):
    """ Trigger the analysis of the flowcell in the analysis sever.

    :param str run: Run directory
    :param dict config: Parsed configuration
    """
    if not config.get('analysis'):
        LOG.warn(("No configuration found for remote analysis server. Not triggering"
                  "analysis of {}".format(os.path.basename(run))))
    else:
        url = "http://{}:{}/flowcell_analysis/{}".format(config['analysis']['host'],
                                                         config['analysis']['port'],
                                                         os.path.basename(run))
        params = {'path': config['sync']['data_archive']}
        try:
            r = requests.get(url, params=params)
            if r.status_code != requests.status_codes.codes.OK:
                LOG.warn(("Something went wrong when triggering the analysis of {}. Please "
                          "check the logfile and make sure to start the analysis!".format(os.path.basename(run))))
            else:
                LOG.info('Analysis of flowcell {} triggered in {}'.format(os.path.basename(run),
                                                                          config['analysis']['host']))
                a_file = os.path.join(config['status_dir'], 'analysis.tsv')
                with open(a_file, 'a') as af:
                    tsv_writer = csv.writer(af, delimiter='\t')
                    tsv_writer.writerow([os.path.basename(run), str(datetime.now())])
        except requests.exceptions.ConnectionError:
            LOG.warn(("Something went wrong when triggering the analysis of {}. Please "
                      "check the logfile and make sure to start the analysis!".format(os.path.basename(run))))

def prepare_sample_sheet(run, config):
    """ This is a temporary function in order to solve the current problem with LIMS system
        not able to generate a compatible samplesheet for HiSeqX. This function needs to massage
        the sample sheet created by GenoLogics in order to correctly demultiplex HiSeqX runs.
        This function returns with success if the samplesheet is in the correct place, otherwise
        this flowcell will not be processed.

        :param str run: Run directory
        :param dict config: Parset configuration file

    """
    #start by checking if samplesheet is in the correct place
    run_name     = os.path.basename(run)
    current_year = '20' + run_name[0:2]
    samplesheets_dirs = config['samplesheets_dir']
    samplesheets_dir  = os.path.join(samplesheets_dirs, current_year)

    run_name_componets = run_name.split("_")
    FCID = run_name_componets[3][1:]

    FCID_samplesheet_origin = os.path.join(samplesheets_dir, FCID + '.csv')
    FCID_samplesheet_dest   = os.path.join(run, "SampleSheet.csv")

    #check that the samplesheet is not already present
    if os.path.exists(FCID_samplesheet_dest):
        LOG.warn(("When trying to generate SampleSheet.csv for sample sheet {}  looks like that "
                 "SampleSheet.csv was already present in {} !!".format(FCID, FCID_samplesheet_dest)))
        return False

    FCID_samplesheet_origin_dict = samplesheet_to_dict(FCID_samplesheet_origin) # store samplesheet in a dict

    FCID_samplesheet_dest_dict = samplesheet_massage(FCID_samplesheet_origin_dict, FCID_samplesheet_origin) # massage the sample sheet

    if not FCID_samplesheet_dest_dict: # something went wrong when working on samplesheet
        return False

    dict_to_samplesheet(FCID_samplesheet_dest_dict, FCID_samplesheet_dest) # store the new smaplesheet file in the fc dir

    return True #everything ended corretly


def samplesheet_massage(samplesheet_dict, samplesheet_name=''):
    """ ASSUMPTION: this samplesheet has been generated by genologics and is problematic.
        it contains index2 in Data section than needs to be removed, and SampleID needs
        to became SampleName

        :param dict FCID_samplesheet_origin_dict: the sample sheet stored in a hash table
    """
    #check that sample sheet is ok
    if not samplesheet_dict["Header"]:
        LOG.warn(("When trying to generate SampleSheet.csv for sample sheet {} I find out that "
          " it does not contain the Header section !!".format(samplesheet_name)))
        return False

    if not samplesheet_dict["Data"]:
        LOG.warn(("When trying to generate SampleSheet.csv for sample sheet {} I find out that "
                  " it does not contain the Data section !!".format(samplesheet_name)))
        return False


    FCID_samplesheet_dest_dict = {}
    FCID_samplesheet_dest_dict["Header"] = []
    for header in samplesheet_dict["Header"]:
        FCID_samplesheet_dest_dict["Header"].append(header)


    FCID_samplesheet_dest_dict["Data"] = []
    for data in samplesheet_dict["Data"]:
        if data: #if not empty
            new_data = data[0:6] + data[7:8] # remove index2
            if data[0] != "Lane": #this is the header section
                new_data[1] = "Sample_" + new_data[2] # make SampleID equal to Sample_SampleName
        FCID_samplesheet_dest_dict["Data"].append(new_data) # keep also white lines
    #I do not care if there are other [.*] sections
    return FCID_samplesheet_dest_dict





def dict_to_samplesheet(samplesheet_dict, file_dest):
    """ Writes samplesheet stroed in samplesheet in a form of hash table
        into a destination file
        ASSUMPTION: this is a Xten sample sheet, contains Header and Data sections

        :param dict samplesheet: samplesheet stored in a dict
        :param str file_dest: destination file

    """
    try:

        with open(file_dest, 'wb') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(["[Header]"])
            for header in samplesheet_dict["Header"]:
                writer.writerow(header)
            writer.writerow(["[Data]"])
            for data in samplesheet_dict["Data"]:
                writer.writerow(data)
    except ValueError:
        LOG.warning("Corrupt samplesheet %s, please fix it" % file_dest)
        pass


def samplesheet_to_dict(samplesheet):
    """ takes as input a samplesheet (Xten compatible) and stores all field in an hash table.
        Samplesheet should look something like:
            [Section1]
            section1,row,1
            section1,row,2
            [Section2]
            section2,row,1
            section2,row,2
            ...
        the hash structure will look like
            "Section1" --> [["section1", "row", "1"],
                            ["section1", "row" , "2"]
                           ]
            "Section2" --> [["section2", "row", "1"],
                            ["section2", "row" , "2"]
                           ]

        :param str samplesheet: the sample sheet to be stored in the hash table
    """
    samplesheet_dict = {}
    try:
        section = ""
        header = re.compile("^\[(.*)\]")
        with open(samplesheet, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for line in reader:
                if len(line)>0 and header.match(line[0]): # new header (or first) section and empy line
                    section = header.match(line[0]).group(1) # in this way I get the section
                    samplesheet_dict[section] = [] #initialise
                else:
                    if section == "":
                        LOG.warn("SampleSheet {} as unexepected format: aborting".format(samplesheet))
                        samplesheet_dict = {}
                        return samplesheet_dict
                    samplesheet_dict[section].append(line)
            return samplesheet_dict

    except ValueError:
        LOG.warning("Corrupt samplesheet %s, please fix it" % samplesheet)
        pass


def run_preprocessing(run):
    """ Run demultiplexing in all data directories """

    config = CONFIG['preprocessing']

    hiseq_runs = glob.glob(os.path.join(config['hiseq_data'], '1*XX')) if not run else [run]
    for _run in hiseq_runs:
        run = Run(_run)
        LOG.info('Checking run {}'.format(run.id))
        if run.is_finished():
            if  run.status == 'TO_START':
                LOG.info(("Starting BCL to FASTQ conversion and demultiplexing for "
                    "run {}".format(run.id)))
                if prepare_sample_sheet(run.run_dir, config): # work around LIMS problem
                    run.demultiplex(run.run_dir)
                    transfer_run(run.run_dir, config)
            elif run.status == 'IN_PROGRESS':
                LOG.info(("BCL conversion and demultiplexing process in progress for "
                    "run {}, skipping it".format(run.id)))
            elif run.status == 'COMPLETED':
                LOG.info(("Preprocessing of run {} is finished, check if run has been "
                    "transferred and transfer it otherwise".format(run.id)))

                t_file = os.path.join(config['status_dir'], 'transfer.tsv')
                transferred = is_transferred(run, t_file)
                if not transferred:
                    LOG.info("Run {} hasn't been transferred yet.".format(run.id))
                    LOG.info('Transferring run {} to {} into {}'.format(run.id,
                        config['sync']['host'],
                        config['sync']['data_archive']))
                    transfer_run(run, config)
                else:
                    LOG.info('Run {} already transferred to analysis server, skipping it'.format(run.id))

        if not run.is_finished():
            # Check status files and say i.e Run in second read, maybe something
            # even more specific like cycle or something
            LOG.info('Run {} is not finished yet'.format(run.id))
