import socket
import os
import couchdb
import glob
import re
import logging
import datetime

from csv import DictReader
from taca.utils.config import CONFIG
from flowcell_parser.classes import SampleSheetParser
from collections import defaultdict, OrderedDict
from taca.utils.misc import send_mail

logger = logging.getLogger(__name__)

def setupServer(conf):
    db_conf = conf['statusdb']
    url="http://{0}:{1}@{2}:{3}".format(db_conf['username'], db_conf['password'], db_conf['url'], db_conf['port'])
    return couchdb.Server(url)

class Tree(defaultdict):
    """
    Constructor for a search tree
    """
    def __init__(self, value=None):
        super(Tree, self).__init__(Tree)
        self.value = value

def collect_runs():
    """
    Update command
    """
    found_runs=[]
    rundir_re = re.compile("\d{6}_[ST-]*\w+\d+_\d+_[AB]?[A-Z0-9\-]+")
    for data_dir in CONFIG['bioinfo_tab']['data_dirs']:
        if os.path.exists(data_dir):
            potential_run_dirs=glob.glob(os.path.join(data_dir, '*'))
            for run_dir in potential_run_dirs:
                if rundir_re.match(os.path.basename(os.path.abspath(run_dir))) and os.path.isdir(run_dir):
                    found_runs.append(os.path.basename(run_dir))
                    logger.info("Working on {}".format(run_dir))   
                    # updates run status     
                    update_statusdb(run_dir)
        nosync_data_dir = os.path.join(data_dir, "nosync")
        potential_nosync_run_dirs=glob.glob(os.path.join(nosync_data_dir, '*'))
        # wades through nosync directories
        for run_dir in potential_nosync_run_dirs:
            if rundir_re.match(os.path.basename(os.path.abspath(run_dir))) and os.path.isdir(run_dir):
                # update the run status
                update_statusdb(run_dir)

def update_statusdb(run_dir):
    """ 
    Gets status for a project
    """
    # fetch individual fields
    project_info = get_ss_projects(run_dir)
    run_id = os.path.basename(os.path.abspath(run_dir))
    couch = setupServer(CONFIG)
    valueskey = datetime.datetime.now().isoformat()
    db = couch['bioinfo_analysis']
    view = db.view('latest_data/sample_id')
    # Construction and sending of individual records, if samplesheet is incorrectly formatted the loop is skipped
    for flowcell in project_info:
        if flowcell == 'UNKNOWN':
            obj = {'run_id':run_id, 'project_id':'ERROR_Samplesheet'}
            logger.info("INVALID SAMPLESHEET, CHECK {} FORMED AT {}".format(run_id, valueskey))
            error_emailer('no_samplesheet', run_id)
            db.save(obj)
        else:
            for lane in project_info[flowcell]:
                for sample in project_info[flowcell][lane]:
                    for project in project_info[flowcell][lane][sample]:
                        project_info[flowcell][lane][sample].value = get_status(run_dir)
                        sample_status = project_info[flowcell][lane][sample].value
                        obj={'run_id':run_id, 'project_id':project, 'flowcell': flowcell, 'lane': lane, 
                             'sample':sample, 'status':sample_status, 'values':{valueskey:{'user':'taca','sample_status':sample_status}} }
                        # If entry exists, append to existing
                        # Special if case to handle lanes written as int, can be safely removed when old lanes
                        # is no longer stored as int
                        if len(view[[project, run_id, int(lane), sample]].rows) >= 1:
                            lane = int(lane)
                        if len(view[[project, run_id, lane, sample]].rows) >= 1:
                            remote_id = view[[project, run_id, lane, sample]].rows[0].id
                            lane = str(lane)
                            remote_doc = db[remote_id]['values']
                            remote_status = db[remote_id]['status']
                            # Only updates the listed statuses
                            if remote_status in ['ERROR', 'Sequencing', 'Demultiplexing', 'QC-Failed', 'BP-Failed', 'Failed']:
                                # Appends old entry to new. Essentially merges the two
                                for k, v in remote_doc.items():
                                    obj['values'][k] = v
                                logger.info("Updating {} {} {} {} {} as {}".format(run_id, project, 
                                flowcell, lane, sample, sample_status))
                                # Sorts timestamps
                                obj['values'] = OrderedDict(sorted(obj['values'].iteritems(),key=lambda (k,v): k,reverse=True))
                                # Update record cluster
                                obj['_rev'] = db[remote_id].rev
                                obj['_id'] = remote_id
                                db.save(obj)
                        # Creates new entry
                        else:
                            logger.info("Creating {} {} {} {} {} as {}".format(run_id, project, 
                            flowcell, lane, sample, sample_status))
                            # creates record
                            db.save(obj)
                        # Sets FC error flag
                        if not project_info[flowcell].value == None:
                            if (("Failed" in project_info[flowcell].value and "Failed" not in sample_status)
                             or ("Failed" in sample_status and "Failed" not in project_info[flowcell].value)): 
                                project_info[flowcell].value = 'Ambiguous' 
                            else:
                                project_info[flowcell].value = sample_status
            # Checks if a flowcell needs partial re-doing
            # Email error per flowcell
            if not project_info[flowcell].value == None:
                if 'Ambiguous' in project_info[flowcell].value:
                    #Jose : TODO run_name does not exist here
                    run_name = ""    
                    error_emailer('failed_run', run_name) 

def get_status(run_dir):   
    """ 
    Gets status of a sample run, based on flowcell info (folder structure)
    """ 
    #default state, should never occur
    status = 'ERROR'
    run_name = os.path.basename(os.path.abspath(run_dir))
    xten_dmux_folder=os.path.join(run_dir, 'Demultiplexing')
    unaligned_folder=glob.glob(os.path.join(run_dir, 'Unaligned_*'))
    nosync_pattern = re.compile("nosync")
    
    # If we're in a nosync folder
    if nosync_pattern.search(run_dir):
        status = 'New'
    # If demux folder exist (or similar)
    elif (os.path.exists(xten_dmux_folder) or unaligned_folder):
        status = 'Demultiplexing'
    # If RTAcomplete doesn't exist
    elif not (os.path.exists(os.path.join(run_dir, 'RTAComplete.txt'))):
        status = 'Sequencing'
    return status

def get_ss_projects(run_dir):
    """
    Fetches project, FC, lane & sample (sample-run) status for a given folder
    """
    proj_tree = Tree()
    #Make miseq well bit more explicit
    lane_pattern=re.compile("^([1-8]{1,2})$")
    sample_proj_pattern=re.compile("^((P[0-9]{3,5})_[0-9]{3,5})")
    run_name = os.path.basename(os.path.abspath(run_dir))
    current_year = '20' + run_name[0:2]
    run_name_components = run_name.split("_")
    FCID = run_name_components[3][1:]
    newData = False
    miseq = False
    
    xten_samplesheets_dir = os.path.join(CONFIG['bioinfo_tab']['xten_samplesheets'],
                                    current_year)
    hiseq_samplesheets_dir = os.path.join(CONFIG['bioinfo_tab']['hiseq_samplesheets'],
                                    current_year)
    #If it is not hiseq
    FCID_samplesheet_origin = os.path.join(hiseq_samplesheets_dir, '{}.csv'.format(FCID))
    if not os.path.exists(FCID_samplesheet_origin):
        # If it is not xten
        FCID_samplesheet_origin = os.path.join(xten_samplesheets_dir, '{}.csv'.format(FCID))
        if not os.path.exists(FCID_samplesheet_origin):
            # It is miseq
            miseq = True
            lanes = str(1)
            # Pattern is a bit more rigid since we're no longer also checking for lanes
            sample_proj_pattern = re.compile("^((P[0-9]{3,5})_[0-9]{3,5})$")
            FCID_samplesheet_origin = os.path.join(run_dir,'Data','Intensities','BaseCalls', 'SampleSheet.csv')
            if not os.path.exists(FCID_samplesheet_origin):
                FCID_samplesheet_origin = os.path.join(run_dir,'SampleSheet.csv')
                if not os.path.exists(FCID_samplesheet_origin):
                    logger.warn("Cannot locate the samplesheet for run {}".format(run_dir))
                    return ['UNKNOWN']

        ss_reader=SampleSheetParser(FCID_samplesheet_origin)
        if 'Description' in ss_reader.header and ss_reader.header['Description'] not in ['Production', 'Application']:
            # This is a non platform MiSeq run. Disregard it.
            return []
        data = ss_reader.data
    else:
        csvf = open(FCID_samplesheet_origin, 'rU')
        data = DictReader(csvf)
    proj_n_sample = False
    lane = False
    for d in data:
        for v in d.values():
            # if sample is found
            if sample_proj_pattern.search(v):
                samples = sample_proj_pattern.search(v).group(1)
                # project is also found
                projects = sample_proj_pattern.search(v).group(2)
                proj_n_sample = True
            # if a lane is found
            elif not miseq and lane_pattern.search(v):
                # In miseq case, FC only has 1 lane
                lanes = lane_pattern.search(v).group(1)
                lane = True
        # Populates structure
        if proj_n_sample and lane or proj_n_sample and miseq:
            proj_tree[FCID][lanes][samples][projects]
            proj_n_sample = False
            lane = False
    
    if proj_tree.keys() == []:
        logger.info("INCORRECTLY FORMATTED SAMPLESHEET, CHECK {}".format(run_name))
        #error_emailer('weird_samplesheet', run_name)
    
    return proj_tree

def error_emailer(flag, info):
    """
    Sends a custom error e-mail
    :param flag e-mail state
    :param info variable that describes the record in some way
    """
    recipients = CONFIG['mail']['recipients']
    #failed_run: Samplesheet for a given project couldn't be found
    body = 'TACA has encountered an issue that might be worth investigating\n'
    body += 'The offending entry is: '
    body += info
    body += '\n\nSincerely, TACA'

    if (flag == 'no_samplesheet'):
        subject='ERROR, Samplesheet error'
    elif (flag == "failed_run"):
        subject='WARNING, Reinitialization of partially failed FC'
    elif (flag == 'weird_samplesheet'):
        subject='ERROR, Incorrectly formatted samplesheet'
       
    hourNow = datetime.datetime.now().hour 
    if hourNow == 7 or hourNow == 12 or hourNow == 16:
        send_mail(subject, body, recipients)
    

    
    