import socket
import os
import couchdb
import glob
import re
import logging
import datetime

from csv import DictReader
from taca.utils.config import CONFIG
from flowcell_parser.classes import SampleSheetParser, RunParametersParser
from collections import defaultdict, OrderedDict
from taca.utils.misc import send_mail

logger = logging.getLogger(__name__)

def setupServer(conf):
    db_conf = conf['statusdb']
    url="http://{0}:{1}@{2}:{3}".format(db_conf['username'], db_conf['password'], db_conf['url'], db_conf['port'])
    return couchdb.Server(url)

"""Constructor for a search tree
"""
class Tree(defaultdict):
    def __init__(self, value=None):
        super(Tree, self).__init__(Tree)
        self.value = value

"""Update command
"""
def collect_runs():
    found_runs=[]
    #Pattern explained:
    #Digits_(maybe ST-)AnythingLetterornumberNumber_Number_AorbLetterornumberordash
    rundir_re = re.compile("\d{6}_[ST-]*\w+\d+_\d+_[AB]?[A-Z0-9\-]+")
    for data_dir in CONFIG['bioinfo_tab']['data_dirs']:
        if os.path.exists(data_dir):
            potential_run_dirs=glob.glob(os.path.join(data_dir, '*'))
            for run_dir in potential_run_dirs:
                if rundir_re.match(os.path.basename(os.path.abspath(run_dir))) and os.path.isdir(run_dir):
                    found_runs.append(os.path.basename(run_dir))
                    logger.info("Working on {}".format(run_dir))
                    #updates run status
                    update_statusdb(run_dir)
        nosync_data_dir = os.path.join(data_dir, "nosync")
        potential_nosync_run_dirs=glob.glob(os.path.join(nosync_data_dir, '*'))
        #wades through nosync directories
        for run_dir in potential_nosync_run_dirs:
            if rundir_re.match(os.path.basename(os.path.abspath(run_dir))) and os.path.isdir(run_dir):
                #update the run status
                update_statusdb(run_dir)

""" Gets status for a project
"""
def update_statusdb(run_dir):
    #fetch individual fields
    project_info=get_ss_projects(run_dir)
    run_id = os.path.basename(os.path.abspath(run_dir))
    couch=setupServer(CONFIG)
    valueskey=datetime.datetime.now().isoformat()
    db=couch['bioinfo_analysis']
    view = db.view('latest_data/sample_id')
    #Construction and sending of individual records, if samplesheet is incorrectly formatted the loop is skipped
    if not project_info == []:
        for flowcell in project_info:
            for lane in project_info[flowcell]:
                for sample in project_info[flowcell][lane]:
                    for project in project_info[flowcell][lane][sample]:
                        project_info[flowcell][lane][sample].value = get_status(run_dir)
                        sample_status = project_info[flowcell][lane][sample].value
                        obj={'run_id':run_id, 'project_id':project, 'flowcell': flowcell, 'lane': lane,
                             'sample':sample, 'status':sample_status, 'values':{valueskey:{'user':'taca','sample_status':sample_status}} }
                        #If entry exists, append to existing
                        #Special if case to handle lanes written as int, can be safely removed when old lanes
                        #is no longer stored as int
                        if len(view[[project, run_id, int(lane), sample]].rows) >= 1:
                            lane = int(lane)
                        if len(view[[project, run_id, lane, sample]].rows) >= 1:
                            remote_id = view[[project, run_id, lane, sample]].rows[0].id
                            lane = str(lane)
                            remote_doc = db[remote_id]['values']
                            remote_status = db[remote_id]['status']
                            #Only updates the listed statuses
                            if remote_status in ['New', 'ERROR', 'Sequencing', 'Demultiplexing'] and sample_status != remote_status:
                                #Appends old entry to new. Essentially merges the two
                                for k, v in remote_doc.items():
                                    obj['values'][k] = v
                                logger.info("Updating {} {} {} {} {} as {}".format(run_id, project,
                                flowcell, lane, sample, sample_status))
                                #Sorts timestamps
                                obj['values'] = OrderedDict(sorted(obj['values'].iteritems(),key=lambda (k,v): k,reverse=True))
                                #Update record cluster
                                obj['_rev'] = db[remote_id].rev
                                obj['_id'] = remote_id
                                db.save(obj)
                        #Creates new entry
                        else:
                            logger.info("Creating {} {} {} {} {} as {}".format(run_id, project,
                            flowcell, lane, sample, sample_status))
                            #creates record
                            db.save(obj)
                        #Sets FC error flag
                        if not project_info[flowcell].value == None:
                            if (("Failed" in project_info[flowcell].value and "Failed" not in sample_status)
                             or ("Failed" in sample_status and "Failed" not in project_info[flowcell].value)):
                                project_info[flowcell].value = 'Ambiguous'
                            else:
                                project_info[flowcell].value = sample_status
            #Checks if a flowcell needs partial re-doing
            #Email error per flowcell
            if not project_info[flowcell].value == None:
                if 'Ambiguous' in project_info[flowcell].value:
                    error_emailer('failed_run', run_name)

""" Gets status of a sample run, based on flowcell info (folder structure)
"""
def get_status(run_dir):
    #default state, should never occur
    status = 'ERROR'
    run_name = os.path.basename(os.path.abspath(run_dir))
    xten_dmux_folder=os.path.join(run_dir, 'Demultiplexing')
    unaligned_folder=glob.glob(os.path.join(run_dir, 'Unaligned_*'))
    nosync_pattern = re.compile("nosync")

    #If we're in a nosync folder
    if nosync_pattern.search(run_dir):
        status = 'New'
    #If demux folder exist (or similar)
    elif (os.path.exists(xten_dmux_folder) or unaligned_folder):
        status = 'Demultiplexing'
    #If RTAcomplete doesn't exist
    elif not (os.path.exists(os.path.join(run_dir, 'RTAComplete.txt'))):
        status = 'Sequencing'
    return status

"""Fetches project, FC, lane & sample (sample-run) status for a given folder
"""
def get_ss_projects(run_dir):
    proj_tree = Tree()
    lane_pattern=re.compile("^([1-8]{1,2})$")
    sample_proj_pattern=re.compile("^((P[0-9]{3,5})_[0-9]{3,5})")
    run_name = os.path.basename(os.path.abspath(run_dir))
    current_year = '20' + run_name[0:2]
    run_name_components = run_name.split("_")
    FCID = run_name_components[3][1:]
    newData = False
    miseq = False

    if os.path.exists(os.path.join(run_dir, 'runParameters.xml')):
        run_parameters_file = "runParameters.xml"
    elif os.path.exists(os.path.join(run_dir, 'RunParameters.xml')):
        run_parameters_file = "RunParameters.xml"
    else:
        logger.error("Cannot find RunParameters.xml or runParameters.xml in the run folder for run {}".format(run_dir))
        return []
    rp = RunParametersParser(os.path.join(run_dir, run_parameters_file))
    try:
        runtype = rp.data['RunParameters']["Setup"]["Flowcell"]
    except KeyError:
        logger.warn("Parsing runParameters to fetch instrument type, "
                "not found Flowcell information in it. Using ApplicationName")
        try:
            runtype = rp.data['RunParameters']["Setup"].get("ApplicationName", "")
        except KeyError:
            logger.warn("Couldn't find 'Setup' or 'ApplicationName' could be Novaseq. Trying 'Application'")
            runtype = rp.data['RunParameters']['Application']

    

    #Miseq case
    if "MiSeq" in runtype:
        if os.path.exists(os.path.join(run_dir,'Data','Intensities','BaseCalls', 'SampleSheet.csv')):
            FCID_samplesheet_origin = os.path.join(run_dir,'Data','Intensities','BaseCalls', 'SampleSheet.csv')
        elif os.path.exists(os.path.join(run_dir,'SampleSheet.csv')):
            FCID_samplesheet_origin = os.path.join(run_dir,'SampleSheet.csv')
        else:
            logger.warn("No samplesheet found for {}".format(run_dir))
        miseq = True
        lanes = str(1)
        #Pattern is a bit more rigid since we're no longer also checking for lanes
        sample_proj_pattern=re.compile("^((P[0-9]{3,5})_[0-9]{3,5})$")
        data = parse_samplesheet(FCID_samplesheet_origin, run_dir, is_miseq=True)
    #HiSeq X case
    elif "HiSeq X" in runtype:
        FCID_samplesheet_origin = os.path.join(CONFIG['bioinfo_tab']['xten_samplesheets'],
                                    current_year, '{}.csv'.format(FCID))
        data = parse_samplesheet(FCID_samplesheet_origin, run_dir)
    #HiSeq 2500 case
    elif "HiSeq" in runtype or "TruSeq" in runtype:
        FCID_samplesheet_origin = os.path.join(CONFIG['bioinfo_tab']['hiseq_samplesheets'],
                                    current_year, '{}.csv'.format(FCID))
        data = parse_samplesheet(FCID_samplesheet_origin, run_dir)
    #NovaSeq 600 case
    elif "NovaSeq" in runtype:
        FCID_samplesheet_origin = os.path.join(CONFIG['bioinfo_tab']['novaseq_samplesheets'],
                                    current_year, '{}.csv'.format(FCID)) 
        data = parse_samplesheet(FCID_samplesheet_origin, run_dir)
    else:
        logger.warn("Cannot locate the samplesheet for run {}".format(run_dir))
        return []

    #If samplesheet is empty, dont bother going through it
    if data == []:
            return data

    proj_n_sample = False
    lane = False
    for d in data:
        for v in d.values():
            #if sample is found
            if sample_proj_pattern.search(v):
                samples = sample_proj_pattern.search(v).group(1)
                #project is also found
                projects = sample_proj_pattern.search(v).group(2)
                proj_n_sample = True

            #if a lane is found
            elif not miseq and lane_pattern.search(v):
                #In miseq case, FC only has 1 lane
                lanes = lane_pattern.search(v).group(1)
                lane = True

        #Populates structure
        if proj_n_sample and lane or proj_n_sample and miseq:
            proj_tree[FCID][lanes][samples][projects]
            proj_n_sample = False
            lane = False

    if proj_tree.keys() == []:
        logger.info("INCORRECTLY FORMATTED SAMPLESHEET, CHECK {}".format(run_name))
    return proj_tree

"""Parses a samplesheet with SampleSheetParser
   :param FCID_samplesheet_origin sample sheet path
"""
def parse_samplesheet(FCID_samplesheet_origin, run_dir, is_miseq=False):
    data = []
    try:
        ss_reader=SampleSheetParser(FCID_samplesheet_origin)
        data=ss_reader.data
    except:
        logger.warn("Cannot initialize SampleSheetParser for {}. Most likely due to poor comma separation".format(run_dir))
        return []

    if is_miseq:
        if not 'Description' in ss_reader.header or not \
        ('Production' in ss_reader.header['Description'] or 'Application' in ss_reader.header['Description']):
            logger.warn("Run {} not labelled as production or application. Disregarding it.".format(run_dir))
            #skip this run
            return []
    return data

"""Sends a custom error e-mail
    :param flag e-mail state
    :param info variable that describes the record in some way
"""
def error_emailer(flag, info):
    recipients = CONFIG['mail']['recipients']

    #failed_run: Samplesheet for a given project couldn't be found

    body='TACA has encountered an issue that might be worth investigating\n'
    body+='The offending entry is: '
    body+= info
    body+='\n\nSincerely, TACA'

    if (flag == 'no_samplesheet'):
        subject='ERROR, Samplesheet error'
    elif (flag == "failed_run"):
        subject='WARNING, Reinitialization of partially failed FC'
    elif (flag == 'weird_samplesheet'):
        subject='ERROR, Incorrectly formatted samplesheet'

    hourNow = datetime.datetime.now().hour
    if hourNow == 7 or hourNow == 12 or hourNow == 16:
        send_mail(subject, body, recipients)


def fail_run(runid, project):
    """Updates status of specified run or project-run to Failed"""
    username = CONFIG.get('statusdb', {}).get('username')
    password = CONFIG.get('statusdb', {}).get('password')
    url = CONFIG.get('statusdb', {}).get('url')
    port = CONFIG.get('statusdb', {}).get('port')
    status_db_url = "http://{username}:{password}@{url}:{port}".format(username=username, password=password, url=url, port=port)
    logger.info('Connecting to status db: {}:{}'.format(url, port))
    try:
        status_db = couchdb.Server(status_db_url)
    except Exception, e:
        logger.error("Can't connect to status_db: {}".format(status_db_url))
        logger.error(e)
        raise e
    bioinfo_db = status_db['bioinfo_analysis']
    if project is not None:
        view = bioinfo_db.view('full_doc/pj_run_to_doc')
        rows = view[[project, runid]].rows
        logger.info('Updating status of {} objects with flowcell_id: {} and project_id {}'.format(len(rows), runid, project))
    else:
        view = bioinfo_db.view('full_doc/run_id_to_doc')
        rows = view[[runid]].rows
        logger.info('Updating status of {} objects with flowcell_id: {}'.format(len(rows), runid))

    new_timestamp = datetime.datetime.now().isoformat()
    updated = 0
    for row in rows:
        if row.value['status'] != 'Failed':
            row.value['values'][new_timestamp] = {'sample_status' : 'Failed', 'user': 'taca'}
            row.value['status'] = 'Failed'
        try:
            bioinfo_db.save(row.value)
            updated += 1
        except Exception, e:
            logger.error('Cannot update object project-sample-run-lane: {}-{}-{}-{}'.format(row.value.get('project_id'), row.value.get('sample'), row.value.get('run_id'), row.value.get('lane')))
            logger.error(e)
            raise e
    logger.info("Successfully updated {} objects".format(updated))
