
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

logger = logging.getLogger(__name__)

def setupServer(conf):
    db_conf = conf['statusdb']
    url="http://{0}:{1}@{2}:{3}".format(db_conf['username'], db_conf['password'], db_conf['url'], db_conf['port'])
    return couchdb.Server(url)

def merge(d1, d2):
    """ Will merge dictionary d2 into dictionary d1.
    On the case of finding the same key, the one in d1 will be used.
    :param d1: Dictionary object
    :param d2: Dictionary object
    """
    for key in d2:
        if key in d1:
            if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                merge(d1[key], d2[key])
        else:
            d1[key] = d2[key]
    return d1


def collect_runs():
    found_runs=[]
    rundir_re=re.compile("^[0-9]{6}_[A-Z0-9\-]+_[0-9]{4}_[A-Z0-9\-]{10,16}$")
    for data_dir in CONFIG['bioinfo_tab']['data_dirs']:
        if os.path.exists(data_dir):
            potential_run_dirs=glob.glob(os.path.join(data_dir, '*'))
            for run_dir in potential_run_dirs:
                if rundir_re.match(os.path.basename(os.path.abspath(run_dir))) and os.path.isdir(run_dir):
                    found_runs.append(os.path.basename(run_dir))
                    logger.info("Working on {}".format(run_dir))        
                    update_statusdb(run_dir)
        #no check the nosync
        nosync_data_dir = os.path.join(data_dir, "nosync")
        potential_nosync_run_dirs=glob.glob(os.path.join(nosync_data_dir, '*'))
        for run_dir in potential_nosync_run_dirs:
             if rundir_re.match(os.path.basename(os.path.abspath(run_dir))) and os.path.isdir(run_dir):
                #update the run status
                update_statusdb(run_dir)



def check_unfound_runs(found_runs):
    couch=setupServer(CONFIG)
    db=couch['bioinfo_analysis']
    valueskey=datetime.datetime.now().isoformat()
    view = db.view('general/not_ongoing_runids')
    for row in view.rows:
        if row.key not in found_runs:
            obj={'run_id':run_name,'status':'Ongoing', 'values':{valueskey:{'user':'taca','status':'Ongoing'}} }
            remote_doc=row.value
            final_obj=merge(obj, remote_doc)
            logger.info("saving {} as  {}".format(run_name, 'Ongoing'))
            db.save(final_obj)

def update_statusdb(run_dir):
    project_ids=get_ss_projects(run_dir)
    run_name = os.path.basename(os.path.abspath(run_dir))
    status=get_status(run_dir)
    couch=setupServer(CONFIG)
    valueskey=datetime.datetime.now().isoformat()
    db=couch['bioinfo_analysis']
    view = db.view('full_doc/pj_run_to_doc')
    for p in project_ids:
        obj={'run_id':run_name, 'project_id':p, 'status':status, 'values':{valueskey:{'user':'taca','status':status}} }
        if len(view[[p, run_name]].rows) == 1:
            remote_doc= view[[p, run_name]].rows[0].value
            remote_status=remote_doc["status"]
            if remote_status in ['Incoming', 'Sequencing Done', 'Demultiplexing', 'Demultiplexed', 'Transferring']:
                final_obj=merge(obj, remote_doc)
                logger.info("saving {} {} as  {}".format(run_name, p, status))
                db.save(final_obj)
        else:
            logger.info("saving {} {} as  {}".format(run_name, p, status))
            db.save(obj)

def get_status(run_dir):
    status='Incoming'

    run_name = os.path.basename(os.path.abspath(run_dir))
    xten_dmux_folder=os.path.join(run_dir, 'Demultiplexing')
    xten_dmux_stats=os.path.join(xten_dmux_folder, 'Stats', 'DemultiplexingStats.xml')
    unaligned_folder=glob.glob(os.path.join(run_dir, 'Unaligned_*'))
    unaligned_dmux_stats=glob.glob(os.path.join(run_dir, 'Unaligned_*', 'Basecall_Stats_*', 'Demultiplexing_Stats.htm'))
    taca_transfer=os.path.join(CONFIG['analysis']['status_dir'], 'transfer.tsv')
    old_transfer=CONFIG['bioinfo_tab']['b5_transfer']

    


    if os.path.exists(os.path.join(run_dir, 'RTAComplete.txt')):
        status='Sequencing Done'
    if os.path.exists(xten_dmux_folder) or unaligned_folder:
        status="Demultiplexing"
    if os.path.exists(xten_dmux_stats) or unaligned_dmux_stats:
        status='Demultiplexed'
    if os.path.exists(os.path.join(run_dir, 'transferring')):
        status='Transferring'

    if os.path.exists(taca_transfer):
        with open(taca_transfer) as t_file:
            for line in t_file:
                if run_name in line:
                    status='Ongoing'

    if os.path.exists(old_transfer):
        with open(old_transfer) as t_file:
            for line in t_file:
                if run_name in line:
                    elements=line.split("\s")
                    if len(elements)==2:
                        status='Transferring'
                    else:
                        status='Ongoing'

    return status




def get_ss_projects(run_dir):
    pattern=re.compile("(P[0-9]{3,5})_[0-9]{3,5}")
    project_ids=set()
    run_name = os.path.basename(os.path.abspath(run_dir))
    current_year = '20' + run_name[0:2]
    run_name_components = run_name.split("_")
    FCID = run_name_components[3][1:]

    xten_samplesheets_dir = os.path.join(CONFIG['bioinfo_tab']['xten_samplesheets'],
                                    current_year)
    hiseq_samplesheets_dir = os.path.join(CONFIG['bioinfo_tab']['hiseq_samplesheets'],
                                    current_year)
    FCID_samplesheet_origin = os.path.join(hiseq_samplesheets_dir, '{}.csv'.format(FCID))
    #if it is not hiseq
    if not os.path.exists(FCID_samplesheet_origin):
        FCID_samplesheet_origin = os.path.join(xten_samplesheets_dir, '{}.csv'.format(FCID))
        #if it is not xten
        if not os.path.exists(FCID_samplesheet_origin):
            #if it is miseq
            FCID_samplesheet_origin = os.path.join(run_dir,'Data','Intensities','BaseCalls', 'SampleSheet.csv')
            if not os.path.exists(FCID_samplesheet_origin):
                FCID_samplesheet_origin = os.path.join(run_dir,'SampleSheet.csv')
                if not os.path.exists(FCID_samplesheet_origin):
                    logger.warn("Cannot locate the samplesheet for run {}".format(run_dir))
                    return ['UNKNOWN']

        ss_reader=SampleSheetParser(FCID_samplesheet_origin)
        if 'Description' in ss_reader.header and ss_reader.header['Description'] not in ['Production', 'Application']:
            #This is a non platform MiSeq run. Disregard it.
            return []
        data=ss_reader.data

    else:
        csvf=open(FCID_samplesheet_origin, 'rU')
        data=DictReader(csvf)
    



    for d in data:
        for v in d.values():
            matches=pattern.search(v)
            if matches:
                project_ids.add(matches.group(1))
    
    return project_ids




    

    

