import os
import re
import csv
import glob
import shutil
import copy
import json
from datetime import datetime
from taca.utils.filesystem import chdir, control_fastq_filename
from taca.illumina.Runs import Run
from taca.utils import misc
from flowcell_parser.classes import RunParametersParser, SampleSheetParser, RunParser, DemuxSummaryParser


import logging

logger = logging.getLogger(__name__)

class HiSeqX_Run(Run):

    def __init__(self,  run_dir, samplesheet_folders):
        super(HiSeqX_Run, self).__init__( run_dir, samplesheet_folders)
        self._set_sequencer_type()
        self._set_run_type()

    def _set_sequencer_type(self):
        self.sequencer_type = "HiSeqX"

    def _set_run_type(self):
        self.run_type = "NGI-RUN"

    def demultiplex_run(self):
        """
           Demultiplex a Xten run:
            - find the samplesheet
            - make a local copy of the samplesheet and name it SampleSheet.csv
            - define if necessary the bcl2fastq commands (if indexes are not of size 8, i.e. neoprep)
            - run bcl2fastq conversion
        """
        ssname   = self._get_samplesheet()
        ssparser = SampleSheetParser(ssname)
        try:
            indexfile = self.CONFIG['bcl2fastq']['index_path']
        except KeyError:
            logger.error("Path to index file (10X) not found in the config file")
            raise RuntimeError
        #samplesheet need to be positioned in the FC directory with name SampleSheet.csv (Illumina default)
        #if this is not the case then create it and take special care of modification to be done on the SampleSheet
        samplesheet_dest = os.path.join(self.run_dir, "SampleSheet.csv")
        #Function that returns a list of which lanes contains 10X samples.
        (lanes_10X,lanes_not_10X) = look_for_lanes_with_10X_indicies(indexfile, ssparser)
        #check that the samplesheet is not already present. In this case go the next step
        if not os.path.exists(samplesheet_dest):
            try:
                with open(samplesheet_dest, 'wb') as fcd:
                    fcd.write(_generate_clean_samplesheet(ssparser,indexfile, rename_samples=True, rename_qPCR_suffix = True, fields_qPCR=[ssparser.dfield_snm]))
            except Exception as e:
                logger.error("encountered the following exception '{}'".format(e))
                return False
            logger.info(("Created SampleSheet.csv for Flowcell {} in {} ".format(self.id, samplesheet_dest)))
        ##SampleSheet.csv generated

        ##when demultiplexing SampleSheet.csv is the one I need to use
        ## Need to rewrite so that SampleSheet_0.csv is always used.
        self.runParserObj.samplesheet  = SampleSheetParser(os.path.join(self.run_dir, "SampleSheet.csv"))
        #we have 10x lane - need to split the  samples sheet and build a 10x command for bcl2fastq
        Complex_run = False
        if len(lanes_10X) and len(lanes_not_10X):
             Complex_run = True

        if Complex_run:
            with chdir(self.run_dir):
                samplesheet_dest_not_10X="SampleSheet_0.csv"
                with open(samplesheet_dest_not_10X, 'wb') as fcd:
                    fcd.write(_generate_samplesheet_subset(self.runParserObj.samplesheet, lanes_not_10X))
                samplesheet_dest_10X="SampleSheet_1.csv"
                with open(samplesheet_dest_10X, 'wb') as fcd:
                    fcd.write(_generate_samplesheet_subset(self.runParserObj.samplesheet, lanes_10X))
        else:
            with chdir(self.run_dir):
                shutil.copy("SampleSheet.csv", "SampleSheet_0.csv")
        
        per_lane_base_masks = self._generate_per_lane_base_mask()
        max_different_base_masks =  max([len(per_lane_base_masks[base_masks]) for base_masks in per_lane_base_masks])
        if max_different_base_masks > 1:
            # in a HiSeqX run I cannot have different index sizes in the SAME lane
            logger.error("In FC {} found one or more lane with more than one base mask (i.e., different index sizes in \
                         in the same lane".format(self.id))
            return False
        bcl2fastq_cmd_counter = 0
        with chdir(self.run_dir):
            # create Demultiplexing dir, this changes the status to IN_PROGRESS
            if not os.path.exists("Demultiplexing"):
                os.makedirs("Demultiplexing")
        with chdir(self.run_dir):
            if lanes_not_10X:
               cmd_normal = self.generate_bcl_command(lanes_not_10X, bcl2fastq_cmd_counter)
               misc.call_external_command_detached(cmd_normal, with_log_files = True, prefix="demux_{}".format(bcl2fastq_cmd_counter))
               logger.info(("BCL to FASTQ conversion and demultiplexing started for "
                   "normal run {} on {}".format(os.path.basename(self.id), datetime.now())))
               bcl2fastq_cmd_counter += 1
            if lanes_10X:
               cmd_10X = self.generate_bcl_command(lanes_10X, bcl2fastq_cmd_counter, is_10X = True)
               misc.call_external_command_detached(cmd_10X, with_log_files = True, prefix="demux_{}".format(bcl2fastq_cmd_counter))
               logger.info(("BCL to FASTQ conversion and demultiplexing started for "
                   "10X run {} on {}".format(os.path.basename(self.id), datetime.now())))
               bcl2fastq_cmd_counter += 1
        return True

    
    def _aggregate_demux_results(self):
        """
        Take the Stats.json files from the different demultiplexing folders and merges them into one
        """
        ssname   = self._get_samplesheet()
        ssparser = SampleSheetParser(ssname)
        try:
            indexfile = self.CONFIG['bcl2fastq']['index_path']
        except KeyError:
            logger.error("Path to index file (10X) not found in the config file")
            raise RuntimeError
        #Function that returns a list of which lanes contains 10X samples.
        (lanes_10X,lanes_not_10X) = look_for_lanes_with_10X_indicies(indexfile, ssparser)
        lanes_10X_dict = {}
        for lane in lanes_10X:
            lanes_10X_dict[lane] = 0
        lanes_not_10X_dict = {}
        for lane in lanes_not_10X:
            lanes_not_10X_dict[lane] = 0
        if len(lanes_not_10X_dict) == 0:
            #in this case I have only 10X lanes, so I can treat it 10X lanes as the easy ones
            self._aggregate_demux_results_simple_complex(lanes_10X_dict, {})
        else:
            self._aggregate_demux_results_simple_complex(lanes_not_10X_dict, lanes_10X_dict)
    



    def generate_bcl_command(self, lanes, bcl2fastq_cmd_counter, is_10X=False):
        #I have everything to run demultiplexing now.
        logger.info('Building a bcl2fastq command')
        per_lane_base_masks = self._generate_per_lane_base_mask()
        with chdir(self.run_dir):
            cl = [self.CONFIG.get('bcl2fastq')['bin']]
            output_dir = "Demultiplexing_{}".format(bcl2fastq_cmd_counter)
            cl.extend(["--output-dir", output_dir])
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            cl_options = []
            if self.CONFIG.get('bcl2fastq').has_key('options'):
                for option in self.CONFIG['bcl2fastq']['options']:
                    cl_options.extend([option])
                # Add the extra 10X command options if we have a 10X run
                if is_10X:
                    cl_options.extend(self.CONFIG['bcl2fastq']['options_10X'])
                # Append all options that appear in the configuration file to the main command.
                for option in cl_options:
                    if isinstance(option, dict):
                        opt, val = option.items()[0]
                        if "output-dir" not in opt:
                            cl.extend(['--{}'.format(opt), str(val)])
                    else:
                        cl.append('--{}'.format(option))

            cl.extend(["--sample-sheet",  os.path.join(os.path.join(self.run_dir, "SampleSheet_{}.csv".format(bcl2fastq_cmd_counter)))])
            #now add the base_mask for each lane
            for lane in sorted(lanes):
                #Iterate thorugh each lane and add the correct --use-bases-mask for that lane
                base_mask = [per_lane_base_masks[lane][bm]['base_mask'] for bm in per_lane_base_masks[lane]][0] # get the base_mask
                base_mask_expr = "{}:".format(lane) + ",".join(base_mask)
                cl.extend(["--use-bases-mask", base_mask_expr])
        return cl
        

def _generate_clean_samplesheet(ssparser, indexfile, fields_to_remove=None, rename_samples=True, rename_qPCR_suffix = False, fields_qPCR= None):
    """
        Will generate a 'clean' samplesheet, the given fields will be removed.
        if rename_samples is True, samples prepended with 'Sample_'  are renamed to match the sample name
        Will also replace 10X idicies like SI-GA-A3 with proper indicies like TGTGCGGG
    """
    output=""
    ##expand the ssparser if there are 10X lanes
    index_dict=parse_10X_indexes(indexfile) #read the 10X indices
    # Replace 10X index with the 4 actual indicies.
    for sample in ssparser.data:
        if sample['index'] in index_dict.keys():
            x=0
            while x<3:
                new_sample=dict(sample)
                new_sample['index']=index_dict[sample['index']][x]
                ssparser.data.append(new_sample)
                x+=1
            #Set the original 10X index to the 4th correct index
            sample['index']=index_dict[sample['index']][x]

    #Sort to get the added indicies from 10x in the right place
    ssparser.data.sort()

    if not fields_to_remove:
        fields_to_remove=[]
    #Header
    output+="[Header]{}".format(os.linesep)
    for field in ssparser.header:
        output+="{},{}".format(field.rstrip(), ssparser.header[field].rstrip())
        output+=os.linesep
    #Data
    output+="[Data]{}".format(os.linesep)
    datafields=[]
    for field in ssparser.datafields:
        if field not in fields_to_remove:
            datafields.append(field)
    output+=",".join(datafields)
    output+=os.linesep
    for line in ssparser.data:
        line_ar=[]
        for field in datafields:
            value = line[field]
            if rename_samples and ssparser.dfield_sid in field :
                try:
                    if rename_qPCR_suffix and ssparser.dfield_snm in fields_qPCR:
                        #substitute SampleID with SampleName, add Sample_ as prefix and remove __qPCR_ suffix
                        value =re.sub('__qPCR_$', '', 'Sample_{}'.format(line[ssparser.dfield_snm]))
                    else:
                        #substitute SampleID with SampleName, add Sample_ as prefix
                        value ='Sample_{}'.format(line[ssparser.dfield_snm])
                except:
                        #otherwise add Sample_ as prefix
                        value = 'Sample_{}'.format(line[ssparser.dfield_sid])
            elif rename_qPCR_suffix and field in fields_qPCR:
                value = re.sub('__qPCR_$', '', line[field])

            line_ar.append(value)

        output+=",".join(line_ar)
        output+=os.linesep

    return output

def look_for_lanes_with_10X_indicies(indexfile, ssparser):
    """
    Given an ssparser object
    returns a list of lanes with 10X indicies
    """
    index_dict=parse_10X_indexes(indexfile)
    tenX_lanes = set() #Set to only get each lane once
    not_tenX_lanes = set()
    for sample in ssparser.data:
        if sample['index'] in index_dict.keys():
            tenX_lanes.add(sample['Lane'])
        else:
            not_tenX_lanes.add(sample['Lane'])

    return (list(tenX_lanes),list(not_tenX_lanes))


def parse_10X_indexes(indexfile):
    """
    Takes a file of 10X indexes and returns them as a dict.
    Todo: Set it up to take the file from config instead
    """
    index_dict={}
    with open(indexfile , 'r') as f:
        for line in f:
            line_=line.rstrip().split(',')
            index_dict[line_[0]]=line_[1:5]
    return index_dict


def _generate_samplesheet_subset(ssparser, lanes):
    output=""

    #Header
    output+="[Header]{}".format(os.linesep)
    for field in ssparser.header:
        output+="{},{}".format(field.rstrip(), ssparser.header[field].rstrip())
        output+=os.linesep
    #Data
    output+="[Data]{}".format(os.linesep)
    datafields=[]
    for field in ssparser.datafields:
        datafields.append(field)
    output+=",".join(datafields)
    output+=os.linesep
    for line in ssparser.data:
        if line['Lane'] in lanes:
            line_ar=[]
            for field in datafields:
                line_ar.append(line[field])
            output+=",".join(line_ar)
            output+=os.linesep

    return output
