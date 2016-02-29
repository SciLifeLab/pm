import os
from datetime import datetime

from flowcell_parser.classes import SampleSheetParser
from taca.utils.filesystem import chdir
from taca.illumina.Runs import Run
from taca.utils import misc

import logging
logger = logging.getLogger(__name__)

class NextSeq_Run(Run):

    def __init__(self,  path_to_run, configuration):
        # Constructor, it returns a NextSeq object only 
        # if the NextSeq run belongs to NGI facility, i.e., contains
        # Application or production in the Description
        super(NextSeq_Run, self).__init__( path_to_run, configuration)
        self._set_sequencer_type()
        self._set_run_type()

    def _set_sequencer_type(self):
        self.sequencer_type = "NextSeq"

    def _set_run_type(self):
        ssname = os.path.join(self.run_dir, 'Data', 'Intensities', 'BaseCalls', 'SampleSheet.csv')
        if not os.path.exists(ssname):
            # Case in which no samplesheet is found, assume it is a non NGI run
            self.run_type = "NON-NGI-RUN"
        else:
            # it SampleSheet exists try to see if it is a NGI-run
            ssparser = SampleSheetParser(ssname)
            # Jose : a key error can perfectly occur here
            if ssparser.header['Description'] == "Production" \
            or ssparser.header['Description'] == "Application" \
            or ssparser.header['Description'] == "Private":
                self.run_type = "NGI-RUN"
            else:
                # otherwise this is a non NGI run
                self.run_type = "NON-NGI-RUN"
            # Jose : This is a hack so to not break the naming convention in the NGI
            # The idea is that private costumers might sequence reads and in that
            # case the demultiplexed reads should not be transfered to Uppmax
            if ssparser.header['Description'] == "Private":
                self.transfer_to_analysis_server = False
                
    def _get_samplesheet(self):
        """ Locate and parse the samplesheet for a run.
            In NextSeq case this is located in run_dir/SampleSheet.csv
        """
        ssname = os.path.join(self.run_dir, 'Data', 'Intensities', 'BaseCalls', 'SampleSheet.csv')
        if os.path.exists(ssname):
            # if exists parse the SampleSheet
            return ssname
     
    def check_run_status(self):
        return
    
    def check_QC(self):
        return
    
    def post_qc(self, qc_file, status, log_file, rcp):
        return

    def demultiplex_run(self): 
        """ Demultiplex a NextSeq run:
            - find the samplesheet
            - make a local copy of the samplesheet and name it SampleSheet.csv
            - define if necessary the bcl2fastq commands (if indexes are not of size 8, i.e. neoprep)
            - run bcl2fastq conversion
        """

        ssname = self._get_samplesheet()
        ssparser = SampleSheetParser(ssname)
        # Samplesheet need to be positioned in the FC directory with name SampleSheet.csv (Illumina default)
        # if this is not the case then create it and take special care of modification to be done on the SampleSheet
        samplesheet_dest = os.path.join(self.run_dir, "SampleSheet.csv")
        # Check that the samplesheet is not already present. In this case go the next step
        if not os.path.exists(samplesheet_dest):
            try:
                with open(samplesheet_dest, 'wb') as fcd:
                    fcd.write(self._generate_clean_samplesheet(ssparser))
            except Exception as e:
                if os.path.exists(samplesheet_dest):
                    os.remove(samplesheet_dest)
                logger.error(e)
                return False
            logger.info(("Created SampleSheet.csv for Flowcell {} in {} "
                         .format(self.id, samplesheet_dest)))
        # SampleSheet.csv generated to be used in bcl2fastq
        self.runParserObj.samplesheet = SampleSheetParser(os.path.join(self.run_dir, "SampleSheet.csv"))
        # Make the demux call
        with chdir(self.run_dir):
            cl = [self.CONFIG.get('bcl2fastq')['bin']]
            if self.CONFIG.get('bcl2fastq').has_key('options'):
                cl_options = self.CONFIG['bcl2fastq']['options']
                # Append all options that appear in the configuration file to the main command.
                for option in cl_options:
                    if isinstance(option, dict):
                        opt, val = option.items()[0]
                        cl.extend(['--{}'.format(opt), str(val)])
                    else:
                        cl.append('--{}'.format(option))
            logger.info(("BCL to FASTQ conversion and demultiplexing started for "
                 " run {} on {}".format(os.path.basename(self.id), datetime.now())))
            misc.call_external_command_detached(cl, with_log_files=True)
            
        return True
    
    def compute_undetermined(self):
        """ This function parses the Undetermined files per lane produced by illumina
            for now nothing done, 
            TODO: Jose : finish this function to deal with undetermined
        """
        return True
        
    def _generate_clean_samplesheet(self, ssparser):
        """ Will generate a 'clean' samplesheet, for bcl2fastq2.17
        """
        output = ""
        # Header
        output += "[Header]{}".format(os.linesep)
        for field in ssparser.header:
            output += "{},{}".format(field.rstrip(), ssparser.header[field].rstrip())
            output += os.linesep
        # now parse the data section
        data = []
        # NextSeq has always 4 lanes (Assuming the Lane info is not in the samplesheet)
        # Therefore, the data sections must be duplicated 4 times, one for each lane
        for lane in xrange(1,5):
            for line in ssparser.data:
                entry = {}
                for field, value in line.iteritems():
                    if 'Sample_ID' in field:
                        entry[field] ='Sample_{}'.format(value)
                    elif 'Sample_Project' in field:
                        entry[field] = value.replace(".", "_")
                    else:
                        entry[field] = value     
                entry['Lane'] = str(lane)
                data.append(entry)

        fields_to_output = ['Lane', 'Sample_ID', 'Sample_Name', 'index', 'Sample_Project']
        # now create the new SampleSheet data section
        output += "[Data]{}".format(os.linesep)
        for field in ssparser.datafields:
            if field not in fields_to_output:
                fields_to_output.append(field)
        output += ",".join(fields_to_output)
        output += os.linesep
        # now process each data entry and output it
        for entry in data:
            line = []
            for field in fields_to_output:
                if field in entry:
                    line.append(entry[field])
            output += ",".join(line)
            output += os.linesep
        return output






