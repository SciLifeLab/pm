import os
from taca.illumina.HiSeq_Runs import HiSeq_Run
from flowcell_parser.classes import SampleSheetParser

import logging
logger = logging.getLogger(__name__)

class NextSeq_Run(HiSeq_Run):

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
        ssname = os.path.join(self.run_dir, 'SampleSheet.csv')
        if not os.path.exists(ssname):
            # Case in which no samplesheet is found, assume it is a non NGI run
            self.run_type = "NON-NGI-RUN"
        else:
            # it SampleSheet exists try to see if it is a NGI-run
            ssparser = SampleSheetParser(ssname)
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
        """
        Locate and parse the samplesheet for a run.
        In NextSeq case this is located in run_dir/SampleSheet.csv
        """
        ssname = os.path.join(self.run_dir, 'SampleSheet.csv')
        if os.path.exists(ssname):
            # if exists parse the SampleSheet
            return ssname
        else:
            # some NextSeq runs do not have the SampleSheet at all, in this case assume they are non NGI.
            # not real clean solution but what else can be done if no samplesheet is provided?
            return None
     
    def check_run_status(self):
        return
    
    def check_QC(self):
        return
    
    def post_qc(self, qc_file, status, log_file, rcp):
        return

    def compute_undetermined(self):
        """
        This function parses the Undetermined files per lane produced by illumina
        for now nothing done, TODO: check all undetermined files are present as sanity check
        """
        return True
        
    def _generate_clean_samplesheet(self, ssparser):
        #Jose : Adjust to the NextSeq samplesheet format
        """
        Will generate a 'clean' samplesheet, for bcl2fastq2.17
        """
        output = ""
        # Header
        output += "[Header]{}".format(os.linesep)
        for field in ssparser.header:
            output += "{},{}".format(field.rstrip(), ssparser.header[field].rstrip())
            output += os.linesep
        # now parse the data section
        data = []
        for line in ssparser.data:
            entry = {}
            for field, value in line.iteritems():
                if 'Sample_ID' in field:
                    entry[field] ='Sample_{}'.format(value)
                elif 'Sample_Project' in field:
                    entry[field] = value.replace(".", "_")
                else:
                    entry[field] = value
            if 'Lane' not in entry:
                entry['Lane'] = '1'
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
                line.append(entry[field])
            output += ",".join(line)
            output += os.linesep
        return output






