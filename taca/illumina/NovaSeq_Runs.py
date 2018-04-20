import os
import re
import csv
import glob
import shutil
import gzip
import operator
import subprocess
from datetime import datetime
from taca.utils.filesystem import chdir, control_fastq_filename
from taca.illumina.Runs import Run
from taca.illumina.HiSeq_Runs import HiSeq_Run
from taca.utils import misc
from flowcell_parser.classes import RunParametersParser, SampleSheetParser, RunParser, LaneBarcodeParser, DemuxSummaryParser


import logging

logger = logging.getLogger(__name__)

class NovaSeq_Run(HiSeq_Run):
    def __init__(self,  run_dir, samplesheet_folders):
        super(NovaSeq_Run, self).__init__( run_dir, samplesheet_folders)
        self._set_sequencer_type()
        self._set_run_type()


    def _set_sequencer_type(self):
        self.sequencer_type = "NovaSeq"

    def _set_run_type(self):
        self.run_type = "NGI-RUN"

    def _run_preprocessing(self):
        try:
            mfs_dest = os.path.join('/srv/mfs/NovaSeq_data', self.flowcell_id)
            logger.info('Copying RunParameters.xml and RunInfo.xml for run {} to {}'.format(self.id, mfs_dest))
            if not os.path.exists(mfs_dest):
                os.mkdir(mfs_dest)
            RunParameters_xml = os.path.join(self.run_dir, self.flowcell_id, 'RunParameters.xml')
            RunInfo_xml = os.path.join(self.run_dir, self.flowcell_id, 'RunInfo.xml')
            copyfile(RunParameters_xml, os.path.join(mfs_dest, 'RunParameters.xml'))
            copyfile(RunInfo_xml, os.path.join(mfs_dest, 'RunInfo.xml'))
        except:
            logger.warn('Could not copy RunParameters.xml and RunInfo.xml for run {}'.format(self.id))

    def _generate_clean_samplesheet(self, ssparser):
        """
        Will generate a 'clean' samplesheet, for bcl2fastq2.19
        """

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
        #now parse the data section
        for line in ssparser.data:
            line_ar=[]
            for field in datafields:
                value = line[field]
                if ssparser.dfield_sid in field :
                    value = 'Sample_{}'.format(line[ssparser.dfield_sid])
                line_ar.append(value)

            output+=",".join(line_ar)
            output+=os.linesep

        return output
