""" Submodule with Illumina-related code
"""
import os

from datetime import datetime
from xml.etree import ElementTree as ET

from taca.log import LOG
from taca.utils import misc
from taca.utils.config import CONFIG
from taca.utils.filesystem import chdir


def demultiplex_HiSeq_X(run):
    """ Demultiplexing for HiSeq X runs
    """
    LOG.info('Building bcl2fastq command')
    config = CONFIG['preprocessing']
    with chdir(run):
        cl_options = config['bcl2fastq']
        cl = [cl_options.get('XTen')]

        # Main options
        if cl_options.get('runfolder'):
            cl.extend(['--runfolder', cl_options.get('runfolder')])
        cl.extend(['--output-dir', cl_options.get('output-dir', 'Demultiplexing')])

        # Advanced options
        if cl_options.get('input-dir'):
            cl.extend(['--input-dir', cl_options.get('input-dir')])
        if cl_options.get('intensities-dir'):
            cl.extend(['--intensities-dir', cl_options.get('intensities-dir')])
        if cl_options.get('interop-dir'):
            cl.extend(['--interop-dir', cl_options.get('interop-dir')])
        if cl_options.get('stats-dir'):
            cl.extend(['--stats-dir', cl_options.get('stats-dir')])
        if cl_options.get('reports-dir'):
            cl.extend(['--reports-dir', cl_options.get('reports-dir')])

        # Processing cl_options
        threads = cl_options.get('loading-threads')
        if threads and type(threads) is int:
            cl.extend(['--loading-threads', '{}'.format(threads)])
        threads = cl_options.get('demultiplexing-threads')
        if threads and type(threads) is int:
            cl.extend(['--demultiplexing-threads', '{}'.format(threads)])
        threads = cl_options.get('processing-threads')
        if threads and type(threads) is int:
            cl.extend(['--processing-threads', '{}'.format(threads)])
        threads = cl_options.get('writing-threads')
        if threads and type(threads) is int:
            cl.extend(['--writing-threads', '{}'.format(threads)])

        # Behavioral options
        adapter_stringency = cl_options.get('adapter-stringency')
        if adapter_stringency and type(adapter_stringency) is float:
            cl.extend(['--adapter-stringency', adapter_stringency])
        aggregated_tiles = cl_options.get('aggregated-tiles')
        if aggregated_tiles and aggregated_tiles in ['AUTO', 'YES', 'NO']:
            cl.etend(['--aggregated-tiles', aggregated_tiles])
        barcode_missmatches = cl_options.get('barcode-missmatches')
        if barcode_missmatches and type(barcode_missmatches) is int \
                and barcode_missmatches in range(3):
            cl.extend(['--barcode-missmatches', barcode_missmatches])
        if cl_options.get('create-fastq-for-index-reads'):
            cl.append('--create-fastq-for-index-reads')
        if cl_options.get('ignore-missing-bcls'):
            cl.append('--ignore-missing-bcls')
        if cl_options.get('ignore-missing-filter'):
            cl.append('--ignore-missing-filter')
        if cl_options.get('ignore-missing-locs'):
            cl.append('--ignore-missing-locs')
        mask = cl_options.get('mask-short-adapter-reads')
        if mask and type(mask) is int:
            cl.extend(['--mask-short-adapter-reads', mask])
        minimum = cl_options.get('minimum-trimmed-reads')
        if minimum and type(minimum) is int:
            cl.extend(['--minimum-trimmed-reads', minimum])
        if cl_options.get('tiles'):
            cl.extend(['--tiles', cl_options.get('tiles')])

        if cl_options.get('use-bases-mask'):
            cl.extend(['--use-bases-mask', cl_options.get('use-bases-mask')])

        if cl_options.get('with-failed-reads'):
            cl.append('--with-failed-reads')
        if cl_options.get('write-fastq-reverse-complement'):
            cl.append('--write-fastq-reverse-complement')

        LOG.info(("BCL to FASTQ conversion and demultiplexing started for "
                  " run {} on {}".format(os.path.basename(run), datetime.now())))

        misc.call_external_command_detached(cl, with_log_files=True)

        LOG.info(("BCL to FASTQ conversion and demultiplexing finished for "
                  "run {} on {}".format(os.path.basename(run), datetime.now())))


def demultiplex_HiSeq(run):
    """ Demultiplexing for HiSeq (V3/V4) runs
    """
    raise NotImplementedError('Meec! Demultiplexing for HiSeq (V3/V4) runs not implemented yet :-/')


def demultiplex_MiSeq(run):
    """ Demultiplexing for MiSeq runs
    """
    raise NotImplementedError('Meec! Demultiplexing for MiSeq runs not implemented yet :-/')


class Run(object):
    """ Defines an Illumina run
    """
    def __init__(self, run_dir):
        if not os.path.exists(run_dir) or not \
                os.path.exists(os.path.join(run_dir, 'runParameters.xml')):
            raise RuntimeError('Could not locate run directory {}'.format(run_dir))
        self.run_dir = run_dir
        self.id = os.path.basename(run_dir)
        self._extract_run_info()

        if self.run_type == 'HiSeq':
            self.demultiplex = demultiplex_HiSeq
        elif self.run_type == 'HiSeq X':
            self.demultiplex = demultiplex_HiSeq_X
        elif self.demultiplex == 'MiSeq':
            self.demultiplex = demultiplex_MiSeq


    def _extract_run_info(self):
        """ Extracts run info from runParameters.xml and adds it to the class attributes

        TODO: This method could be used to extract A LOT of information about the
        run and maybe... populate statusdb or similar? Just leaving this comment here
        till now...
        """

        run_parameters = ET.parse(os.path.join(self.run_dir, 'runParameters.xml')).getroot().find('Setup')
        # HiSeq and HiSeq X runParameter.xml files will have a Flowcell child with run type info
        run_type = run_parameters.find('Flowcell')
        # But MiSeqs doesn't...
        if run_type is None:
            run_type = run_parameters.find('ApplicationName')

        try:
            if 'HiSeq X' in run_type.text:
                self.run_type = 'HiSeq X'
            elif 'HiSeq Flow Cell' in run_type.text:
                self.run_type = 'HiSeq'
            elif 'MiSeq' in run_type.text:
                self.run_type = 'MiSeq'
        except AttributeError:
            raise RuntimeError('Run type could not be determined for run {}'.format(self.id))


    def is_finished(self):
        """ Returns true if the run is finished, false otherwise
        """
        return os.path.exists(os.path.join(self.run_dir, 'RTAComplete.txt'))

    @property
    def status(self):
        if self.run_type == 'HiSeq X':
            demux_dir = os.path.join(self.run_dir, 'Demultiplexing')
            if not os.path.exists(demux_dir):
                return 'TO_START'
            elif os.path.exists(os.path.join(demux_dir, 'Stats', 'DemultiplexingStats.xml')):
                return 'COMPLETED'
            else:
                return 'IN_PROGRESS'
        else:
            raise NotImplementedError('Sorry... no status method defined for {} runs'.format(self.run_type))
