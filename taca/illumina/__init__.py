""" Submodule with Illumina-related code
"""
import csv
import glob
import logging
import os

from datetime import datetime
from xml.etree import ElementTree as ET

from taca.illumina import utils
from taca.utils import misc
from taca.utils import parsers
from taca.utils.config import CONFIG
from taca.utils.filesystem import chdir

logger = logging.getLogger(__name__)

finished_run_indicator = CONFIG.get('storage', {}).get('finished_run_indicator',
                                                       'RTAComplete.txt')


def _run_casava_task(args):
    """Perform demultiplexing and generate fastq.gz files for the current
    flowecell using CASAVA (>1.8).
    """
    config = CONFIG['analysis']
    bp = args.get('bp')
    samples_group = args.get('samples')
    base_mask = samples_group['base_mask']
    samples = samples_group['samples']
    fc_dir = args.get('fc_dir')
    ss = 'SampleSheet_{bp}bp.csv'.format(bp=str(bp))
    _demux_folder = 'Unaligned'
    for option in CONFIG['analysis']['bcl2fastq']['options']:
        if isinstance(option, dict) and option.get('output-dir'):
            _demux_folder = option.get('output-dir')
    demux_folder = '{}_{}bp'.format(_demux_folder, str(bp))
    num_cores = config.get('make').get('num_cores')

    #Create separate samplesheet and folder
    with open(os.path.join(fc_dir, ss), 'w') as fh:
        samplesheet = csv.DictWriter(fh, fieldnames=samples['fieldnames'], dialect='excel')
        samplesheet.writeheader()
        samplesheet.writerows(samples['samples'])

    # Run configureBclToFastq
    with chdir(fc_dir):
        cl = [config.get('bcl2fastq').get(args.get('run_type'))]
        if config['bcl2fastq'].has_key('options'):
            cl_options = config['bcl2fastq']['options']

            # Append all options that appear in the configuration file to the main command.
            for option in cl_options:
                if isinstance(option, dict) and option:
                    opt, val = option.items()[0]
                    # In the case of [H/M]iSeq the final output-dir will be maned with
                    # a suffix which will be the length of the index
                    if opt == 'output-dir':
                        val = demux_folder
                    cl.extend(['--{}'.format(opt), str(val)])
                else:
                    cl.append('--{}'.format(option))
            # In the case of [H/M]iSeq we may have several samplesheet files,
            # built on runtime, so we have to specify them here, as well as the
            # basemask to be used
            cl.extend(["--sample-sheet", ss])
            cl.extend(["--use-bases-mask", ','.join(base_mask)])

        logger.info(("Running configureBclToFastq.pl for run {} on {}".format(
                      os.path.basename(os.path.basename(fc_dir)), datetime.now())))

        misc.call_external_command(cl, with_log_files=True)

    # Go to <Unaligned> folder
    with chdir(os.path.join(fc_dir, demux_folder)):
        # Perform make
        cl = ["make", "-j", str(num_cores)]

        logger.info(("Running make command for run {} on {}".format(
                      os.path.basename(os.path.basename(fc_dir)), datetime.now())))
        misc.call_external_command(cl, with_log_files=True)

    return demux_folder


def _run_casava(fc_dir, run_type):
    """Prepare and call the task to perform demultiplexing and generation of
    fastq.gz files for the current flowcell in using CASAVA (1.8).

    :param str fc_dir: Directory of the flowcell
    :param str run_type: Type of the run, i-e HiSeq or MiSeq
    """
    base_masks = utils.get_base_masks(fc_dir)

    #Prepare the list of arguments to call configureBclToFastq
    args_list = []
    [args_list.append({'bp': k, 'samples': v, 'fc_dir': fc_dir, 'run_type': run_type}) \
                        for k, v in base_masks.iteritems()]

    unaligned_dirs = map(_run_casava_task, args_list)

    return unaligned_dirs

def _demultiplex_HiSeqX_flowcell(run):
    """Specific method for demultiplexing a HiSeqX flowcell

    :param taca.illumina.Run run: Run/flowcell to be demultiplexed
    """
    logger.info('Building bcl2fastq command')
    config = CONFIG['analysis']
    with chdir(self.run_dir):
        cl = [config.get('bcl2fastq').get(self.run_type)]
        if config['bcl2fastq'].has_key('options'):
            cl_options = config['bcl2fastq']['options']

            # Append all options that appear in the configuration file to the main command.
            # Options that require a value, i.e --use-bases-mask Y8,I8,Y8, will be returned
            # as a dictionary, while options that doesn't require a value, i.e --no-lane-splitting
            # will be returned as a simple string
            for option in cl_options:
                if isinstance(option, dict):
                    opt, val = option.popitem()
                    cl.extend(['--{}'.format(opt), str(val)])
                else:
                    cl.append('--{}'.format(option))

        logger.info(("BCL to FASTQ conversion and demultiplexing started for "
                     " run {} on {}".format(os.path.basename(self.id), datetime.now())))

        misc.call_external_command_detached(cl, with_log_files=True)


def _demultiplex_flowcell(run):
    """Sepecific method for demultiplexing a non-X10 flowcell, i.e [H/M]iSeq

    :param taca.illumina.Run run: Run/flowcell to be demultiplexed
    """
    logger.info('Generating FASTQ files for run {}'.format(run.id))
    demux_dirs = _run_casava(run.run_dir, run.run_type)
    logger.info("Done generating fastq.gz files for {}".format(run.id))
    # Merge demultiplexing results into a single Unaligned folder
    utils.merge_demux_results(run.run_dir)


class Run(object):
    """ Defines an Illumina run
    """
    def __init__(self, run_dir):
        if not os.path.exists(run_dir) or not \
                os.path.exists(os.path.join(run_dir, 'runParameters.xml')):
            raise RuntimeError('Could not locate run directory {}'.format(run_dir))
        self.run_dir = os.path.abspath(run_dir)
        self.id = os.path.basename(os.path.normpath(run_dir))
        self._extract_run_info()


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
                self.run_type = 'HiSeqX'
            elif 'HiSeq Flow Cell' in run_type.text:
                self.run_type = 'HiSeq'
            elif 'MiSeq' in run_type.text:
                self.run_type = 'MiSeq'
        except AttributeError:
            raise RuntimeError('Run type could not be determined for run {}'.format(self.id))


    def is_finished(self):
        """ Returns true if the run is finished, false otherwise
        """
        return os.path.exists(os.path.join(self.run_dir, finished_run_indicator))


    def demultiplex(self):
        """Perform demultiplexing of the flowcell.

        Takes software (bcl2fastq version to use) and parameters from the configuration
        file.
        """
        if self.run_type == 'HiSeqX':
            _demultiplex_HiSeqX_flowcell(self)
        else:
            _demultiplex_flowcell(self)


    @property
    def status(self):
        _demux_dir = 'Demultiplexing'
        for option in CONFIG['analysis']['bcl2fastq']['options']:
            if isinstance(option, dict) and option.get('output-dir'):
                _demux_dir = option.get('output-dir')
        if self.run_type == 'HiSeqX':
            demux_dir = os.path.join(self.run_dir, _demux_dir)
            if not os.path.exists(demux_dir):
                return 'TO_START'
            elif os.path.exists(os.path.join(demux_dir, 'Stats', 'DemultiplexingStats.xml')):
                return 'COMPLETED'
            else:
                return 'IN_PROGRESS'
        elif self.run_type == 'HiSeq' or self.run_type == 'MiSeq':
            # Given how demultiplexing is implemented for [H/M]iSeq, the demux_dir
            # will only be present at the end, when merging the resulting demux_dir_Xbp directories
            demux_dir = os.path.join(self.run_dir, _demux_dir)
            if not glob.glob('{}_*'.format(demux_dir)):
                return 'TO_START'
            elif os.path.exists(demux_dir):
                return 'COMPLETED'
            else:
                return 'IN_PROGRESS'
        else:
            raise NotImplementedError('Sorry... no status method defined for {} runs'.format(self.run_type))
