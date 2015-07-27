#!/usr/bin/env python

import os
import shutil
import unittest

from datetime import datetime

from taca.analysis.analysis import *
from taca.illumina import Run

from taca.utils import config

# This is only run if TACA is called from the CLI, as this is a test, we need to
# call it explicitely
config.load_yaml_config(os.path.join(os.environ.get('HOME'), '.taca/taca.yaml'))

def processing_status(run_dir):
    demux_dir = os.path.join(run_dir, 'Demultiplexing')
    if not os.path.exists(demux_dir):
        return 'TO_START'
    elif os.path.exists(os.path.join(demux_dir, 'Stats', 'DemultiplexingStats.xml')):
        return 'COMPLETED'
    else:
        return 'IN_PROGRESS'

class TestTracker(unittest.TestCase):
    """ run_tracker.py script tests
    """
    @classmethod
    def setUpClass(self):
        """ Creates the following directory tree for testing purposes:

        tmp/
        |__ 141124_FINISHED_FCIDXX
        |   |__ RunInfo.xml
        |   |__ Demultiplexing
        |   |   |__ Stats
        |   |       |__ DemultiplexingStats.xml
        |   |__ RTAComplete.txt
        |__ 141124_IN_PROGRESS_FCIDXX
        |   |__ RunInfo.xml
        |   |__ Demultiplexing
        |   |__ RTAComplete.txt
        |__ 141124_RUNNING_FCIDXX
        |   |__ RunInfo.xml
        |__ 141124_TOSTART_FCIDXXX
            |__ RunInfo.xml
            |__ RTAComplete.txt
        """
        self.tmp_dir = 'tmp'
        self.transfer_file = os.path.join(self.tmp_dir, 'transfer.tsv')

        running = os.path.join(self.tmp_dir, '141124_RUNNING_FCIDXX')
        to_start = os.path.join(self.tmp_dir, '141124_TOSTART_FCIDXXX')
        in_progress = os.path.join(self.tmp_dir, '141124_IN_PROGRESS_FCIDXX')
        completed = os.path.join(self.tmp_dir, '141124_COMPLETED_FCIDXX')
        finished_runs = [to_start, in_progress, completed]

        # Create runs directory structure
        os.makedirs(self.tmp_dir)
        os.makedirs(running)
        os.makedirs(to_start)
        os.makedirs(os.path.join(in_progress, 'Demultiplexing'))
        os.makedirs(os.path.join(completed, 'Demultiplexing', 'Stats'))

        # Create files indicating that the run is finished
        for run in finished_runs:
            open(os.path.join(run, 'RTAComplete.txt'), 'w').close()

        # Create files indicating that the preprocessing is done
        open(os.path.join(completed, 'Demultiplexing', 'Stats', 'DemultiplexingStats.xml'), 'w').close()

        # Create transfer file and add the completed run
        with open(self.transfer_file, 'w') as f:
            tsv_writer = csv.writer(f, delimiter='\t')
            tsv_writer.writerow([os.path.basename(completed), str(datetime.now())])

        # Move sample RunInfo.xml file to every run directory
        for run in [running, to_start, in_progress, completed]:
            shutil.copy('data/RunInfo.xml', run)
            shutil.copy('data/runParameters.xml', run)

        # Create run objects
        self.running = Run(os.path.join(self.tmp_dir, '141124_RUNNING_FCIDXX'))
        self.to_start = Run(os.path.join(self.tmp_dir, '141124_TOSTART_FCIDXXX'))
        self.in_progress = Run(os.path.join(self.tmp_dir, '141124_IN_PROGRESS_FCIDXX'))
        self.completed = Run(os.path.join(self.tmp_dir, '141124_COMPLETED_FCIDXX'))
        self.finished_runs = [self.to_start, self.in_progress, self.completed]
        self.transfer_file = os.path.join(self.tmp_dir, 'transfer.tsv')

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.tmp_dir)

    def test_1_is_finished(self):
        """ Is finished should be True only if "RTAComplete.txt" file is present...
        """
        self.assertFalse(self.running.is_finished())
        self.assertTrue(all(map(lambda run: run.is_finished, self.finished_runs)))

    def test_2_processing_status(self):
        """ Status of the processing depends on the generated files
        """
        self.assertEqual('TO_START', self.running.status)
        self.assertEqual('TO_START', self.to_start.status)
        self.assertEqual('IN_PROGRESS', self.in_progress.status)
        self.assertEqual('COMPLETED', self.completed.status)

    def test_3_is_transferred(self):
        """ is_transferred should rely on the info in transfer.tsv
        """
        self.assertTrue(is_transferred(self.completed.id, self.transfer_file))
        self.assertFalse(is_transferred(self.running.id, self.transfer_file))
        self.assertFalse(is_transferred(self.to_start.id, self.transfer_file))
        self.assertFalse(is_transferred(self.in_progress.id, self.transfer_file))
