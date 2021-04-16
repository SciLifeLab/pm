#!/usr/bin/env python
import unittest
import logging
import filecmp
import mock
import os

from taca.analysis.analysis_nanopore import *
from taca.utils import config as conf


CONFIG = conf.load_yaml_config('data/taca_test_nanopore_cfg.yaml')

class TestNanoporeAnalysis(unittest.TestCase):

    def test_find_runs_to_process(self):
        """Find all expected nanopore runs to process."""
        expected_dirs = ["data/nanopore_data/run1/still_sequencing/20200101_1412_MN19414_AAU641_68125dc2",
                         "data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2",
                         "data/nanopore_data/run2/done_sequencing/20200102_1412_MN19414_AAU642_68125dc2",
                         "data/nanopore_data/run3/demultiplexing/20200103_1412_MN19414_AAU643_68125dc2",
                         "data/nanopore_data/run5/done_demuxing/20200104_1412_MN19414_AAU645_68125dc2",
                         "data/nanopore_data/run7/done_no_sample_sheet/20200107_1412_MN19417_AAU645_68125dc2",
                         "data/nanopore_data/run8/demux_failed/20200108_1412_MN19414_AAU648_68125dc2"]
        found_dirs = find_runs_to_process()
        self.assertEqual(sorted(found_dirs), sorted(expected_dirs))

    @mock.patch('taca.analysis.analysis_nanopore.parse_samplesheet')
    def test_parse_lims_sample_sheet(self, mock_parser):
        """Find and parse lims sample sheet."""
        mock_parser.return_value = ('data/nanopore_data/run2/done_sequencing/20200102_1412_MN19414_AAU642_68125dc2/SQK-LSK109_sample_sheet.csv',
                                    'data/nanopore_data/run2/done_sequencing/20200102_1412_MN19414_AAU642_68125dc2/anglerfish_sample_sheet.csv')
        run_dir = 'data/nanopore_data/run2/done_sequencing/20200102_1412_MN19414_AAU642_68125dc2'
        nanoseq_sample_sheet, anglerfish_sample_sheet = parse_lims_sample_sheet(run_dir)
        self.assertEqual(nanoseq_sample_sheet, 'data/nanopore_data/run2/done_sequencing/20200102_1412_MN19414_AAU642_68125dc2/SQK-LSK109_sample_sheet.csv')
        self.assertEqual(anglerfish_sample_sheet, 'data/nanopore_data/run2/done_sequencing/20200102_1412_MN19414_AAU642_68125dc2/anglerfish_sample_sheet.csv')

    def test_get_original_samplesheet(self):
        """Get location of lims sample sheet."""
        run_id = '20200102_1412_MN19414_AAU642_68125dc2'
        found_sample_sheet = get_original_samplesheet(run_id)
        expected_sample_sheet = 'data/nanopore_samplesheets/2020/SQK-LSK109_AAU642_Samplesheet_22-594126.csv'
        self.assertEqual(found_sample_sheet, expected_sample_sheet)

    def test_parse_samplesheet(self):
        """Make nanoseq sample sheet from lims sample sheet."""
        run_dir = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2'
        lims_samplesheet = 'data/nanopore_samplesheets/2020/SQK-LSK109_AAU644_Samplesheet_24-594126.csv'
        anglerfish_sample_sheet = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2/anglerfish_sample_sheet.csv'
        nanoseq_samplesheet = parse_samplesheet(run_dir, lims_samplesheet)
        self.assertTrue(filecmp.cmp(nanoseq_samplesheet, 'data/nanopore_samplesheets/expected/SQK-LSK109_sample_sheet.csv'))
        self.assertTrue(filecmp.cmp(anglerfish_sample_sheet, 'data/nanopore_samplesheets/expected/anglerfish_sample_sheet.csv'))

    @mock.patch('taca.analysis.analysis_nanopore.get_flowcell_id')
    @mock.patch('taca.analysis.analysis_nanopore.is_multiplexed')
    @mock.patch('taca.analysis.analysis_nanopore.subprocess.Popen')
    def test_start_analysis_pipeline_multiplexed(self, mock_popen, mock_is_multiplexed, mock_get_id):
        """Submit detached nanoseq job for multiplexed data."""
        mock_get_id.return_value = 'FLO-FLG001'
        mock_is_multiplexed.return_value = True
        run_dir = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2'
        sample_sheet = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2/SQK-LSK109_sample_sheet.csv'
        start_nanoseq(run_dir, sample_sheet)
        expected_parameters = ('nextflow run nf-core/nanoseq --input ' + sample_sheet
                               + ' --input_path ' + os.path.join(run_dir, 'fast5')
                               + ' --outdir ' + os.path.join(run_dir, 'nanoseq_output')
                               + ' --flowcell FLO-FLG001'
                               + ' --guppy_gpu'
                               + ' --skip_alignment'
                               + ' --kit SQK-LSK109'
                               + ' --max_cpus 6'
                               + ' --max_memory 20.GB'
                               + ' --barcode_kit EXP-NBD104'
                               + ' -profile singularity; echo $? > .exitcode_for_nanoseq')
        mock_popen.assert_called_once_with(expected_parameters, stdout=subprocess.PIPE, shell=True, cwd=run_dir)

    @mock.patch('taca.analysis.analysis_nanopore.get_flowcell_id')
    @mock.patch('taca.analysis.analysis_nanopore.is_multiplexed')
    @mock.patch('taca.analysis.analysis_nanopore.subprocess.Popen')
    def test_start_analysis_pipeline_not_multiplexed(self, mock_popen, mock_is_multiplexed, mock_get_id):
        """Submit detached nanoseq job for non multiplexed data."""
        mock_get_id.return_value = 'FLO-FLG001'
        mock_is_multiplexed.return_value = False
        run_dir = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2'
        sample_sheet = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2/SQK-LSK109_sample_sheet.csv'
        start_nanoseq(run_dir, sample_sheet)
        expected_parameters = ('nextflow run nf-core/nanoseq --input ' + sample_sheet
                               + ' --input_path ' + os.path.join(run_dir, 'fast5')
                               + ' --outdir ' + os.path.join(run_dir, 'nanoseq_output')
                               + ' --flowcell FLO-FLG001'
                               + ' --guppy_gpu'
                               + ' --skip_alignment'
                               + ' --kit SQK-LSK109'
                               + ' --max_cpus 6'
                               + ' --max_memory 20.GB'
                               + ' -profile singularity; echo $? > .exitcode_for_nanoseq')
        mock_popen.assert_called_once_with(expected_parameters, stdout=subprocess.PIPE, shell=True, cwd=run_dir)

    def test_get_flowcell_id(self):
        """Get flowcell ID from report.md."""
        run_dir = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2'
        got_id = get_flowcell_id(run_dir)
        expected_id = 'FLO-FLG001'
        self.assertEqual(got_id, expected_id)

    def test_is_multiplexed(self):
        """Return True if run is multiplexed, else False."""
        multiplexed_sample_sheet = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2/SQK-LSK109_sample_sheet.csv'
        non_multiplexed_sample_sheet = 'data/nanopore_data/run3/demultiplexing/20200103_1412_MN19414_AAU643_68125dc2/SQK-LSK109_AAU643_sample_sheet.csv'
        self.assertTrue(is_multiplexed(multiplexed_sample_sheet))
        self.assertFalse(is_multiplexed(non_multiplexed_sample_sheet))

    def test_get_barcode_kit(self):
        """Return EXP-NBD104 or EXP-NBD114 barcode kit based on sample sheet."""
        sample_sheet_104 = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2/SQK-LSK109_sample_sheet.csv'
        got_kit_104 = get_barcode_kit(sample_sheet_104)
        sample_sheet_114 = 'data/nanopore_data/run8/demux_failed/20200108_1412_MN19414_AAU648_68125dc2/SQK-LSK109_sample_sheet.csv'
        got_kit_114 = get_barcode_kit(sample_sheet_114)
        self.assertEqual(got_kit_104, 'EXP-NBD104')
        self.assertEqual(got_kit_114, 'EXP-NBD114')

    def test_check_exit_status(self):
        """Check nanoseq exit status from file."""
        self.assertTrue(check_exit_status('data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2/.exitcode_for_nanoseq'))
        self.assertFalse(check_exit_status('data/nanopore_data/run8/demux_failed/20200108_1412_MN19414_AAU648_68125dc2/.exitcode_for_nanoseq'))

    @mock.patch('taca.analysis.analysis_nanopore.os.makedirs')
    @mock.patch('taca.analysis.analysis_nanopore.subprocess.Popen')
    def test_start_anglerfish(self, mock_popen, mock_mkdir):
        """Start Anglerfish."""
        run_dir = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2'
        af_sample_sheet = 'anglerfish_sample_sheet.csv'
        output_dir = 'anglerfish_output'
        start_anglerfish(run_dir, af_sample_sheet, output_dir)
        expected_parameters = ('anglerfish.py'
                          + ' --samplesheet anglerfish_sample_sheet.csv'
                          + ' --out_fastq anglerfish_output'
                          + ' --threads 2'
                          + ' --skip_demux'
                          + ' --skip_fastqc; echo $? > .exitcode_for_anglerfish')
        mock_popen.assert_called_once_with(expected_parameters, stdout=subprocess.PIPE, shell=True, cwd=run_dir)

    @mock.patch('taca.analysis.analysis_nanopore.find_anglerfish_results')
    @mock.patch('taca.analysis.analysis_nanopore.shutil.copyfile')
    def test_copy_results_for_lims(self, mock_copy, mock_results):
        """Copy Anglerfish results to lims."""
        run = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2'
        anglerfish_results_path = 'anglerfish_output'
        anglerfish_results_file = os.path.join(run, anglerfish_results_path, 'anglerfish_2020_09_23_141922', 'anglerfish_stats.txt')
        lims_results_file = 'some/dir/2020/anglerfish_stats_AAU644.txt'
        mock_results.return_value = anglerfish_results_file
        copy_results_for_lims(run, anglerfish_results_path)
        mock_copy.assert_called_once_with(anglerfish_results_file, lims_results_file)

    def test_find_anglerfish_results(self):
        """Locate Anglerfish results file."""
        anglerfish_dir = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2/anglerfish_output'
        found_file = find_anglerfish_results(anglerfish_dir)
        expected_file = os.path.join(anglerfish_dir, 'anglerfish_2020_09_23_141922', 'anglerfish_stats.txt')
        self.assertEqual(expected_file, found_file)

    def test_is_not_transferred(self):
        """Check if nanopore run has been transferred."""
        self.assertTrue(is_not_transferred('20200104_1412_MN19414_AAU644_68125dc2', 'data/nanopore_data/transfer.tsv'))
        self.assertFalse(is_not_transferred('20200105_1412_MN19414_AAU645_68125dc2', 'data/nanopore_data/transfer.tsv'))

    @mock.patch('taca.analysis.analysis_nanopore.RsyncAgent')
    def test_transfer_run(self, mock_rsync):
        """Start rsync of finished run."""
        run_dir = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2'
        transfer_run(run_dir)
        rsync_opts = {'-Lav': None,
                      '--chown': ':ngi2016003',
                      '--chmod' : 'Dg+s,g+rw',
                      '-r' : None,
                      '--exclude' : 'work'}
        mock_rsync.assert_called_with(run_dir,
                                      dest_path='some_dir',
                                      remote_host='some_host',
                                      remote_user='some_user',
                                      validate=False,
                                      opts=rsync_opts)

    @mock.patch('taca.analysis.analysis_nanopore.shutil.move')
    def test_archive_run(self, mock_move):
        """Move directory to archive."""
        run_dir = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2'
        archive_run(run_dir)
        mock_move.assert_called_once_with('data/nanopore_data/run4', 'data/nanopore_data/nosync')

    @mock.patch('taca.analysis.analysis_nanopore.parse_lims_sample_sheet')
    @mock.patch('taca.analysis.analysis_nanopore.os.path.isfile')
    @mock.patch('taca.analysis.analysis_nanopore.start_nanoseq')
    def test_process_run_start_analysis(self, mock_start, mock_isfile, mock_parse_ss):
        """Start nanoseq analysis."""
        nanoseq_sample_sheet = 'data/nanopore_data/run2/done_sequencing/20200102_1412_MN19414_AAU642_68125dc2/SQK-LSK109_sample_sheet.csv'
        anglerfish_sample_sheet = 'some/path'
        mock_parse_ss.return_value = nanoseq_sample_sheet
        mock_isfile.return_value = True
        run_dir = 'data/nanopore_data/run2/done_sequencing/20200102_1412_MN19414_AAU642_68125dc2'
        process_run(run_dir, None, None)
        mock_start.assert_called_once_with(run_dir, nanoseq_sample_sheet)

    @mock.patch('taca.analysis.analysis_nanopore.transfer_run')
    @mock.patch('taca.analysis.analysis_nanopore.update_transfer_log')
    @mock.patch('taca.analysis.analysis_nanopore.archive_run')
    @mock.patch('taca.analysis.analysis_nanopore.send_mail')
    def test_process_run_transfer(self, mock_mail, mock_archive, mock_update, mock_transfer):
        """Start transfer of run directory."""
        mock_transfer.return_value = True
        run_dir = 'data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2'
        process_run(run_dir, 'dummy/path', None)
        email_subject = ('Run successfully processed: 20200104_1412_MN19414_AAU644_68125dc2')
        email_message = 'Run 20200104_1412_MN19414_AAU644_68125dc2 has been analysed, transferred and archived successfully.'
        email_recipients = 'test@test.com'
        mock_mail.assert_called_once_with(email_subject, email_message, email_recipients)

    @mock.patch('taca.analysis.analysis_nanopore.send_mail')
    def test_process_run_fail_analysis(self, mock_mail):
        """Send email to operator if nanoseq analysis failed."""
        run_dir = 'data/nanopore_data/run8/demux_failed/20200108_1412_MN19414_AAU648_68125dc2'
        process_run(run_dir, None, None)
        email_subject = ('Analysis failed for run 20200108_1412_MN19414_AAU648_68125dc2')
        email_message = 'The nanoseq analysis failed for run {}.'.format(run_dir)
        email_recipients = 'test@test.com'
        mock_mail.assert_called_once_with(email_subject, email_message, email_recipients)

    @mock.patch('taca.analysis.analysis_nanopore.start_anglerfish')
    @mock.patch('taca.analysis.analysis_nanopore.start_nanoseq')
    @mock.patch('taca.analysis.analysis_nanopore.find_runs_to_process')
    @mock.patch('taca.analysis.analysis_nanopore.transfer_run')
    @mock.patch('taca.analysis.analysis_nanopore.update_transfer_log')
    @mock.patch('taca.analysis.analysis_nanopore.parse_lims_sample_sheet')
    @mock.patch('taca.analysis.analysis_nanopore.archive_run')
    @mock.patch('taca.analysis.analysis_nanopore.send_mail')
    def test_process_run_preprocessing_still_sequencing(self, mock_mail, mock_archive, mock_parser, mock_update, mock_transfer,
                                                        mock_find_runs, mock_start_nanoseq, mock_start_anglerfish):
        """The main entry point should not start nanoseq since sequencing is ongoing. """

        mock_parser.return_value = 'data/nanopore_samplesheets/expected/SQK-LSK109_sample_sheet.csv'

        expected_dirs = ["data/nanopore_data/run1/still_sequencing/20200101_1412_MN19414_AAU641_68125dc2",
                         "data/nanopore_data/run2/done_sequencing/20200102_1412_MN19414_AAU642_68125dc2",
                         "data/nanopore_data/run3/demultiplexing/20200103_1412_MN19414_AAU643_68125dc2",
                         "data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2",
                         "data/nanopore_data/run5/done_demuxing/20200104_1412_MN19414_AAU645_68125dc2",
                         "data/nanopore_data/run7/done_no_sample_sheet/20200107_1412_MN19417_AAU645_68125dc2",
                         "data/nanopore_data/run8/demux_failed/20200108_1412_MN19414_AAU648_68125dc2"]

        mock_find_runs.return_value = expected_dirs
        run_preprocessing(None, None, None)
        self.assertEqual(mock_start_anglerfish.call_count, 1)
        self.assertEqual(mock_start_nanoseq.call_count, 0)

    @mock.patch('taca.analysis.analysis_nanopore.start_anglerfish')
    @mock.patch('taca.analysis.analysis_nanopore.start_nanoseq')
    @mock.patch('taca.analysis.analysis_nanopore.find_runs_to_process')
    @mock.patch('taca.analysis.analysis_nanopore.transfer_run')
    @mock.patch('taca.analysis.analysis_nanopore.update_transfer_log')
    @mock.patch('taca.analysis.analysis_nanopore.parse_lims_sample_sheet')
    @mock.patch('taca.analysis.analysis_nanopore.archive_run')
    @mock.patch('taca.analysis.analysis_nanopore.send_mail')
    def test_process_run_preprocessing_done_sequencing(self, mock_mail, mock_archive, mock_parser, mock_update, mock_transfer,
                                                        mock_find_runs, mock_start_nanoseq, mock_start_anglerfish):
        """The main entry point should start one nanoseq when sequencing is not ongoing. """

        mock_parser.return_value = 'data/nanopore_samplesheets/expected/SQK-LSK109_sample_sheet.csv'

        expected_dirs = ["data/nanopore_data/run2/done_sequencing/20200102_1412_MN19414_AAU642_68125dc2",
                         "data/nanopore_data/run3/demultiplexing/20200103_1412_MN19414_AAU643_68125dc2",
                         "data/nanopore_data/run4/done_demuxing/20200104_1412_MN19414_AAU644_68125dc2",
                         "data/nanopore_data/run5/done_demuxing/20200104_1412_MN19414_AAU645_68125dc2",
                         "data/nanopore_data/run7/done_no_sample_sheet/20200107_1412_MN19417_AAU645_68125dc2",
                         "data/nanopore_data/run8/demux_failed/20200108_1412_MN19414_AAU648_68125dc2"]

        mock_find_runs.return_value = expected_dirs
        run_preprocessing(None, None, None)
        self.assertEqual(mock_start_anglerfish.call_count, 1)
        self.assertEqual(mock_start_nanoseq.call_count, 1)
