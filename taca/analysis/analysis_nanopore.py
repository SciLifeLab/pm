"""Nanopore analysis methods for TACA."""
import os
import logging
import glob
import csv
import subprocess
import shutil
import smtplib

from datetime import datetime
from taca.utils.config import CONFIG
from taca.utils.minion_barcodes import BARCODES
from taca.utils.transfer import RsyncAgent, RsyncError
from taca.utils.misc import send_mail

logger = logging.getLogger(__name__)

def find_runs_to_process():
    """Find nanopore runs to process."""
    nanopore_data_dir = CONFIG.get('nanopore_analysis').get('data_dir')[0]
    found_run_dirs = []
    # These directories were found on the server, not sure which are actually created by the MinKnow software
    # but I'm taking a conservative approach and skipping all of them. None of them are good names for projects
    # anyhow. 'nosync' is created and used by TACA.
    skip_dirs = ['core-dump-db', 'intermediate', 'nosync', 'queued_reads', 'reads', 'user_scripts']
    try:
        found_top_dirs = [os.path.join(nanopore_data_dir, top_dir) for top_dir in os.listdir(nanopore_data_dir)
                          if os.path.isdir(os.path.join(nanopore_data_dir, top_dir))
                          and top_dir not in skip_dirs]
    except OSError:
        logger.warn('There was an issue locating the following directory: {}. '
                    'Please check that it exists and try again.'.format(nanopore_data_dir))
    # Get the actual location of the run directories in /var/lib/MinKnow/data/USERDETERMINEDNAME/USERDETSAMPLENAME/run
    if found_top_dirs:
        for top_dir in found_top_dirs:
            for sample_dir in os.listdir(top_dir):
                for run_dir in os.listdir(os.path.join(top_dir, sample_dir)):
                    found_run_dirs.append(os.path.join(top_dir, sample_dir, run_dir))
    else:
        logger.warn('Could not find any run directories in {}'.format(nanopore_data_dir))
    return found_run_dirs


def check_ongoing_sequencing(run_dir):
    """Check if sequencing is ongoing for the given run dir """
    summary_file = glob.glob(run_dir + '/final_summary*.txt')

    if not len(summary_file):
        logger.info('Sequencing ongoing for run {}. Will not start nanoseq at this time'.format(run_dir))
        return True
    else:
        return False


def process_run(run_dir, nanoseq_sample_sheet, anglerfish_sample_sheet, sequencing_ongoing=False, nanoseq_ongoing=False):
    """Process nanopore runs.

    Will not start nanoseq if a sequencing run is ongoing, to limit memory usage.
    Will also maximum start one nanoseq run at once, for the same reason.
    """
    qc_run = True
    if nanoseq_sample_sheet and not anglerfish_sample_sheet:
        qc_run = False

    logger.info('Processing run: {} as a {}'.format(run_dir, 'QC run' if qc_run else 'non-QC run'))
    summary_file = glob.glob(run_dir + '/final_summary*.txt')
    nanoseq_dir = os.path.join(run_dir, 'nanoseq_output')
    anglerfish_dir = os.path.join(run_dir, 'anglerfish_output')
    anglerfish_sample_sheet = os.path.join(run_dir, 'anglerfish_sample_sheet.csv')
    nanoseq_exit_status_file = os.path.join(run_dir, '.exitcode_for_nanoseq')
    anglerfish_exit_status_file = os.path.join(run_dir, '.exitcode_for_anglerfish')
    email_recipients = CONFIG.get('mail').get('recipients')

    if len(summary_file) and os.path.isfile(summary_file[0]) and not os.path.isdir(nanoseq_dir):
        logger.info('Sequencing done for run {}. Attempting to start analysis.'.format(run_dir))
        if not nanoseq_sample_sheet:
            nanoseq_sample_sheet = parse_lims_sample_sheet(run_dir)

        if os.path.isfile(nanoseq_sample_sheet):
            if nanoseq_ongoing:
                logger.warn('Nanoseq already started, will not attempt to start for {}'.format(run_dir))
            elif sequencing_ongoing:
                logger.warn('Sequencing ongoing, will not attempt to start Nanoseq for {}'.format(run_dir))
            else:
                start_nanoseq(run_dir, nanoseq_sample_sheet)
                nanoseq_ongoing = True
        else:
            logger.warn('Samplesheet not found for run {}. Operator notified. Skipping.'.format(run_dir))
            email_subject = ('Samplesheet missing for run {}'.format(os.path.basename(run_dir)))
            email_message = 'There was an issue locating the samplesheet for run {}.'.format(run_dir)
            send_mail(email_subject, email_message, email_recipients)

    elif os.path.isdir(nanoseq_dir) and not os.path.isfile(nanoseq_exit_status_file):
        logger.info('Nanoseq has started for run {} but is not yet done. Skipping.'.format(run_dir))

    elif os.path.isdir(nanoseq_dir) and os.path.isfile(nanoseq_exit_status_file):
        nanoseq_successful = check_exit_status(nanoseq_exit_status_file)
        if nanoseq_successful:
            run_id = os.path.basename(run_dir)
            transfer_log = CONFIG.get('nanopore_analysis').get('transfer').get('transfer_file')

            if qc_run and not os.path.isdir(anglerfish_dir):
                logger.info('Nanoseq done for run {}. Attempting to start Anglerfish.'.format(run_id))
                start_anglerfish(run_dir, anglerfish_sample_sheet, anglerfish_dir)

            elif qc_run and not os.path.isfile(anglerfish_exit_status_file):
                logger.info('Anglerfish has started for run {} but is not yet done. Skipping.'.format(run_id))

            elif qc_run and os.path.isfile(anglerfish_exit_status_file):
                anglerfish_successful = check_exit_status(anglerfish_exit_status_file)
                if anglerfish_successful:
                    copy_results_for_lims(run_dir, anglerfish_dir)
                    logger.info('Anglerfish finished OK for run {}. Notifying operator.'.format(run_id))
                    email_subject = ('Anglerfish successfully processed run {}'.format(os.path.basename(run_id)))
                    email_message = ('Anglerfish has successfully finished for run {}. Please '
                                     'finish the QC step in lims.').format(run_id)
                    send_mail(email_subject, email_message, email_recipients)

                    if is_not_transferred(run_id, transfer_log):
                        if transfer_run(run_dir):
                            update_transfer_log(run_id, transfer_log)
                            logger.info('Run {} has been synced to the analysis cluster.'.format(run_id))
                            archive_run(run_dir)
                            logger.info('Run {} is finished and has been archived. Notifying operator.'.format(run_id))
                            email_subject = ('Run successfully processed: {}'.format(os.path.basename(run_id)))
                            email_message = ('Run {} has been analysed, transferred and archived '
                                             'successfully.').format(run_id)
                            send_mail(email_subject, email_message, email_recipients)

                        else:
                            logger.warn('An error occurred during transfer of run {} '
                                        'to Irma. Notifying operator.'.format(run_dir))
                            email_subject = ('Run processed with errors: {}'.format(os.path.basename(run_id)))
                            email_message = ('Run {} has been analysed, but an error occurred during '
                                             'transfer.').format(run_id)
                            send_mail(email_subject, email_message, email_recipients)

                    else:
                        logger.warn('The following run has already been transferred, '
                                    'skipping: {}'.format(run_dir))

                else:
                    logger.warn('Anglerfish exited with a non-zero exit status for run {}. '
                                'Notifying operator.'.format(run_dir))
                    email_subject = ('Run processed with errors: {}'.format(os.path.basename(run_id)))
                    email_message = ('Anglerfish exited with errors for run {}. Please '
                                     'check the log files and restart.').format(run_id)
                    send_mail(email_subject, email_message, email_recipients)

            elif not qc_run:
                if is_not_transferred(run_id, transfer_log):
                    if transfer_run(run_dir):
                        update_transfer_log(run_id, transfer_log)
                        logger.info('Run {} has been synced to the analysis cluster.'.format(run_id))
                        archive_run(run_dir)
                        logger.info('Run {} is finished and has been archived. Notifying operator.'.format(run_id))
                        email_subject = ('Run successfully processed: {}'.format(run_id))
                        email_message = ('Run {} has been analysed, transferred and archived '
                                         'successfully.').format(run_id)
                        send_mail(email_subject, email_message, email_recipients)

                    else:
                        logger.warn('An error occurred during transfer of run {} '
                                    'to Irma. Notifying operator.'.format(run_dir))
                        email_subject = ('Run processed with errors: {}'.format(run_id))
                        email_message = ('Run {} has been analysed, but an error occurred during '
                                         'transfer.').format(run_id)
                        send_mail(email_subject, email_message, email_recipients)

                else:
                    logger.warn('The following run has already been transferred, '
                                'skipping: {}'.format(run_id))

        else:
            logger.warn('Nanoseq exited with a non-zero exit status for run {}. '
                        'Notifying operator.'.format(run_dir))
            email_subject = ('Analysis failed for run {}'.format(os.path.basename(run_dir)))
            email_message = 'The nanoseq analysis failed for run {}.'.format(run_dir)
            send_mail(email_subject, email_message, email_recipients)

    else:
        logger.info('Run {} not finished sequencing yet. Skipping.'.format(run_dir))

    return nanoseq_ongoing


def parse_lims_sample_sheet(run_dir):
    """Generate nanoseq samplesheet based on Lims original."""
    run_id = os.path.basename(run_dir)
    lims_samplesheet = get_original_samplesheet(run_id)
    if lims_samplesheet:
        nanoseq_samplesheet_location = parse_samplesheet(run_dir, lims_samplesheet)
    else:
        nanoseq_samplesheet_location = ''
        anglerfish_samplesheet_location = ''
    return nanoseq_samplesheet_location


def get_original_samplesheet(run_id):
    """Find original lims sample sheet."""
    year_processed = run_id[0:4]
    flowcell_id = run_id.split('_')[3]
    lims_samplesheet_dir = os.path.join(CONFIG.get('nanopore_analysis').get('samplesheets_dir'),
                                        year_processed)
    found_samplesheets = glob.glob(lims_samplesheet_dir + '/*' + flowcell_id + '*')
    if not found_samplesheets:
        logger.warn('No Lims sample sheets found for run {}'.format(run_id))
        return
    elif len(found_samplesheets) > 1:
        logger.warn('Found more than one Lims sample sheets for run {}'.format(run_id))
        return
    return found_samplesheets[0]


def parse_samplesheet(run_dir, lims_samplesheet):
    """Parse Lims samplesheet into one suitable for nanoseq and anglerfish."""
    nanopore_kit = os.path.basename(lims_samplesheet).split('_')[0]
    nanoseq_samplesheet = os.path.join(run_dir, nanopore_kit + '_sample_sheet.csv')
    anglerfish_samplesheet = os.path.join(run_dir, 'anglerfish_sample_sheet.csv')
    nanoseq_content = 'sample,fastq,barcode,genome,transcriptome'
    anglerfish_content = ''
    with open(lims_samplesheet, 'r') as f:
        lines = sorted(f.readlines())
        first_sample_name = lines[0].split(',')[0]  # Get name of first sample
        fastq_location = os.path.join(run_dir, 'nanoseq_output', 'guppy', 'fastq')  # Set the location of the first sample
        for line in lines:
            sample_name, nanoseq_barcode, run_type, illumina_barcode, location = line.split(',')  #TODO remove location once/if removed in lims
            if nanoseq_barcode and nanoseq_barcode in BARCODES:
                barcode = BARCODES[nanoseq_barcode]
                is_pool = False
            else:
                barcode = '0'
                is_pool = True
            nanoseq_content += '\n' + sample_name + ',,' + barcode + ',,'  # Only need sample and barcode for now.
            if illumina_barcode:
                # If there are no nanopore barcodes, the samples are from the same pool and will end up in
                # the same nanoseq output file named after the firts sample in the sample sheet
                if is_pool:
                    anglerfish_content += sample_name + ',' + run_type + ',' + illumina_barcode + ',' + os.path.join(fastq_location, first_sample_name + '.fastq.gz') + '\n'
                # If the samples are not the same pool, the nanoseq output is named by the barcode
                else:
                    anglerfish_content += sample_name + ',' + run_type + ',' + illumina_barcode + ',' + os.path.join(fastq_location, 'barcode' + barcode + '.fastq.gz') + '\n'
    with open(nanoseq_samplesheet, 'w') as f:
        f.write(nanoseq_content)
    if anglerfish_content:
        with open(anglerfish_samplesheet, 'w') as f:
            f.write(anglerfish_content)
    return nanoseq_samplesheet


def start_nanoseq(run_dir, sample_sheet):
    """Start Nanoseq analysis."""
    flowcell_id = get_flowcell_id(run_dir)
    kit_id = os.path.basename(sample_sheet).split('_')[0]
    if is_multiplexed(sample_sheet):
        logger.info('Run {} is multiplexed. Starting nanoseq with --barcode_kit option'.format(run_dir))
        barcode_kit = get_barcode_kit(sample_sheet)
        analysis_command = ('nextflow run nf-core/nanoseq --input ' + sample_sheet
                            + ' --input_path ' + os.path.join(run_dir, 'fast5')
                            + ' --outdir ' + os.path.join(run_dir, 'nanoseq_output')
                            + ' --flowcell ' + flowcell_id
                            + ' --guppy_gpu'
                            + ' --skip_alignment'
                            + ' --kit ' + kit_id
                            + ' --max_cpus 6'
                            + ' --max_memory 20.GB'
                            + ' --barcode_kit ' + barcode_kit
                            + ' -profile singularity; echo $? > .exitcode_for_nanoseq')
    else:
        logger.info('Run {} is not multiplexed. Starting nanoseq without --barcode_kit option'.format(run_dir))
        analysis_command = ('nextflow run nf-core/nanoseq --input ' + sample_sheet
                            + ' --input_path ' + os.path.join(run_dir, 'fast5')
                            + ' --outdir ' + os.path.join(run_dir, 'nanoseq_output')
                            + ' --flowcell ' + flowcell_id
                            + ' --guppy_gpu'
                            + ' --skip_alignment'
                            + ' --kit ' + kit_id
                            + ' --max_cpus 6'
                            + ' --max_memory 20.GB'
                            + ' -profile singularity; echo $? > .exitcode_for_nanoseq')

    try:
        p_handle = subprocess.Popen(analysis_command, stdout=subprocess.PIPE, shell=True, cwd=run_dir)
        logger.info('Started Nanoseq for run {}'.format(run_dir))
    except subprocess.CalledProcessError:
        logger.warn('An error occurred while starting the Nanoseq for run {}. '
                    'Please check the logfile for info.'.format(run_dir))
    return


def get_flowcell_id(run_dir):
    """Look for flow_cell_product_code in report.md and return the corresponding value."""
    report_file = glob.glob(run_dir + '/report*.md')[0]
    with open(report_file, 'r') as f:
        for line in f.readlines():
            if 'flow_cell_product_code' in line:
                return line.split('"')[3]


def is_multiplexed(sample_sheet):
    """Look in the sample_sheet and return True if the run was multiplexed, else False.
    Assumes that a run that has not been multiplexed has the barcode 0."""
    with open(sample_sheet, 'r') as f:
        for i, line in enumerate(f):
            if i == 1:  # Only need to check first non-header line
                line_entries = line.split(',')
    if line_entries[2] == '0':
        return False
    else:
        return True


def get_barcode_kit(sample_sheet):
    """Figure out which barcode kit was used. Assumes only one kit is ever used."""
    with open(sample_sheet, 'r') as f:
        for i, line in enumerate(f):
            if i == 1:  # Only need to check first non-header line
                line_entries = line.split(',')
    if int(line_entries[2]) <= 12:
        return 'EXP-NBD104'
    elif int(line_entries[2]) >= 13:
        return 'EXP-NBD114'
    barcode_kit = get_barcode_kit(sample_sheet)


def check_exit_status(status_file):
    """Read pipeline exit status file and return True if 0, False if anything else"""
    with open(status_file, 'r') as f:
        exit_status = f.readline().strip()
    return exit_status == '0'


def start_anglerfish(run_dir, af_sample_sheet, output_dir):
    """Start Anglerfish."""
    os.makedirs(output_dir)
    anglerfish_command = ('anglerfish.py'
                          + ' --samplesheet ' + af_sample_sheet
                          + ' --out_fastq ' + output_dir
                          + ' --threads 2'
                          + ' --skip_demux'
                          + ' --skip_fastqc; echo $? > .exitcode_for_anglerfish')
    try:
        p_handle = subprocess.Popen(anglerfish_command, stdout=subprocess.PIPE, shell=True, cwd=run_dir)
        logger.info('Started Anglerfish for run {} using: {}'.format(run_dir, anglerfish_command))
    except subprocess.CalledProcessError:
        logger.warn('An error occurred while starting the Anglerfish for run {}. '
                    'Please check the logfile for info.'.format(run_dir))
    return


def copy_results_for_lims(run_dir, anglerfish_results_dir):
    """Find results and copy to lims directory."""
    run_id = os.path.basename(run_dir)
    year_processed = run_id[0:4]
    flowcell_id = run_id.split('_')[3]
    lims_result_file = os.path.join(CONFIG.get('nanopore_analysis').get('lims_results_dir'),
                                    year_processed, 'anglerfish_stats_' + flowcell_id + '.txt')
    anglerfish_results = find_anglerfish_results(anglerfish_results_dir)
    try:
        shutil.copyfile(anglerfish_results, lims_result_file)
    except OSError as e:
        logger.warn('An error occurred while copying the Anglerfish results for {} to lims: {}'.format(run_id, e))
    return


def find_anglerfish_results(anglerfish_dir):
    """Return location of Anglerfish results."""
    results_file = ''
    for sub_dir in os.listdir(anglerfish_dir):
        if 'anglerfish_stats.txt' in os.listdir(os.path.join(anglerfish_dir, sub_dir)):
            results_file = os.path.join(anglerfish_dir, sub_dir, 'anglerfish_stats.txt')
            return results_file
    if not results_file:
        logger.warn('Could not find any Anglerfish results in {}'.format(anglerfish_dir))


def is_not_transferred(run_id, transfer_log):
    """Return True if run id not in transfer.tsv, else False."""
    with open(transfer_log, 'r') as f:
        return run_id not in f.read()


def transfer_run(run_dir):
    """rsync dir to Irma."""
    logger.info('Transferring run {} to analysis cluster'.format(run_dir))
    destination = CONFIG.get('nanopore_analysis').get('transfer').get('destination')
    rsync_opts = {'-Lav': None,
                  '--chown': ':ngi2016003',
                  '--chmod': 'Dg+s,g+rw',
                  '-r': None,
                  '--exclude': 'work'}
    connection_details = CONFIG.get('nanopore_analysis').get('transfer').get('analysis_server')
    transfer_object = RsyncAgent(run_dir,
                                 dest_path=destination,
                                 remote_host=connection_details['host'],
                                 remote_user=connection_details['user'],
                                 validate=False,
                                 opts=rsync_opts)
    try:
        transfer_object.transfer()
    except RsyncError:
        logger.warn('An error occurred while transferring {} to the '
                    'ananlysis server. Please check the logfiles'.format(run_dir))
        return False
    return True


def update_transfer_log(run_id, transfer_log):
    """Update transfer log with run id and date."""
    try:
        with open(transfer_log, 'a') as f:
            tsv_writer = csv.writer(f, delimiter='\t')
            tsv_writer.writerow([run_id, str(datetime.now())])
    except IOError:
        logger.warn('Could not update the transfer logfile for run {}. '
                    'Please make sure gets updated.'.format(run_id, transfer_log))
    return


def archive_run(run_dir):
    """Move directory to nosync."""
    logger.info('Archiving run ' + run_dir)
    archive_dir = CONFIG.get('nanopore_analysis').get('finished_dir')
    top_dir = '/'.join(run_dir.split('/')[0:-2])  # Get the project folder to archive
    try:                                          # Try pathlib (pathlib.Path(run_dir).parent.parent) when running completely on python3
        shutil.move(top_dir, archive_dir)
        logger.info('Successfully archived {}'.format(run_dir))
    except shutil.Error:
        logger.warn('An error occurred when archiving {}. '
                    'Please check the logfile for more info.'.format(run_dir))
    return


def run_preprocessing(run, nanoseq_sample_sheet, anglerfish_sample_sheet):
    """Find runs and kick off processing."""
    if run:
        process_run(os.path.abspath(run), nanoseq_sample_sheet, anglerfish_sample_sheet)
    else:
        runs_to_process = find_runs_to_process()
        sequencing_ongoing, nanoseq_ongoing = False, False
        for run_dir in runs_to_process:
            if check_ongoing_sequencing(run_dir):
                sequencing_ongoing = True
                break
        for run_dir in runs_to_process:
            nanoseq_ongoing = process_run(run_dir, nanoseq_sample_sheet, anglerfish_sample_sheet,
                                          sequencing_ongoing=sequencing_ongoing, nanoseq_ongoing=nanoseq_ongoing)
