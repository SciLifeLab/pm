""" Divers utilities for Illumina runs processing """

import csv
import glob
import os
import re
import shutil
import xml.etree.ElementTree as ET


from bs4 import BeautifulSoup
from itertools import izip_longest

from taca.utils import parsers
from taca.utils import filesystem
from taca.utils.config import CONFIG


def last_index_read(directory):
    """Parse the number of the highest index read from the RunInfo.xml
    """
    read_numbers = [int(read.get("Number", 0)) for read in parsers.get_read_configuration(directory) if read.get("IsIndexedRead", "") == "Y"]
    return 0 if len(read_numbers) == 0 else max(read_numbers)


def get_base_masks(rundir):
    """ Return a set of base masks to be used when demultiplexing.

    :param str rundir: Path to the run directory.
    :returns list: Basemasks to be used based on the SampleSeet information.
    """
    runsetup = parsers.get_read_configuration(rundir, sort=True)
    flowcell_id = parsers.get_flowcell_id(rundir)
    base_masks = {}

    #Create groups of reads by index length
    ss_name = os.path.join(rundir, str(flowcell_id) + '.csv')
    if os.path.exists(ss_name):
        ss = csv.DictReader(open(ss_name, 'rb'), delimiter=',')
        samplesheet = []
        [samplesheet.append(read) for read in ss]
        for r in samplesheet:
            index_length = len(r['Index'].replace('-', '').replace('NoIndex', ''))
            if not base_masks.has_key(index_length):
                base_masks[index_length] = {'base_mask': [],
                                            'samples': {'fieldnames': ss.fieldnames, 'samples':[]}}
            base_masks[index_length]['samples']['samples'].append(r)

    #Create the basemask for each group
    for index_size, index_group in base_masks.iteritems():
        index_size = index_size
        group = index_size
        bm = []
        per_index_size = index_size/(int(last_index_read(rundir)) - 1)

        for read in runsetup:
            cycles = read['NumCycles']
            if read['IsIndexedRead'] == 'N':
                bm.append('Y' + cycles)
            else:
                # I_iN_y(,I_iN_y) or I(,I)
                if index_size > int(cycles):
                    i_remainder = int(cycles) - per_index_size
                    if i_remainder > 0:
                        bm.append('I' + str(per_index_size) + 'N' + str(i_remainder))
                    else:
                        bm.append('I' + cycles)
                # I_iN_y(,N) or I(,N)
                else:
                    if index_size > 0:
                        to_mask = "I" + str(index_size)
                        if index_size < int(cycles):
                           to_mask = to_mask + 'N' + str(int(cycles) - index_size)
                        bm.append(to_mask)
                        index_size = 0
                    else:
                        bm.append('N' + cycles)
        base_masks[group]['base_mask'] = bm
    return base_masks


def merge_flowcell_demux_summary(u1, u2, fc_id):
    """Merge two Flowcell_Demux_Summary.xml files.

    It assumes the structure:
    <Summary>
        <Lane index="X">
           .
           .
           .
        </Lane index="X">
    </Summary>

    Where X is the lane number [1-8].

    Also assumes that indexes of different length are run in different lanes.

    :param: u1: Unaligned directory where to find the fist file
    :patam: u2: Unaligned directory where to find the second file
    :param: fc_id: Flowcell id

    :return: merged: ElementTree resulting of merging both files.
    """
    #Read the XML to merge
    fc1_f = os.path.join(u1, 'Basecall_Stats_{fc_id}'.format(fc_id=fc_id),
            'Flowcell_demux_summary.xml')
    fc2_f = os.path.join(u2, 'Basecall_Stats_{fc_id}'.format(fc_id=fc_id),
            'Flowcell_demux_summary.xml')
    fc1 = ET.parse(fc1_f).getroot()
    fc2 = ET.parse(fc2_f).getroot()

    #Create a new one and merge there
    merged = ET.ElementTree(ET.Element('Summary'))
    merged_r = merged.getroot()
    lanes = merged_r.getchildren()
    for l1, l2 in izip_longest(fc1.getchildren(), fc2.getchildren()):
        lanes.append(l1) if l1 is not None else []
        lanes.append(l2) if l2 is not None else []

    #Sort the children by lane number and return the merged file
    lanes.sort(key= lambda x: x.attrib['index'])
    return merged


def merge_demultiplex_stats(u1, u2, fc_id):
    """Merge two Demultiplex_Stats.htm files.

    Will append to the Demultiplex_Stats.htm file in u1 the Barcode Lane
    Statistics and Sample Information found in Demultiplex_Stats.htm file in u2.

    The htm file should be structured in such a way that it has two tables (in
    this order): Barcode Lane Statistics and Sample Information. The tables have
    an attribute 'id' which value is ScrollableTableBodyDiv.

    :param: u1: Unaligned directory where to find the fist file
    :patam: u2: Unaligned directory where to find the second file
    :param: fc_id: Flowcell id

    :return: merged: BeautifulSoup object representing the merging of both files.
    """
    with open(os.path.join(u1, 'Basecall_Stats_{fc_id}'.format(fc_id=fc_id),
            'Demultiplex_Stats.htm')) as f:
        ds1 = BeautifulSoup(f.read())
    with open(os.path.join(u2, 'Basecall_Stats_{fc_id}'.format(fc_id=fc_id),
            'Demultiplex_Stats.htm')) as f:
        ds2 = BeautifulSoup(f.read())

    #Get the information from the HTML files
    barcode_lane_statistics_u1, sample_information_u1 = ds1.find_all('div',
        attrs={'id':'ScrollableTableBodyDiv'})
    barcode_lane_statistics_u2, sample_information_u2 = ds2.find_all('div',
        attrs={'id':'ScrollableTableBodyDiv'})

    #Append to the end (tr is the HTML tag under the <div> tag that delimites
    #the sample and barcode statistics information)
    for sample in barcode_lane_statistics_u1.find_all('tr'):
        last_sample = sample
    [last_sample.append(new_sample) for new_sample in \
        barcode_lane_statistics_u2.find_all('tr')]

    for sample in sample_information_u1.find_all('tr'):
        last_sample = sample
    [last_sample.append(new_sample) for new_sample in \
        sample_information_u2.find_all('tr')]

    return ds1


def merge_undemultiplexed_stats_metrics(u1, u2, fc_id):
    """Merge and sort two Undemultiplexed_stats.metrics files.
    """
    with open(os.path.join(u1, 'Basecall_Stats_{fc_id}'.format(fc_id=fc_id),
            'Undemultiplexed_stats.metrics'), 'a+') as us1:
        with open(os.path.join(u2, 'Basecall_Stats_{fc_id}'.format(fc_id=fc_id),
                'Undemultiplexed_stats.metrics')) as us2:
            header = us1.readline()
            lines = []
            for line in us1.readlines():
                lines.append(line.split())
            for line in us2.readlines()[1:]:
                lines.append(line.split())

            us1.seek(0)
            us1.truncate()
            us1.writelines(header)
            for line in lines:
                us1.writelines("\t".join(str(line_field) for line_field in line) + "\n")


def merge_demux_results(fc_dir):
    """Merge results of demultiplexing from different demultiplexing folders

    :param str fc_dir: Path to the flowcell directory.
    """
    for option in CONFIG['analysis']['bcl2fastq']['options']:
        if isinstance(option, dict) and option.get('output-dir'):
            _demux_folder = option.get('output-dir')
    unaligned_dirs = glob.glob(os.path.join(fc_dir, '{}_*'.format(_demux_folder)))
    #If it is a MiSeq run, the fc_id will be everything after the -
    if '-' in os.path.basename(fc_dir):
        fc_id = os.path.basename(fc_dir).split('_')[-1]
    #If it is a HiSeq run, we only want the flowcell id (without A/B)
    else:
        fc_id = os.path.basename(fc_dir).split('_')[-1][1:]
    basecall_dir = 'Basecall_Stats_{fc_id}'.format(fc_id=fc_id)
    merged_dir = os.path.join(fc_dir, _demux_folder)
    merged_basecall_dir = os.path.join(merged_dir, basecall_dir)
    #Create the final Unaligned folder and copy there all configuration files
    filesystem.create_folder(os.path.join(merged_dir, basecall_dir))
    shutil.copy(os.path.join(unaligned_dirs[0], basecall_dir,
                    'Flowcell_demux_summary.xml'), merged_basecall_dir)
    shutil.copy(os.path.join(unaligned_dirs[0], basecall_dir,
                    'Demultiplex_Stats.htm'), merged_basecall_dir)
    #The file Undemultiplexed_stats.metrics may not always be there.
    u_s_file = os.path.exists(os.path.join(unaligned_dirs[0], basecall_dir,
                            'Undemultiplexed_stats.metrics'))
    if u_s_file:
        shutil.copy(os.path.join(unaligned_dirs[0], basecall_dir,
                    'Undemultiplexed_stats.metrics'), merged_basecall_dir)
        #And it is possible that it is empty, in which case we have to add
        #the header
        u_s_file_final = os.path.join(merged_basecall_dir, 'Undemultiplexed_stats.metrics')
        with open(u_s_file_final, 'r') as f:
            content = f.readlines()
            header = ['lane', 'sequence', 'count', 'index_name']
            if content and content[0].split() != header:
                with open(u_s_file_final, 'w') as final:
                    final.writelines('\t'.join(header) + '\n')
    if len(unaligned_dirs) > 1:
        for u in unaligned_dirs[1:]:
            #Merge Flowcell_demux_summary.xml
            m_flowcell_demux = merge_flowcell_demux_summary(merged_dir, u, fc_id)
            m_flowcell_demux.write(os.path.join(merged_dir, basecall_dir,
                            'Flowcell_demux_summary.xml'))

            #Merge Demultiplex_Stats.htm
            m_demultiplex_stats = merge_demultiplex_stats(merged_dir, u, fc_id)
            with open(os.path.join(merged_dir, basecall_dir, 'Demultiplex_Stats.htm'), 'w+') as f:
                f.writelines(re.sub(r"Unaligned_[0-9]{1,2}bp", 'Unaligned',
                    m_demultiplex_stats.renderContents()))

            #Merge Undemultiplexed_stats.metrics
            if u_s_file:
                merge_undemultiplexed_stats_metrics(merged_dir, u, fc_id)
