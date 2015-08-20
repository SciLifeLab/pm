""" Different file parsers for TACA
"""
import csv
import os
import re
import xml.etree.ElementTree as ET

from collections import OrderedDict, defaultdict

#######################################
# Raw data status/stats files parsers #
#######################################

def get_read_configuration(run_path, sort=False):
    """Parse the RunInfo.xml to read configuration and return a list of dicts

    :param str run_path: Path to the run directory
    :param boolean sort: Sort the reads dicts by read number
    :returns: List of dicts corresponding to sections in RunInfo.xml
    :rtype: list
    :raises RunTimeError: If no RunInfo.xml file is found
    """
    reads = []
    run_info_file = os.path.join(run_path, "RunInfo.xml")
    try:
        tree = ET.ElementTree()
        tree.parse(run_info_file)
        read_elem = tree.find("Run/Reads")
        for read in read_elem:
            reads.append(dict(zip(read.keys(), [read.get(k) for k in read.keys()])))
        if not sort:
            return reads
        else:
            return sorted(reads, key=lambda r: int(r.get("Number", 0)))
    except IOError:
        raise RuntimeError('No RunInfo.xml file found in {}. Please check.'.format(run_path))


def last_index_read(run):
    """Parse the number of the highest index read from the RunInfo.xml file

    :param str run: Run directory
    :returns: Highest index read
    :rtype: int
    """
    read_numbers = [int(read.get("Number", 0)) for read in get_read_configuration(run) if read.get("IsIndexedRead", "") == "Y"]
    return 0 if len(read_numbers) == 0 else max(read_numbers)


def get_flowcell_id(rundir):
    """Parese the RunInfo.xml and return the Flowcell ID

    :param str rundir: Path to the run
    :returns str: ID of the flowcell
    """
    assert os.path.exists(os.path.join(rundir, 'RunInfo.xml')), ("No RunInfo.xml found "
                          "for run {}".format(os.path.basename(rundir)))

    run_info_file = os.path.join(rundir, "RunInfo.xml")
    flowcell_id = ''
    tree = ET.ElementTree()
    tree.parse(run_info_file)

    return tree.find("Run/Flowcell").text


class MiSeqSampleSheet(object):
    def __init__(self, ss_file):
        assert os.path.exists(ss_file), \
            "Samplesheet %s does not exist" % ss_file

        setattr(self, "samplesheet", ss_file)
        self.data_header = ["Sample_ID",
                            "Sample_Name",
                            "Sample_Plate",
                            "Sample_Well",
                            "Sample_Project",
                            "index",
                            "I7_Index_ID",
                            "index2",
                            "I5_Index_ID",
                            "Description",
                            "Manifest",
                            "GenomeFolder"]
        self._parse_sample_sheet()

    def _parse_sample_sheet(self):

        # Parse the samplesheet file into a data structure
        data = defaultdict(dict)
        with open(self.samplesheet,"rU") as fh:
            current = None
            for line in fh:
                line = line.strip()
                if line.startswith("["):
                    current = line.strip("[], ")
                else:
                    if current is None:
                        current = "NoSection"
                    s = line.split(",",1)
                    if len(s) > 1:
                        data[current][s[0]] = s[1]
                    else:
                        data[current][line] = ''

        # Assign the parsed attributes to class attributes
        for option, value in data.get("Header",{}).items():
            setattr(self, option.replace(" ", ""), value)

        for option, value in data.get("Settings",{}).items():
            setattr(self, option, value)
        if "Data" not in data:
            data["Data"] = {}
            data["Data"][self.data_header[0]] = ",".join(self.data_header[1:])
            for option, value in data.get("NoSection",{}).items():
                data["Data"][option] = value

        # Parse sample data
        first_data_col = "Sample_ID"
        if "Data" in data and first_data_col in data["Data"]:
            self.data_header = [s.lower() for s in data["Data"][first_data_col].split(",")]
            samples = {}
            for sample_id, sample_data in data["Data"].items():
                if sample_id == first_data_col:
                    continue

                samples[sample_id] = dict(zip(self.data_header,sample_data.split(",")))
                samples[sample_id][first_data_col.lower()] = sample_id

            setattr(self, "samples", samples)

    def sample_names(self):
        """Return the name of the samples in the same order as they are listed in
        the samplesheet.
        """
        samples = getattr(self,"samples",{})

        if getattr(self, "_sample_names", None) is None:
            sample_names = []
            with open(self.samplesheet,"rU") as fh:
                for line in fh:
                    if line.startswith("[Data]"):
                        for line in fh:
                            data = line.split(",")
                            if len(data) == 0 or data[0].startswith("["):
                                break

                            if data[0] in samples:
                                sample_names.append(data[0])

            self._sample_names = sample_names

        return self._sample_names


    def sample_field(self, sample_id, sample_field=None):
        samples = getattr(self,"samples",{})
        assert sample_id in samples, \
            "The sample '%s' was not found in samplesheet %s" % (sample_id,self.samplesheet)
        if sample_field is None:
            return samples[sample_id]

        assert sample_field in samples[sample_id], \
            "The sample field '%s' was not found in samplesheet %s" % (sample_field,self.samplesheet)

        return samples[sample_id][sample_field]


    def to_hiseq(self, fc_id, write=True):
        """Convert Miseq SampleSheet to HiSeq formatted Samplesheet.
        """
        header = ["FCID",
                  "Lane",
                  "SampleID",
                  "SampleRef",
                  "Index",
                  "Description",
                  "Control",
                  "Recipe",
                  "Operator",
                  "SampleProject"]
        Lane = "1"
        SampleRef = "NA"
        Description = "NA"
        Control = "N"
        Recipe = "NA"
        Operator = "NA"

        rows = []
        for sampleID, info in self.samples.iteritems():
            row = OrderedDict()
            row["FCID"] = fc_id
            row["Lane"] = Lane
            row["SampleID"] = sampleID
            row["SampleRef"] = self._extract_reference_from_path(info.get('genomefolder',''))
            row["Index"] = info.get('index','')
            if 'index2' in info and len(info['index2']) > 0:
                row["Index"] = "{}-{}".format(row["Index"],info["index2"])
            row["Description"] = info.get('description','')
            row["Control"] = Control
            row["Recipe"] = Recipe
            row["Operator"] = Operator
            row["SampleProject"] = info.get('sample_project','Unknown')

            rows.append(row)

        if write:
            with open('{}.csv'.format(fc_id), "w") as outh:
                csvw = csv.writer(outh)
                if len(rows) > 0:
                    csvw.writerow(rows[0].keys())
                else:
                    csvw.writerow(self.header)
                csvw.writerows([row.values() for row in rows])

        return rows

    def _extract_reference_from_path(self, path):
        """Attempts to extract a name of a reference assembly from a path
        """

        head = path
        regexp = r'[a-zA-Z]+[0-9\.]+$'
        while head is not None and len(head) > 0:
            head, tail = os.path.split(head.replace('\\','/'))
            if re.match(regexp, tail) is not None:
                return tail

        return path
