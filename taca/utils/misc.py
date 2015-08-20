""" Miscellaneous or general-use methods
"""
import hashlib
import os
import smtplib
import subprocess
import sys
import glob

from datetime import datetime
from email.mime.text import MIMEText


def send_mail(subject, content, receiver):
    """ Sends an email

    :param str subject: Subject for the email
    :param str content: Content of the email
    :param str receiver: Address to send the email
    """
    msg = MIMEText(content)
    msg['Subject'] = "TACA - {}".format(subject)
    msg['From'] = 'TACA'
    msg['to'] = receiver

    s = smtplib.SMTP('localhost')
    s.sendmail('TACA', [receiver], msg.as_string())
    s.quit()


def call_external_command(cl, with_log_files=False, prefix=None, log_dir=""):
    """ Executes an external command

    :param string cl: Command line to be executed (command + options and parameters)
    :param bool with_log_files: Create log files for stdout and stderr
    :param string prefix: the prefics to add to log file
    :param string log_dir: where to write the log file (to avoid problems with rights)
    """
    if type(cl) == str:
        cl = cl.split(' ')
    logFile = os.path.basename(cl[0])
    stdout = sys.stdout
    stderr = sys.stderr
    if with_log_files:
        if prefix:
            logFile = '{}_{}'.format(prefix, logFile)
        if os.path.exists(log_dir):
            logFile = os.path.join(log_dir,  logFile) # otherwise create it in the cwd
        stdout = open(logFile + '.out', 'wa')
        stderr = open(logFile + '.err', 'wa')
        started = "Started command {} on {}".format(' '.join(cl), datetime.now())
        stdout.write(started + '\n')
        stdout.write(''.join(['=']*len(cl)) + '\n')

    try:
        subprocess.check_call(cl, stdout=stdout, stderr=stderr)
    except subprocess.CalledProcessError, e:
        e.message = "The command {} failed.".format(' '.join(cl))
        raise e
    finally:
        if with_log_files:
            stdout.close()
            stderr.close()


def call_external_command_detached(cl, with_log_files=False, prefix=None):
    """ Executes an external command

        :param string cl: Command line to be executed (command + options and parameters)
        :param bool with_log_files: Create log files for stdout and stderr
        """
    if type(cl) == str:
        cl = cl.split(' ')
    command = os.path.basename(cl[0])
    stdout = sys.stdout
    stderr = sys.stderr

    if with_log_files:
        if prefix:
            command = '{}_{}'.format(prefix, command)
        stdout = open(command + '.out', 'wa')
        stderr = open(command + '.err', 'wa')
        started = "Started command {} on {}".format(' '.join(cl), datetime.now())
        stdout.write(started + '\n')
        stdout.write(''.join(['=']*len(cl)) + '\n')

    try:
        p_handle = subprocess.Popen(cl, stdout=stdout, stderr=stderr)
    except subprocess.CalledProcessError, e:
        e.message = "The command {} failed.".format(' '.join(cl))
        raise e
    finally:
        if with_log_files:
            stdout.close()
            stderr.close()
    return p_handle

def days_old(date, date_format="%y%m%d"):
    """ Return the number days between today and given date

        :param string date: date to ckeck with
        :param date_format: the format of given 'date' string
    """
    try:
        time_dif = datetime.today() - datetime.strptime(date,date_format)
    except ValueError:
        return None
    return time_dif.days

def hashfile(afile, hasher='sha1', blocksize=65536):
    """ Calculate the hash digest of a file with the specified algorithm and 
        return it.
        
        This solution was adapted from http://stackoverflow.com/a/3431835
    
        :param string afile: the file to calculate the digest for
        :param string hasher: the hashing algorithm to be used, default is sha1
        :param int blocksize: the blocksize to use, default is 65536 bytes
        :returns: the hexadecimal hash digest or None if input was not a file
    """ 
    if not os.path.isfile(afile):
        return None
    hashobj = hashlib.new(hasher)
    with open(afile,'rb') as fh:
        buf = fh.read(blocksize)
        while len(buf) > 0:
            hashobj.update(buf)
            buf = fh.read(blocksize)
    return hashobj.hexdigest()
    
def query_yes_no(question, default="yes", force=False):
    """Ask a yes/no question via raw_input() and return their answer.
    "question" is a string that is presented to the user. "default"
    is the presumed answer if the user just hits <Enter>. It must be
    "yes" (the default), "no" or None (meaning an answer is required
    of the user). The force option simply sets the answer to default.
    The "answer" return value is one of "yes" or "no".

    :param question: the displayed question
    :param default: the default answer
    :param force: set answer to default
    :returns: yes or no
    """
    valid = {"yes":True,   "y":True,  "ye":True,
             "no":False,     "n":False}
    if default == None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        if not force:
            choice = raw_input().lower()
        else:
            choice = "yes"
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "\
                                 "(or 'y' or 'n').\n")





def return_unique(seq):
    seen = set()
    seen_add = seen.add
    return [ x for x in seq if not (x in seen or seen_add(x))]


def link_undet_to_sample(run, dmux_folder, lane, path_per_lane):
    """symlinks the undetermined file to the right sample folder with a RELATIVE path so it's carried over by rsync
    
    :param run: path of the flowcell
    :type run: str
    :param lane: lane identifier
    :type lane: int
    :param path_per_lane: {lane:path/to/the/sample}
    :type path_per_lane: dict"""
    for fastqfile in glob.glob(os.path.join(run, dmux_folder, '*Undetermined*_L0?{}_*'.format(lane))):
        if not os.path.exists(os.path.join(path_per_lane[lane], os.path.basename(fastqfile))):
            fqbname=os.path.basename(fastqfile)
            os.symlink(os.path.join('..','..',fqbname), os.path.join(path_per_lane[lane], os.path.basename(fastqfile)))


