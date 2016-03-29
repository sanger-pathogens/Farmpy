import os
import re
from datetime import datetime, date, time, timedelta

class Error (Exception): pass

date_time_match_string = '(at|on)\s+[a-zA-Z]+\s+([a-zA-Z]+)\s+([0-9]+)\s+([0-9]{2}):([0-9]{2}):([0-9]{2})\s+([0-9]{4})$'

regexes = {
    'job_name': re.compile('^Job <(.*)> was submitted from host <(.*)> by user <(.*)> in cluster <.*>.$'),
    'exec_host': re.compile('^Job was executed on host\(s\) <(.*)>, in queue <.*>, as user <.*> in cluster <.*>.$'),
    'working_dir': re.compile('^<(.*)> was used as the working directory.$'),
    'exit_code': re.compile('(^Successfully completed\.$)|(?:^Exited with exit code ([0-9]+)\.$)'),
    'cpu_time': re.compile('^\s+CPU time\s+:\s+([0-9]+\.[0-9]+) sec.$'),
    'max_memory': re.compile('^\s+Max Memory\s+:\s+([0-9]+) MB$'),
    'requested_memory': re.compile('^\s+Total Requested Memory\s+:\s+([0-9]+\.[0-9]+) MB'),
    'max_processes': re.compile('^\s+Max Processes\s+:\s+([0-9]+)$'),
    'max_threads': re.compile('^\s+Max Threads\s+:\s+([0-9]+)$'),
    'start_time': re.compile('^Started ' + date_time_match_string),
    'end_time': re.compile('^Results reported ' + date_time_match_string)
}


def file_reader(fname):
    '''Iterates over a file of bsub output, yielding the stats of next job in the file until there are no more'''
    try:
        f = open(fname)
    except:
        raise Error('Error opening file "' + fname + '"')

    stats = Stats()

    while stats.get_next_from_file(f):
        yield stats

    f.close()


all_stats = [
    'exit_code',
    'cpu_time',
    'wall_clock_time',
    'max_memory',
    'requested_memory',
    'max_processes',
    'max_threads',
    'start_time',
    'end_time',
    'exec_host',
    'username',
    'working_dir',
    'job_name'
]


short_stats = [
    'exit_code',
    'cpu_time',
    'wall_clock_time',
    'max_memory',
    'requested_memory',
]


tsv_header = '\t'.join(all_stats)
tsv_header_short = '\t'.join(short_stats)


class Stats:
    '''A class for getting stats from an lsf output file. E.g. memory, CPU usage etc'''
    def __init__(self):
        for stat in all_stats:
            exec('self.' + stat + ' = None')


    def __eq__(self, other):
        return type(other) is type(self) and self.__dict__ == other.__dict__


    def to_tsv(self, job_name_limit=None, show_all=True, time_in_hours=False):
        l = []
        if time_in_hours:
            original_cpu = self.cpu_time
            original_wall_clock = self.wall_clock_time
            self.cpu_time = None if (self.cpu_time is None) else round(self.cpu_time / (60*60), 2)
            self.wall_clock_time = None if (self.wall_clock_time is None) else round(self.wall_clock_time / (60*60), 2)


        if show_all:
            for x in all_stats:
                l.append(eval('self.' + x))
            if job_name_limit is not None and len(self.job_name) > job_name_limit:
                l[-1] = '*' + self.job_name[-job_name_limit:]
        else:
            for x in short_stats:
                l.append(eval('self.' + x))


        for i in range(len(l)):
            if l[i] is None:
                l[i] = '*'
            else:
                l[i] = str(l[i])

        if time_in_hours:
            self.cpu_time = original_cpu
            self.wall_clock_time = original_wall_clock

        return '\t'.join(l)


    def _parse_job_name_line(self, line):
        hits = regexes['job_name'].search(line)
        try:
            self.job_name = hits.group(1)
            self.username = hits.group(3)
        except:
            pass


    def _parse_exec_host_line(self, line):
        hits = regexes['exec_host'].search(line)
        try:
            self.exec_host = hits.group(1)
        except:
            pass


    def _parse_working_dir_line(self, line):
        hits = regexes['working_dir'].search(line)
        try:
            self.working_dir = hits.group(1)
        except:
            pass


    def _parse_exit_code_line(self, line):
        hits = regexes['exit_code'].search(line)

        if hits is None:
            pass
        elif hits.group(1) is not None:
            self.exit_code = 0
        elif hits.group(2) is not None:
            self.exit_code = int(hits.group(2))
        else:
            pass


    def _parse_cpu_time_line(self, line):
        hits = regexes['cpu_time'].search(line)
        try:
            self.cpu_time = float(hits.group(1))
        except:
            pass


    def _parse_max_memory_line(self, line):
        hits = regexes['max_memory'].search(line)
        try:
            self.max_memory = float(hits.group(1)) / 1000
        except:
            pass


    def _parse_requested_memory_line(self, line):
        hits = regexes['requested_memory'].search(line)
        try:
            self.requested_memory = float(hits.group(1)) / 1000
        except:
            pass


    def _parse_max_processes_line(self, line):
        hits = regexes['max_processes'].search(line)
        try:
            self.max_processes = int(hits.group(1))
        except:
            pass


    def _parse_max_threads_line(self, line):
        hits = regexes['max_threads'].search(line)
        try:
            self.max_threads = int(hits.group(1))
        except:
            pass


    def _time_line_to_datetime(self, line):
        regex = re.compile(date_time_match_string)
        hits = regex.search(line)

        try:
            month = hits.group(2)
            day = int(hits.group(3))
            hrs = int(hits.group(4))
            mins = int(hits.group(5))
            secs = int(hits.group(6))
            year = int(hits.group(7))
        except:
            return None

        months = {
            'Jan': 1,
            'Feb': 2,
            'Mar': 3,
            'Apr': 4,
            'May': 5,
            'Jun': 6,
            'Jul': 7,
            'Aug': 8,
            'Sep': 9,
            'Oct': 10,
            'Nov': 11,
            'Dec': 12
        }

        month = months[month]
        job_date = date(year, month, day)
        job_time = time(hrs, mins, secs)
        return datetime.combine(job_date, job_time)


    def _parse_start_time_line(self, line):
        self.start_time = self._time_line_to_datetime(line)


    def _parse_end_time_line(self, line):
        self.end_time = self._time_line_to_datetime(line)
        if self.start_time is not None and self.end_time is not None:
            self.wall_clock_time = int ((self.end_time - self.start_time).total_seconds())


    def get_next_from_file(self, filehandle):
        '''Constructs stats from next job (if it exists) in the file'''
        # need to get past all the stdout at the start
        while 1:
            line = filehandle.readline()
            if not line:
                return False

            if line.startswith('Sender: LSF System <'):
                break

        # get bsub stats from the file, stop when we're at the end of the current job.
        end_re = re.compile('^Read file <.*> for stderr output of this job.$')
        while not end_re.match(line):
            line = filehandle.readline().rstrip()

            for key, val in regexes.items():
                if val.search(line) is not None:
                    eval('self._parse_' + key + '_line(line)')

        return True
