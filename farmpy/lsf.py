"""A module to help submitting jobs to a compute farm that uses Platform's LSF

Consists of one class - Job.
You can create jobs, run them and set jobs to depend on other jobs.

Run a job:
  job1 = Job('out', 'err', 'job name', 'normal', 1, 'run.sh')
  job1.run()

This creates a job called 'job name', which will run in the normal queue.
It asks for 1GB of memory, writes stdout/stderr to the files out/err.
It runs the script run.sh.

Dependencies:
You can use job1.add_dependency('another job name') to set job1 to run when the job with 'another job name' finishes. This means you can do this to set jobs to depend on other jobs:
  job2 = Job('out2', 'err2', 'job2', 'normal', 1, 'run2.sh')
  job2.add_dependency(job1.name)
  job2.run()
job2 will be put into the queue, but will only run when job1 finishes successfully.
Alternatively, running a job sets the variable job1.job_id, which means you can set a depndency is like this:
  job1.run() # this sets the variable job1.job_id.
  job2.add_dependency(job1.job_id)
This is safer than using a job name because the name might not be unique, but the ID is always unique.

You can also use Job(..., ended=job1.job_id) to make job2 run when job1 finishes, regardless of whether or not job1 finished successfully.

Job arrays:
You can run a job array using stert= and end= when constructing a Job. Example:
  job = ('out', 'err', 'name', 'normal', 1, 'run.sh INDEX', start=1, end=10)
This sets up a job array with 1 elements. stdout files will be called out.1, out.2, ....etc and similarly for stderr. Every appearance of 'INDEX' in the command is translated to be the job index (technically, the $LSB_JOBINDEX environment variable). So this would run submit 10 jobs to LSF:
  run.sh 1
  run.sh 2
  ...
  run.sh 10

Extra options:
When calling Job(...), these are options that can be given:
  start=x, end=y   - for running job arrays (se above)
  threads=N        - ask for N threads (default = 1)
  tmo_space=x      - ask for x GB of tmp space
  nax_array_size=N - limit number of jobs running at the same time in an array to N (default 100)
  memory_units=KB or MB - the units used in the -M option. It should be detected automatically, but you can override using this option (but might cause run() to fail)

"""

import socket
import subprocess
import getpass
import os


class Error (Exception): pass

class Job:
    def __init__(self, out, error, name, queue, mem, cmd,
                 array_start=0, array_end=0,
                 depend=None,
                 ended=None,
                 threads=1,
                 tmp_space=0,
                 memory_units=None,
                 no_resources=False,
                 checkpoint=False,
                 checkpoint_dir=None,
                 checkpoint_period=600,
                 tokens_name=None,
                 tokens_number=100,
                 max_array_size=100):
        '''Creates Job object. See main module help for a description and example usage'''

        self.stdout_file = out
        self.stderr_file = error
        self.name = name
        self.queue = queue
        self.memory = int(1000 * round(mem, 3))
        self.command = cmd
        self.array_start = array_start
        self.array_end = array_end
        self.run_when_done = []
        self.run_when_ended = []
        self.add_dependency(depend)
        self.add_dependency(ended, ended=True)
        self.threads = threads
        self.tmp_space = int(1000 * tmp_space)
        self.memory_units = memory_units
        self.max_array_size = max_array_size
        self.job_id = None
        self.no_resources=no_resources
        self.checkpoint = checkpoint
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_period = checkpoint_period
        self.tokens_name = tokens_name
        self.tokens_number = tokens_number


        # these are used for unittests to call test scripts instead of the
        # real commands you would run on a farm
        self._lsadmin_cmd = 'lsadmin showconf lim ' + socket.gethostname()
        self._run_test_cmd = None


    def run(self, verbose=False):
        '''Submits the job to the farm. Dies if not successful.'''
        if self._run_test_cmd is not None:
            cmd = self._run_test_cmd
        else:
            cmd = str(self)

        try:
            bsub_out = subprocess.check_output(cmd, shell=True).decode('utf-8')
        except:
            raise Error('Error in bsub call. I tried to run:\n' + str(self))

        self._set_job_id_from_bsub_output(bsub_out)


    def run_not_bsubbed(self):
        '''Runs the job directly on the node. Does not bsub it. Stdout and stderr will be output as if the command was run directly in a terminal'''
        def run_command(cmd):
            retcode = subprocess.call(cmd, shell=True)
            if retcode != 0:
                raise Error('Error running command:\n' + str(self))

        if self.array_start > 0:
            for i in range(self.array_start, self.array_end + 1):
                os.environ['LSB_JOBINDEX'] = str(i)
                run_command(self._make_command_string().replace('\$LSB_JOBINDEX', '$LSB_JOBINDEX'))
        else:
            run_command(self._make_command_string())


    def add_dependency(self, deps, ended=False):
        '''Makes the job depend on another job or jobs.

        deps -- can either be a job name, job id, or a list of job names and/or ids. It is assumed that anything that is all digits is a job id, otherwise it is a job name.

        Default is to make the job only run when the jobs it depends on finish successfully (i.e. return zero error code). If you want this job to run when the job it depends on finishes, regardless or error code, set ended=True.'''
        if deps is None:
            return

        if type(deps) is not list:
            deps = [deps]

        for d in deps:
            try:
                x = int(d)
                x = str(d)
            except ValueError:
                x = '"' + d + '"'

            if ended:
                self.run_when_ended.append(x)
            else:
                self.run_when_done.append(x)

    def _make_checkpoint_string(self):
        if self.checkpoint:
            if self.checkpoint_dir is None:
                self.checkpoint_dir = self.stdout_file + '.checkpoint'

            return '-k "' + os.path.abspath(self.checkpoint_dir) + ' method=blcr ' + str(self.checkpoint_period) + '"'
        else:
            return ''

    def _make_prexec_test_string(self):
        # a reasonable sanity check is that the home directory of the user
        # running the job is mounted on the node that will run the job
        return "-E 'test -e " + os.path.expanduser('~') + "'"


    def _make_queue_string(self):
        return '' if self.queue is None else '-q ' + self.queue


    def _set_memory_units(self):
        if self.memory_units is not None:
            return self.memory_units

        try:
            output = subprocess.check_output(self._lsadmin_cmd, shell=True).decode('utf-8').rstrip().split('\n')
        except:
            raise Error("Error getting lsf memory units using: lsadmin showconf lim " + socket.gethostname())

        # get the line with LSF_UNIT_FOR_LIMITS in it, if it exists
        for line in output:
            data = line.strip().split()
            if data[0] == 'LSF_UNIT_FOR_LIMITS':
                self.memory_units = data[2]
                break
        else:
            self.memory_units = 'KB'

        if self.memory_units not in ['KB', 'MB']:
            raise Error('Error getting lsf memory units. Expected KB or MB')


    def _make_resources_string(self):
        if self.no_resources:
            return ''

        self._set_memory_units()
        s = ''

        if self.threads > 1:
            s += '-n ' + str(self.threads) + ' -R "span[hosts=1]' + ' '
        else:
            s = '-R "'

        s += 'select[mem>' + str(self.memory)

        if self.tmp_space:
            s += ' && tmp>' + str(self.tmp_space)

        s += '] rusage[mem=' + str(self.memory)

        if self.tmp_space:
            s += ',tmp=' + str(self.tmp_space)

        if self.tokens_name:
            s += ',' + self.tokens_name + '=' + str(self.tokens_number)

        s += ']" -M' + str(self.memory)

        if self.memory_units == 'KB':
            s += '000'

        return s


    def _make_output_files_string(self):
        if self.array_start > 0:
            return '-o ' + self.stdout_file + '.%I -e ' + self.stderr_file + '.%I'
        else:
            return '-o ' + self.stdout_file + ' -e ' + self.stderr_file


    def _make_job_name_string(self):
        if self.array_start > 0:
            return '-J "' + self.name + '[' + str(self.array_start) + '-' + str(self.array_end) \
                   + ']%' + str(self.max_array_size) + '"'
        else:
            return '-J ' + self.name


    def _make_dependencies_string(self):
        if len(self.run_when_done) + len(self.run_when_ended) > 0:
            return "-w '" \
                   + ' && '.join(['done(' + x + ')' for x in self.run_when_done] + ['ended(' + x + ')' for x in self.run_when_ended]) \
                   + "'"
        else:
            return ''


    def _make_command_string(self):
        command = ''

        if self.checkpoint:
            command = 'cr_run '

        if self.array_start > 0:
            return command + self.command.replace('INDEX', '\$LSB_JOBINDEX')
        else:
            return command + self.command


    def __str__(self):
        return ' '.join([x for x in [
                            'bsub',
                            self._make_checkpoint_string(),
                            self._make_queue_string(),
                            self._make_prexec_test_string(),
                            self._make_resources_string(),
                            self._make_output_files_string(),
                            self._make_job_name_string(),
                            self._make_dependencies_string(),
                            self._make_command_string()
                        ] if x != ''])


    def _set_job_id_from_bsub_output(self, bsub_output):
        # for ok bsub call, output should look like this:
        # Job <42> is submitted to queue <normal>.
        if bsub_output is None:
            raise Error('Error - got no output from bsub call')

        first_line = bsub_output.split('\n')[0]

        if 'is submitted to' not in first_line:
            raise Error("Error getting job ID from bsub output. I got this:\n" + bsub_output)

        self.job_id = first_line.split()[1][1:-1]

        # check that the job ID looks like an int
        try:
            int(self.job_id)
        except ValueError:
            raise Error('Error getting job ID from this:' + first_line)

