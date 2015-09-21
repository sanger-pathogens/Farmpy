#!/usr/bin/env python3

import argparse
from farmpy import lsf

parser = argparse.ArgumentParser(
    description = 'Wrapper script for running jobs using LSF',
    usage = '%(prog)s <memory> <name> <command>',
    epilog = 'Note: to run a job array, use --start and --end. Every appearance of INDEX in the command to be run will be replaced with \\$LSB_JOBINDEX. e.g. try bsub.py --norun --start 1 --end 10 1 name foo.sh INDEX')

parser.add_argument('memory', type=float,  help='Memory in GB to reserve for the job')
parser.add_argument('name', help='Name of the job')
parser.add_argument('command', help='Command to be bsubbed', nargs=argparse.REMAINDER)

parser.add_argument('-e', '--err', help='Name of file that stderr gets written to [job_name.e]', metavar='filename', default=None)
parser.add_argument('-o', '--out', help='Name of file that stdout gets written to [job_name.o]', metavar='filename', default=None)
parser.add_argument('-c', '--checkpoint', action='store_true', help='Use checkpointing')
parser.add_argument('-d', '--checkpoint_dir', help='Specify directory in which to put the checkpoint files. Default is to use stdout_file.checkpoint', metavar='/path/to/directory')
parser.add_argument('-p', '--checkpoint_period', help='Time interval between checkpoints in minutes [%(default)s]', default=600, metavar='time_in_minutes')
parser.add_argument('--start', type=int, help='Starting index of job array', metavar='int', default=0)
parser.add_argument('--end', type=int, help='Ending index of job array', metavar='int', default=0)
parser.add_argument('--done', action='append', help='Only start the job running when the given job finishes successfully. All digits is interpreted as a job ID, otherwise a job name. This can be used more than once to make the job depend on two or more other jobs', metavar='Job ID/job name')
parser.add_argument('--ended', action='append', help='As for --done, except the job must only finish, whether successful or not', metavar='job ID/job name')
parser.add_argument('--memory_units', help='Set to MB or KB as appropriate (this is a hack to be used when it is not detected automatically)', metavar='MB or KB', default=None)
parser.add_argument('--tmp_space', type=float, help='Reserve this much /tmp space in GB [%(default)s]', default=0, metavar='float')
parser.add_argument('--threads', type=int, help='Number of threads to request [%(default)s]', metavar='int', default=1)
parser.add_argument('--tokens_name', help='Name of resource tokens', metavar='string')
parser.add_argument('--tokens_number', type=int, help='Value of resource tokens (only used if --tokens_name is used) [%(default)s]', metavar='INT', default=100)
parser.add_argument('-q', '--queue', help='Queue in which to run job [%(default)s]', default='normal', metavar='queue_name')
parser.add_argument('--norun', action='store_true', help='Don\'t run, just print the bsub command')

options = parser.parse_args()

command = ' '.join(options.command)

# if error and/or output file not given, call them name.{e,o}
if options.err is None:
    options.err = options.name + '.e'

if options.out is None:
    options.out = options.name + '.o'

# make bsub object and run it
b = lsf.Job(options.out,
            options.err,
            options.name,
            options.queue,
            options.memory,
            command,
            checkpoint=options.checkpoint,
            checkpoint_dir=options.checkpoint_dir,
            checkpoint_period=options.checkpoint_period,
            array_start=options.start,
            array_end=options.end,
            depend=options.done,
            threads=options.threads,
            tmp_space=options.tmp_space,
            ended=options.ended,
            tokens_name=options.tokens_name,
            tokens_number=options.tokens_number,
            memory_units=options.memory_units)

print(b)

if not options.norun:
    b.run()
    print(b.job_id, 'submitted')
