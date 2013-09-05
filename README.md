Farmpy
======


This is a Python3 package to handle job submission to a compute farm. Currently supports Platform's LSF.  It has a script to make submitting jobs easier and also an API to run jobs from within scripts.


Installation
------------

We assume that you are using Platform's LSF, i.e. it is installed already with bsub and lsadmin in your path.

Run the tests:

    python3 setup.py test

Install:

    python3 setup.py install


Synposis - command line
-----------------------

To submit a job called "name" that asks for 1GB of memory, runs the script foo.sh and writes stdout and stderr to name.o and name.e:

    bsub.py 1 name foo.sh

To submit a job that will only run when job with ID 42 has finished, use:

    bsub.py --depend 42 1 name foo.sh

If you want 10GB of /tmp space on the node:

    bsub.py --tmp_space 10 1 name foo.sh

There are many more options. Use -h or --help to see the full list of options

    bsub.py --help

Synposis - running within a script
----------------------------------

Amongst other things, you run a job, set dependencies, change queues and resources and run arrays.  Use

    help(lsf)

to find out more.

### Make a job and run it ###

    job1 = lsf.Job('out', 'err', 'name', 'normal', 1, 'foo.sh')
    job1.run()

This created a job called 'name', which will run in the normal queue.  It asked for 1GB of memory and will write stdout/stderr to the files out/err.  It runs the script run.sh.

### Dependencies ###

When the job was run, the variable job1.job_id was set, so that you can use dependencies. For example:

    job2 = lsf.Job('out2', 'err2', 'job2', 'normal', 1, 'run2.sh')
    job2.add_dependency(job1.job_id)
    job2.run()

Alternatively, dependcies can be set using job names, but these might not be unique so IDs are safer. If you want to use a name instead:

    job2.add_dependency(job1.name)

### Job arrays ###

You can run a job array using start= and end= when constructing a Job. Example:

  job = ('out', 'err', 'name', 'normal', 1, 'run.sh INDEX', start=1, end=10)

This sets up a job array with 10 elements. stdout files will be called out.1, out.2, ....etc and similarly for stderr. Every appearance of 'INDEX' in the command is translated to be the job index (technically, the $LSB_JOBINDEX environment variable). So this would submit 10 jobs to LSF:
    run.sh 1
    run.sh 2
    ...
    run.sh 10

