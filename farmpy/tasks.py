from farmpy import lsf_stats
import sys

class Error (Exception): pass


def lsf_out_to_tsv(infiles, outfile, show_all=False, compress_job_name=10, compress_filename=None, time_in_hours=False):
    '''Given a list of files out bsub output, makes a tsv file of their stats'''
    if outfile == '-':
        fout = sys.stdout
    else:
        try:
            fout = open(outfile, 'w')
        except:
            raise Error ('Error opening file "' + outfile + '"')

    if show_all:
        print('#number_in_file', lsf_stats.tsv_header, 'filename', sep='\t', file=fout)
    else:
        print('#number_in_file', lsf_stats.tsv_header_short, 'filename', sep='\t', file=fout)

    for infile in infiles:
        attempt_number = 1
        reader = lsf_stats.file_reader(infile)
        
        for stats in reader:
            if compress_filename is None:
                filename = infile
            elif len(infile) > compress_filename:
                filename = '*' + infile[-compress_filename:]
            else:
                filename = infile
            print(attempt_number, stats.to_tsv(job_name_limit=compress_job_name, show_all=show_all, time_in_hours=time_in_hours), filename, sep='\t', file=fout)
            attempt_number += 1

    if outfile != '-':
        fout.close()
