#!/usr/bin/env python3

import sys
import unittest
import filecmp
from farmpy import tasks, lsf_stats
import os

tasks_dir = os.path.dirname(os.path.abspath(tasks.__file__))
data_dir = os.path.join(tasks_dir, 'tests', 'data')


class TestToTsv(unittest.TestCase):
    def test_lsf_out_to_tsv(self):
        '''Test conversion to tsv with defaults'''
        infiles = [
            os.path.join(data_dir, 'lsf_unittest_outfile'),
            os.path.join(data_dir, 'lsf_unittest_outfile2')
        ]

        outfile = 'tmp.test_left_out_to_tsv'
        expected = 'tmp.test_left_out_to_tsv.expected'
        f = open(expected, 'w')
        print('#number_in_file', lsf_stats.tsv_header, 'filename', sep='\t', file=f)
        print('\t'.join(['1', '0', '10864.48', '3457', '1.184', '2.0', '6', '7', '2013-09-16 12:13:29', '2013-09-16 13:11:06', 'exec_host', 'username', '/the/working/dir', 'name_of_job', infiles[0]]), file=f)
        print('\t'.join(['2', '42', '10464.48', '7057', '1.174', '2.0', '6', '7', '2013-09-16 13:13:29', '2013-09-16 15:11:06', 'exec_host', 'username', '/the/working/dir', 'name_of_job', infiles[0]]), file=f)
        print('\t'.join(['1', '0', '10864.48', '3457', '1.184', '2.0', '6', '7', '2013-09-16 12:13:29', '2013-09-16 13:11:06', 'exec_host', 'username', '/the/working/dir', 'job', infiles[1]]), file=f)
        f.close()
        tasks.lsf_out_to_tsv(infiles, outfile, compress_job_name=None, show_all=True)
        self.assertTrue(filecmp.cmp(expected, outfile, shallow=False))
        os.unlink(outfile)
        os.unlink(expected)

    def test_lsf_out_to_tsv_compressed(self):
        '''Test conversion to tsv with compresed names'''
        infiles = [
            os.path.join(data_dir, 'lsf_unittest_outfile'),
            os.path.join(data_dir, 'lsf_unittest_outfile2')
        ]

        outfile = 'tmp.test_left_out_to_tsv'
        expected = 'tmp.test_left_out_to_tsv.expected'
        f = open(expected, 'w')
        print('#number_in_file', lsf_stats.tsv_header, 'filename', sep='\t', file=f)
        print('\t'.join(['1', '0', '10864.48', '3457', '1.184', '2.0', '6', '7', '2013-09-16 12:13:29', '2013-09-16 13:11:06', 'exec_host', 'username', '/the/working/dir', '*job', '*' + infiles[0][-3:]]), file=f)
        print('\t'.join(['2', '42', '10464.48', '7057', '1.174', '2.0', '6', '7', '2013-09-16 13:13:29', '2013-09-16 15:11:06', 'exec_host', 'username', '/the/working/dir', '*job', '*' + infiles[0][-3:]]), file=f)
        print('\t'.join(['1', '0', '10864.48', '3457', '1.184', '2.0', '6', '7', '2013-09-16 12:13:29', '2013-09-16 13:11:06', 'exec_host', 'username', '/the/working/dir', 'job', '*' + infiles[1][-3:]]), file=f)
        f.close()
        tasks.lsf_out_to_tsv(infiles, outfile, compress_job_name=3, compress_filename=3, show_all=True)
        self.assertTrue(filecmp.cmp(expected, outfile, shallow=False))
        os.unlink(outfile)
        os.unlink(expected)

    def test_lsf_out_to_tsv_short(self):
        '''Test conversion to tsv with short version and time in hours'''
        infiles = [
            os.path.join(data_dir, 'lsf_unittest_outfile'),
            os.path.join(data_dir, 'lsf_unittest_outfile2')
        ]

        outfile = 'tmp.test_left_out_to_tsv'
        expected = 'tmp.test_left_out_to_tsv.expected'
        f = open(expected, 'w')
        print('#number_in_file', lsf_stats.tsv_header_short, 'filename', sep='\t', file=f)
        print('\t'.join(['1', '0', '3.02', '0.96', '1.184', '2.0', '*' + infiles[0][-3:]]), file=f)
        print('\t'.join(['2', '42', '2.91', '1.96', '1.174', '2.0', '*' + infiles[0][-3:]]), file=f)
        print('\t'.join(['1', '0', '3.02', '0.96', '1.184', '2.0', '*' + infiles[1][-3:]]), file=f)
        f.close()
        tasks.lsf_out_to_tsv(infiles, outfile, compress_job_name=3, compress_filename=3, time_in_hours=True)
        self.assertTrue(filecmp.cmp(expected, outfile, shallow=False))
        os.unlink(outfile)
        os.unlink(expected)

if __name__ == '__main__':
    unittest.main()
