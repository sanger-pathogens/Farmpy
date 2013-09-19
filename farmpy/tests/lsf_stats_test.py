#!/usr/bin/env python3

import sys
import unittest
from datetime import datetime, date, time, timedelta
from farmpy import lsf_stats
import os

lsf_stats_dir = os.path.dirname(os.path.abspath(lsf_stats.__file__))
data_dir = os.path.join(lsf_stats_dir, 'tests', 'data')

class TestLsfStats(unittest.TestCase):
    def test_to_tsv(self):
        '''Test cnvert to tsv'''
        reader = lsf_stats.file_reader(os.path.join(data_dir, 'lsf_unittest_outfile'))
        stats = next(reader)
        expected = [
            stats.exit_code,
            stats.cpu_time,
            stats.wall_clock_time,
            stats.max_memory,
            stats.requested_memory,
            stats.max_processes,
            stats.max_threads,
            stats.start_time,
            stats.end_time,
            stats.exec_host,
            stats.username,
            stats.working_dir,
            stats.job_name,
        ]

        self.assertEqual('\t'.join([str(x) for x in expected]), stats.to_tsv())

        expected[-1] = '*' + expected[-1][-10:]
        self.assertEqual('\t'.join([str(x) for x in expected]), stats.to_tsv(job_name_limit=10))

        stats = lsf_stats.Stats()
        self.assertEqual('\t'.join(['*']*13), stats.to_tsv())


        reader = lsf_stats.file_reader(os.path.join(data_dir, 'lsf_unittest_outfile'))
        stats = next(reader)

        expected = [
            stats.exit_code,
            stats.cpu_time,
            stats.wall_clock_time,
            stats.max_memory,
            stats.requested_memory,
        ]
        
        self.assertEqual('\t'.join([str(x) for x in expected]), stats.to_tsv(show_all=False))

    def test_tsv_header(self):
        '''Test tsv header string'''
        expected = '\t'.join([
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
        ])

        self.assertEqual(expected, lsf_stats.tsv_header)
        
            
class TestLineParsing(unittest.TestCase):
    def test_parse_job_name_line(self):
        '''Test name of job and username correctly extracted from bsub output'''
        stats = lsf_stats.Stats()
        line = 'Job <name_of_job> was submitted from host <farm3-head3> by user <username> in cluster <farm3>.'
        stats._parse_job_name_line(line)
        self.assertEqual('name_of_job', stats.job_name)
        self.assertEqual('username', stats.username)
 
        stats = lsf_stats.Stats()
        stats._parse_job_name_line('x')
        self.assertEqual(None, stats.job_name)
        self.assertEqual(None, stats.username)


    def test_parse_exec_host_line(self):
        stats = lsf_stats.Stats()
        line = 'Job was executed on host(s) <exec_host>, in queue <normal>, as user <username> in cluster <farm3>.'
        stats._parse_exec_host_line(line)
        self.assertEqual('exec_host', stats.exec_host)
 
        stats = lsf_stats.Stats()
        stats._parse_exec_host_line('x')
        self.assertEqual(None, stats.exec_host)


    def test_parse_working_dir_line(self):
        stats = lsf_stats.Stats()
        line = '</the/working/dir> was used as the working directory.'
        stats._parse_working_dir_line(line)
        self.assertEqual('/the/working/dir', stats.working_dir)
 
        stats = lsf_stats.Stats()
        stats._parse_working_dir_line('x')
        self.assertEqual(None, stats.working_dir)


    def test_parse_exit_code_line(self):
        stats = lsf_stats.Stats()
        lines = [
            'Successfully completed.',
            'Exited with exit code 42.',
            'x'
        ]
        
        exit_codes = [0, 42, None]

        for i in range(len(lines)):
            stats = lsf_stats.Stats()
            stats._parse_exit_code_line(lines[i])
            self.assertEqual(exit_codes[i], stats.exit_code)

        stats = lsf_stats.Stats()
        stats._parse_exit_code_line('x')
        self.assertEqual(None, stats.exit_code)

    def test_parse_cpu_time_line(self):
        stats = lsf_stats.Stats()
        line = '    CPU time :               10464.48 sec.'
        stats._parse_cpu_time_line(line)
        self.assertEqual(10464.48, stats.cpu_time)
        
        stats = lsf_stats.Stats()
        stats._parse_cpu_time_line('x')
        self.assertEqual(None, stats.cpu_time)
        

    def test_parse_max_memory_line(self):
        stats = lsf_stats.Stats()
        line = '    Max Memory :             1174 MB'
        stats._parse_max_memory_line(line)
        self.assertEqual(1.174, stats.max_memory)
        
        stats = lsf_stats.Stats()
        stats._parse_max_memory_line('x')
        self.assertEqual(None, stats.max_memory)
        
    
    def test_parse_requested_memory_line(self):
        stats = lsf_stats.Stats()
        line = '    Total Requested Memory : 2000.00 MB'
        stats._parse_requested_memory_line(line)
        self.assertEqual(2, stats.requested_memory)
        
        stats = lsf_stats.Stats()
        stats._parse_requested_memory_line('x')
        self.assertEqual(None, stats.requested_memory)


    def test_parse_max_processes_line(self):
        stats = lsf_stats.Stats()
        line = '    Max Processes :          6'
        stats._parse_max_processes_line(line)
        self.assertEqual(6, stats.max_processes)

        stats = lsf_stats.Stats()
        stats._parse_max_processes_line('x')
        self.assertEqual(None, stats.max_processes)


    def test_parse_max_threads_line(self):
        stats = lsf_stats.Stats()
        line = '    Max Threads :            7'
        stats._parse_max_threads_line(line)
        self.assertEqual(7, stats.max_threads)

        stats = lsf_stats.Stats()
        stats._parse_max_threads_line('x')
        self.assertEqual(None, stats.max_threads)


    def test_time_line_to_datetime(self):
        stats = lsf_stats.Stats()
        line = 'foo bar at Sun Sep 16 12:13:29 2013'
        expected_date = date(2013, 9, 16)
        expected_time = time(12, 13, 29)
        expected_datetime = datetime.combine(expected_date, expected_time)
        self.assertEqual(expected_datetime, stats._time_line_to_datetime(line))

        stats = lsf_stats.Stats()
        self.assertEqual(None, stats._time_line_to_datetime('x'))


    def test_parse_start_time_line(self):
        stats = lsf_stats.Stats()
        line = 'Started at Sun Sep 16 12:13:29 2013'
        expected_date = date(2013, 9, 16)
        expected_time = time(12, 13, 29)
        expected_datetime = datetime.combine(expected_date, expected_time)
        stats._parse_start_time_line(line)
        self.assertEqual(expected_datetime, stats.start_time)
        

    def test_parse_end_time_line(self):
        stats = lsf_stats.Stats()
        line = 'Results reported at Mon Sep 16 13:11:06 2013'
        expected_date = date(2013, 9, 16)
        expected_time = time(13, 11, 6)
        expected_datetime = datetime.combine(expected_date, expected_time)
        stats._parse_end_time_line(line)
        self.assertEqual(expected_datetime, stats.end_time)


class TestFileReader(unittest.TestCase):
    def test_file_reader(self):
        expected_stats = [lsf_stats.Stats(), lsf_stats.Stats()]
        expected_stats[0].job_name = 'name_of_job'
        expected_stats[0].exec_host = 'exec_host'
        expected_stats[0].username = 'username'
        expected_stats[0].working_dir = '/the/working/dir'
        expected_stats[0].start_time = datetime.combine(date(2013, 9, 16), time(12, 13, 29))
        expected_stats[0].end_time = datetime.combine(date(2013, 9, 16), time(13, 11, 6))
        expected_stats[0].wall_clock_time = 0.96
        expected_stats[0].cpu_time = 10864.48
        expected_stats[0].max_memory = 1.184
        expected_stats[0].requested_memory = 2
        expected_stats[0].max_processes = 6
        expected_stats[0].max_threads = 7
        expected_stats[0].exit_code = 0

        expected_stats[1].job_name = 'name_of_job'
        expected_stats[1].exec_host = 'exec_host'
        expected_stats[1].username = 'username'
        expected_stats[1].working_dir = '/the/working/dir'
        expected_stats[1].start_time = datetime.combine(date(2013, 9, 16), time(13, 13, 29))
        expected_stats[1].end_time = datetime.combine(date(2013, 9, 16), time(15, 11, 6))
        expected_stats[1].wall_clock_time = 1.96
        expected_stats[1].cpu_time = 10464.48
        expected_stats[1].max_memory = 1.174
        expected_stats[1].requested_memory = 2
        expected_stats[1].max_processes = 6
        expected_stats[1].max_threads = 7
        expected_stats[1].exit_code = 42

        reader = lsf_stats.file_reader(os.path.join(data_dir, 'lsf_unittest_outfile'))
        i = 0
        
        for stats in reader:
            self.assertEqual(expected_stats[i], stats)
            i += 1

        with self.assertRaises(lsf_stats.Error):
            reader = lsf_stats.file_reader('notafilesothrowanerror')
            next(reader)
         


if __name__ == '__main__':
    unittest.main()
