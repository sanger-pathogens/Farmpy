#!/usr/bin/env python3

import sys
import unittest
import filecmp
from farmpy import lsf
import os

lsf_dir = os.path.dirname(os.path.abspath(lsf.__file__))
test_dir = os.path.join(lsf_dir, 'tests', 'data')

class TestJob(unittest.TestCase):
    def test_make_prexec_test_string(self):
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        home = os.path.expanduser('~')
        self.assertEqual("-E 'test -e " + home + "'", bsub._make_prexec_test_string())

    def test_set_memory_units(self):
        '''Check that we can get memory units or die gracefully'''
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        
        with self.assertRaises(lsf.Error):
            bsub._lsadmin_cmd = 'this_is_not_a_command_and_should_cause_error'
            bsub._set_memory_units()

        with self.assertRaises(lsf.Error):
            bsub._lsadmin_cmd = 'cat ' + os.path.join(test_dir, 'lsf_unittest_lsadmin_showconf_bad_units.txt')
            bsub._set_memory_units()

        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        bsub._lsadmin_cmd = 'cat ' + os.path.join(test_dir, 'lsf_unittest_lsadmin_showconf_no_units.txt')
        bsub._set_memory_units()
        self.assertEqual('KB', bsub.memory_units)

        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        bsub._lsadmin_cmd = 'cat ' + os.path.join(test_dir, 'lsf_unittest_lsadmin_showconf_kb.txt')
        bsub._set_memory_units()
        self.assertEqual('KB', bsub.memory_units)

        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        bsub._lsadmin_cmd = 'cat ' + os.path.join(test_dir, 'lsf_unittest_lsadmin_showconf_mb.txt')
        bsub._set_memory_units()
        self.assertEqual('MB', bsub.memory_units)

    def test_make_resources_string(self):
        '''Check resources in bsub call get set correctly'''
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1.5, 'cmd', no_resources=True)
        self.assertEqual('', bsub._make_resources_string())

        # need to manually set the lsf units for these tests
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1.5, 'cmd')
        bsub.memory_units='MB'
        resources = bsub._make_resources_string()
        self.assertEqual('-R "select[mem>1500] rusage[mem=1500]" -M1500', resources)

        bsub = lsf.Job('out', 'error', 'name', 'queue', 1.5, 'cmd', threads=42)
        bsub.memory_units='KB'
        resources = bsub._make_resources_string()
        self.assertEqual('-n 42 -R "span[hosts=1] select[mem>1500] rusage[mem=1500]" -M1500000', resources)

        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd', tmp_space=42)
        bsub.memory_units='MB'
        resources = bsub._make_resources_string()
        self.assertEqual('-R "select[mem>1000 && tmp>42000] rusage[mem=1000,tmp=42000]" -M1000', resources)

    def test_make_queue_string(self):
        '''Check that queue set correctly'''
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        self.assertEqual('-q queue', bsub._make_queue_string())

    def test_make_output_files_string(self):
        '''Check that the names of the stdout and stderr files set properly'''
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        self.assertEqual('-o out -e error', bsub._make_output_files_string())
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd', array_start=1, array_end=42)
        self.assertEqual('-o out.%I -e error.%I', bsub._make_output_files_string())
        
    def test_make_job_name_string(self):
        '''Check that the name of the job is set correctly'''
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        self.assertEqual('-J name', bsub._make_job_name_string())
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd', array_start=1, array_end=42)
        self.assertEqual('-J "name[1-42]%100"', bsub._make_job_name_string())

    def test_make_dependencies_string(self):
        '''Check that -w 'done()' and 'ended()' are constructed correctly'''
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        self.assertEqual('', bsub._make_dependencies_string())
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd', depend=42)
        self.assertEqual("-w 'done(42)'", bsub._make_dependencies_string())
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd', depend=[42,'a'])
        self.assertEqual("-w 'done(42) && done(\"a\")'", bsub._make_dependencies_string())
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd', ended=42)
        self.assertEqual("-w 'ended(42)'", bsub._make_dependencies_string())

    def test_make_command_string(self):
        '''Check that command to be bsubbed is made correctly - including for job array'''
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        self.assertEqual('cmd', bsub._make_command_string())
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd.INDEX')
        self.assertEqual('cmd.INDEX', bsub._make_command_string())
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd.INDEX foo bar.INDEX', array_start=1, array_end=42)
        self.assertEqual('cmd.\$LSB_JOBINDEX foo bar.\$LSB_JOBINDEX', bsub._make_command_string())

    def test_add_dependency(self):
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        bsub.add_dependency('jobname')
        dependencies = ['"jobname"']
        self.assertListEqual(bsub.run_when_done, dependencies)

        bsub.add_dependency(42)
        dependencies.append('42')
        self.assertListEqual(bsub.run_when_done, dependencies)
     
        bsub.add_dependency('43')
        dependencies.append('43')
        self.assertListEqual(bsub.run_when_done, dependencies)

        bsub.add_dependency(['jobname2', 44])
        dependencies.extend(['"jobname2"', '44'])
        self.assertListEqual(bsub.run_when_done, dependencies)

        self.assertListEqual(bsub.run_when_ended, [])

        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        bsub.add_dependency('jobname', ended=True)
        dependencies = ['"jobname"']
        self.assertListEqual(bsub.run_when_ended, dependencies)

        bsub.add_dependency(42, ended=True)
        dependencies.append('42')
        self.assertListEqual(bsub.run_when_ended, dependencies)
     
        bsub.add_dependency('43', ended=True)
        dependencies.append('43')
        self.assertListEqual(bsub.run_when_ended, dependencies)

        bsub.add_dependency(['jobname2', 44], ended=True)
        dependencies.extend(['"jobname2"', '44'])
        self.assertListEqual(bsub.run_when_ended, dependencies)

        self.assertListEqual(bsub.run_when_done, [])

    def test_set_job_id_from_bsub_output(self):
        '''Check that stdout from calling bsub is parsed correctly'''
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        bsub_out = 'This is not good bsub output\nso should throw error\n'
        with self.assertRaises(lsf.Error):
            bsub._set_job_id_from_bsub_output(bsub_out)

        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        with self.assertRaises(lsf.Error):
            bsub._set_job_id_from_bsub_output('')

        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        with self.assertRaises(lsf.Error):
            bsub._set_job_id_from_bsub_output(None)

        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        bsub_out = 'Job <42> is submitted to queue <normal>.\nAnother line of output\n'
        bsub._set_job_id_from_bsub_output(bsub_out)
        self.assertEqual('42', bsub.job_id)

        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        bsub_out = 'Job <not_an_int> is submitted to queue <normal>\n'
        with self.assertRaises(lsf.Error):
            bsub._set_job_id_from_bsub_output(bsub_out)
         
    def test_run(self):
        '''Test run() runs/fails as expected'''
        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        bsub._run_test_cmd = os.path.join(test_dir, 'lsf_unittest_run_bsub_fails.sh')
        with self.assertRaises(lsf.Error):
            bsub.run()

        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        bsub._run_test_cmd = 'not_a_command_and_should_fail'
        with self.assertRaises(lsf.Error):
            bsub.run()

        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'cmd')
        bsub._run_test_cmd = os.path.join(test_dir, 'lsf_unittest_run_bsub_ok.sh')
        bsub.run()
        self.assertEqual('42', bsub.job_id)

    def test_run_not_bsubbed(self):
        '''Test running not bsubbed on normal command'''
        tmp_out = 'tmp.test_run_not_bsubbed'
        bsub = lsf.Job('tmp.out', 'tmp.err', 'test', 'normal', 1, 'echo "test_run_not_bsubbed" > ' + tmp_out)
        bsub.run_not_bsubbed()
        self.assertTrue(filecmp.cmp(tmp_out, os.path.join(test_dir, 'lsf_unittest_run_not_bsubbed.out')))
        os.unlink(tmp_out)

        bsub = lsf.Job('out', 'error', 'name', 'queue', 1, 'not_a_command_and_should_fail')
        with self.assertRaises(lsf.Error):
            bsub.run_not_bsubbed()

    def test_run_not_bsubbed_array(self):
        '''Test running not bsubbed on job array'''
        tmp_out = 'tmp.test_run_not_bsubbed_array'
        bsub = lsf.Job('tmp.out', 'tmp.err', 'test', 'normal', 1, 'echo "test_run_not_bsubbed array INDEX" >> ' + tmp_out, array_start=1, array_end=3)
        bsub.run_not_bsubbed()
        self.assertTrue(filecmp.cmp(tmp_out, os.path.join(test_dir, 'lsf_unittest_run_not_bsubbed_array.out')))
        os.unlink(tmp_out)


if __name__ == '__main__':
    unittest.main()
