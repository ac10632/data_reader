
from unittest import TestCase
import data_reader.data_reader as d
import importlib
import pkg_resources
import pandas as pd
import numpy as np
import datetime
import time


class TestBuild_data_dictionary(TestCase):

    # 3: w/date: 4870
    def test_new(self):
        
        data_path = pkg_resources.resource_filename('data_reader','data/')
        test_data_path = pkg_resources.resource_filename('data_reader', 'test_data/')

        da = d.BuildDataDictionary()
        da.add_field('originator', field_type='str')
        da.add_field('orignum',field_type='int')
        d.create_reader(da.dictionary, file_format='delim', delimiter=',', string_delim='"')
        import data_reader.reader.reader as r
        parametersa = {}
        parametersa['data_file'] = test_data_path + 'originatorFull.csv'
        parametersa['headers'] = True
        parametersa['output_type'] = 'pandas'
        da_data = r.reader(parametersa)


        # read the zip/MSA data built into data_reader.
        d0 = d.BuildDataDictionary()
        d0.add_field('zip', 'zip',illegal_replacement_value='"00000"')
        d0.add_field('cbsa_code1', 'str')
        d0.add_field('state', 'stateterr')
        d0.add_field('level','str')
        d0.add_field('cbsa_name1', 'str')
        d0.print()

        d.create_reader(d0.dictionary, file_format='delim', delimiter='|')
        importlib.reload(r)
        parameters0 = {}
        parameters0['data_file'] = data_path + 'zipCBSA.dat'
        parameters0['headers'] = False
        parameters0['output_type'] = 'pandas'
        parameters0['user_class'] = d.PopulateCBSAData
        parameters0['user_class_init'] = {'check_state': True}
        parameters0['user_method'] = 'cbsa_code_and_name'
        d0_data = r.reader(parameters0)
        print(d0_data.shape)
        chk = (d0_data['cbsa_code'] != d0_data['cbsa_code1']).sum()
        
        self.assertEqual(chk, 0, 'cbsa map did not work')
        
        
        d1 = d.BuildDataDictionary()
        d1.add_field('obs', 'int')       # counts 1 to 100
        d1.add_field('sin', 'float')     # sin of obs, rounded to 3 digits
        d1.add_field('letters', 'str')   #5 chars of abcdefghijklmnopqurstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ
                                 # starts at obs % 48 character of this
        d1.add_field('state', 'state')   # chooses state starting at 2 *(obs % 11)
        
        base = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        chk_letters = np.array([base[i % 48: 5 + i % 48] for i in range(1,101)])
        base_state = 'AZTXNYCAFLMIOHWIMNNVUT'  #1  2*mod(x,11)
        chk_state = [base_state[2*(i % 11): 2 + 2*(i % 11)] for i in range(1,101)]
        chk_obs = np.arange(1,101)
        chk_sin = np.round(np.sin(chk_obs),3)

        d.create_reader(d1.dictionary, file_format='delim',delimiter=',', string_delim='"')
        #exit()
        importlib.reload(r)
        parameters1 = {}
        parameters1['data_file'] = test_data_path + 'test1.csv'
        parameters1['headers'] = False
        parameters1['output_type'] = 'pandas'
        d1_data = r.reader(parameters1)
        print(d1_data.shape)

        chk = max(abs(np.array(d1_data.obs) - chk_obs))
        self.assertEqual(chk,0,'var obs is not correct')
        
        chk = max(abs(chk_sin - d1_data.sin))
        self.assertEqual(chk,0,'var sin is not correct')
        
        chk = (d1_data.letters != chk_letters).sum()
        self.assertEqual(chk, 0, 'var letters is not correct')
        
        chk = (d1_data.state != chk_state).sum()
        self.assertEqual(chk, 0, 'var state is not correct')
        
        # this file is the same as the above but with headers.  Make the dictionary in a different order
        # and verify the values are the same as above
        d2 = d.BuildDataDictionary()
        d2.add_field('sin', 'float')     # sin of obs, rounded to 3 digits
        d2.add_field('state', 'str')   # chooses state starting at 2 *(obs % 11)
        d2.add_field('letters', 'str')   #5 chars of abcdefghijklmnopqurstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ
                                 # starts at obs % 48 character of this
        d2.add_field('obs', 'int')       # counts 1 to 100

        d.create_reader(d2.dictionary, file_format='delim', delimiter=',')

        importlib.reload(r)
        parameters2 = {}
        parameters2['data_file'] = test_data_path + 'test1_header.dat'
        parameters2['headers'] = True
        parameters2['output_type'] = 'pandas'
        d2_data = r.reader(parameters2)
        
        chk = (d2_data['obs'] != d1_data['obs']).sum()
        self.assertEqual(chk, 0, 'header file var obs does not match')

        chk = (d2_data['letters'] != d1_data['letters']).sum()
        self.assertEqual(chk, 0, 'header file var letters does not match')

        chk = (d2_data['sin'] != d1_data['sin']).sum()
        self.assertEqual(chk, 0, 'header file var sin does not match')

        chk = (d2_data['state'] != d1_data['state']).sum()
        self.assertEqual(chk, 0, 'header file var state does not match')

        # now read a flat file version of the file
        d3 = d.BuildDataDictionary()
        d3.add_field('obs', 'int', field_start=1, field_width=3)
        d3.add_field('sin', 'float', field_start=4, field_width=6)
        d3.add_field('letters', 'str', field_start=10, field_width=5)
        d3.add_field('state', 'state', field_start=15, field_width=2)
        d3.add_field('date1', 'date', field_start=17, field_width=8, field_format='CCYYMMDD')
        d3.add_field('date2', 'date', field_start=25, field_width=10, field_format='MM/DD/CCYY')

        d.create_reader(d3.dictionary, file_format='flat', lrecl=35)

        importlib.reload(r)
        parameters3 = {}
        parameters3['data_file'] = test_data_path + 'test2.dat'
        parameters3['headers'] = False
        parameters3['output_type'] = 'pandas'
        d3_data = r.reader(parameters3)
        print(d3_data.shape)

        chk = (d2_data['obs'] != d3_data['obs']).sum()
        self.assertEqual(chk, 0, 'header file var obs does not match')

        chk = (d2_data['letters'] != d3_data['letters']).sum()
        self.assertEqual(chk, 0, 'header file var letters does not match')

        chk = (d2_data['sin'] != d3_data['sin']).sum()
        self.assertEqual(chk, 0, 'header file var sin does not match')

        chk = (d2_data['state'] != d3_data['state']).sum()
        self.assertEqual(chk, 0, 'header file var state does not match')
        
        chk = (d3_data['date1'] != d3_data['date2']).sum()
        self.assertEqual(chk, 0, 'dates not the same')
        
        # check format E and B

        # now read a flat file version of the file
        d4 = d.BuildDataDictionary()
        d4.add_field('obs', 'int', field_start=1, field_width=3)
        d4.add_field('sin', 'float', field_start=4, field_width=6)
        d4.add_field('letters', 'str', field_start=10, field_width=5)
        d4.add_field('state', 'state', field_start=15, field_width=2)
        d4.add_field('date1', 'date', field_start=17, field_width=8, field_format='CCYYMMDDE')
        d4.add_field('date2', 'date', field_start=25, field_width=10, field_format='mm/dd/ccyyb')

        d.create_reader(d4.dictionary, file_format='flat', lrecl=35)

        importlib.reload(r)
        parameters4 = {}
        parameters4['data_file'] = test_data_path + 'test2.dat'
        parameters4['headers'] = False
        parameters4['output_type'] = 'pandas'
        d4_data = r.reader(parameters4)
        print(d4_data.shape)
        
        days = np.array([x.day for x in np.array(d4_data.date1)])
        chk = (days < 28).sum()
        self.assertEqual(chk, 0, 'not end of month')

        days = np.array([x.day for x in np.array(d4_data.date2)])
        chk = (days != 1).sum()
        self.assertEqual(chk, 0, 'not first of month')

        # check min/max
        d5 = d.BuildDataDictionary()
        d5.add_field('obs', 'int', field_start=1, field_width=3)
        d5.add_field('sin', 'float', field_start=4, field_width=6, maximum_value=0.5, maximum_replacement_value=2,
                     minimum_value=0.0, minimum_replacement_value=-2.0, action='fix')
        d5.add_field('letters', 'str', field_start=10, field_width=5)
        d5.add_field('state', 'state', field_start=15, field_width=2)
        d5.add_field('date1', 'date', field_start=17, field_width=8, field_format='CCYYMMDDE')
        d5.add_field('date2', 'date', field_start=25, field_width=10, field_format='mm/dd/ccyyb')

        d.create_reader(d5.dictionary, file_format='flat', lrecl=35)

        importlib.reload(r)
        parameters5 = {}
        parameters5['data_file'] = test_data_path + 'test2.dat'
        parameters5['headers'] = False
        parameters5['output_type'] = 'pandas'
        d5_data = r.reader(parameters5)
        
        i = d4_data.sin > 0.5
        chk = (d5_data.sin[i] != 2).sum()
        self.assertEqual(chk, 0, 'maximum replacement did not work')

        i = d4_data.sin < 0
        chk = (d5_data.sin[i] != -2).sum()
        self.assertEqual(chk, 0, 'minimum replacement did not work')
        print(d5_data.shape)
        
        # test drop option
        d6 = d.BuildDataDictionary()
        d6.add_field('obs', 'int', field_start=1, field_width=3)
        d6.add_field('sin', 'float', field_start=4, field_width=6, maximum_value=0.5,
                     minimum_value=0.0, action='drop')
        d6.add_field('letters', 'str', field_start=10, field_width=5)
        d6.add_field('state', 'state', field_start=15, field_width=2)
        d6.add_field('date1', 'date', field_start=17, field_width=8, field_format='CCYYMMDDE')
        d6.add_field('date2', 'date', field_start=25, field_width=10, field_format='mm/dd/ccyyb')

        d.create_reader(d6.dictionary, file_format='flat', lrecl=35)

        importlib.reload(r)
        parameters6 = {}
        parameters6['data_file'] = test_data_path + 'test2.dat'
        parameters6['headers'] = False
        parameters6['output_type'] = 'pandas'
        d6_data = r.reader(parameters6)
        print(d6_data.shape)
        chk = d5_data.shape[0] - d6_data.shape[0] - sum(d5_data.sin < 0) - sum(d5_data.sin > 0.5)
        self.assertEqual(chk, 0, 'drop option did not work')

        # Test first_row, last_row
        d7 = d.BuildDataDictionary()
        d7.add_field('obs', 'int', field_start=1, field_width=3)
        d7.add_field('sin', 'float', field_start=4, field_width=6)
        d7.add_field('letters', 'str', field_start=10, field_width=5)
        d7.add_field('state', 'state', field_start=15, field_width=2)
        d7.add_field('date1', 'date', field_start=17, field_width=8, field_format='CCYYMMDDE')
        d7.add_field('date2', 'date', field_start=25, field_width=10, field_format='mm/dd/ccyyb')

        d.create_reader(d7.dictionary, file_format='flat', lrecl=35)

        importlib.reload(r)
        parameters7 = {}
        parameters7['data_file'] = test_data_path + 'test2.dat'
        parameters7['headers'] = False
        parameters7['output_type'] = 'pandas'
        parameters7['first_row'] = 20
        parameters7['last_row'] = 30
        d7_data = r.reader(parameters7)
        print(d7_data.shape)
        chk = (d7_data.obs != np.arange(20,31)).sum()
        self.assertEqual(chk, 0, 'first_row/last_row did not work')
        
        # Test user function
        def user1(fx):
            fx['sin2'] = fx['sin']*fx['sin']
            return True
            
        d8 = d.BuildDataDictionary()
        d8.add_field('obs', 'int', field_start=1, field_width=3)
        d8.add_field('sin', 'float', field_start=4, field_width=6)
        d8.add_field('letters', 'str', field_start=10, field_width=5)
        d8.add_field('state', 'state', field_start=15, field_width=2)
        d8.add_field('date1', 'date', field_start=17, field_width=8, field_format='CCYYMMDDE')
        d8.add_field('date2', 'date', field_start=25, field_width=10, field_format='mm/dd/ccyyb')

        d.create_reader(d8.dictionary, file_format='flat', lrecl=35)

        importlib.reload(r)
        parameters8 = {}
        parameters8['data_file'] = test_data_path + 'test2.dat'
        parameters8['headers'] = False
        parameters8['output_type'] = 'pandas'
        parameters8['user_function'] = user1
        d8_data = r.reader(parameters8)
        print(d8_data.shape)
        delta = abs(d8_data.sin*d8_data.sin- d8_data.sin2).max()
        self.assertAlmostEqual(delta, 0, places=4, msg='user function did not work')
        
        parameters8['output_type'] = 'delim'
        parameters8['output_file'] = '/home/will/tmp/test.csv'
        parameters8['output_delim'] = '|'
        r.reader(parameters8)

        d0 = d.BuildDataDictionary()
        d0.add_field('zip', 'zip',illegal_replacement_value='"00000"')
        d0.add_field('cbsa_code', 'str')
        d0.add_field('state', 'stateterr')
        d0.add_field('level','str')
        d0.add_field('cbsa_name', 'str')
        d0.print()

        d.create_reader(d0.dictionary, file_format='delim', delimiter='|')
        importlib.reload(r)
        parameters0 = {}
        parameters0['data_file'] = data_path + 'zipCBSA.dat'
        parameters0['headers'] = False
        parameters0['output_type'] = 'delim'
        parameters0['output_file'] = '/home/will/tmp/zipCBSA.csv'
        parameters0['output_headers'] = False
        d0_data = r.reader(parameters0)

        import re
        # the first part says 'anything between quotes and leading/trailing spaces
        # the second part says anything except tab, cr, lf, delim and quotes
        print(re.findall(r'( *".*?" *|[^\t\n\r\f\v","]+)', '"a,b", cd|ef, def, jkl m|no,"hel,lo", "goodbye!"'))
        print(re.findall(r'( *".*?" *|[^\t\n\r\f\v"|"]+)', 'a,b| "cd|ef"| def| jkl "m|no" | hel,lo'))

        ###################
        test_data_path = pkg_resources.resource_filename('data_reader', 'test_data/')
        big_data_path = '/media/will/ExtraDrive1/bigdata/'

        path_to_use = test_data_path

        ds = d.BuildDataDictionary()
        ds.add_field('account_number', 'str')
        ds.add_field('r', 'float')
        ds.add_field('income', 'float')
        ds.add_field('age', 'int')
        ds.add_field('open_date', 'date', 'mm/dd/ccyy')
        ds.add_field('close_date', 'date', 'ccyymmdd')
        ds.add_field('trans_code', 'str')
        ds.add_field('state', 'state')
        ds.add_field('rate', 'float')
        ds.add_field('gender', 'str')
        ds.add_field('marketing_flag', 'str')
        d.create_reader(ds.dictionary, file_format='delim', delimiter=',')
        params = {}
        params['data_file'] = path_to_use + 'customers.csv'
        params['output_file'] = path_to_use + 'outcust.csv'
        params['output_type'] = 'pandas'  # 'delim'
        params['headers'] = True
        params['sample_rate'] = 0.01
        # params['first_row'] = 1
        # params['last_row'] = 1000

        #import data_reader.reader.reader as r
        importlib.reload(r)

        # r.reader(params)
        ds_data = d.multi_process(r.reader, params, 2)
        chkinc = (100000 + 100000 * ds_data.r - ds_data.income).sum()
        self.assertEqual(chkinc, 0, 'income did not check')

        def months(row):
            rx = row['close_date'].month - row['open_date'].month
            rx += 12 * (row['close_date'].year - row['open_date'].year)
            return rx

        ad = ds_data.apply(months, axis=1) + 1
        ad.index = ds_data['account_number']
        ad.sort_index(inplace=True)

        ok_id = np.asarray(np.squeeze(ds_data['account_number']))
        ok_id.sort()
        dm = d.BuildDataDictionary()
        dm.add_field('account_number', 'str', legal_values=ok_id, action='drop')
        dm.add_field('r', 'float')
        dm.add_field('cutoff_date', 'date', field_format='CCYYMMDD')
        dm.add_field('balance', 'float')
        d.create_reader(dm.dictionary, file_format='delim', delimiter=',')
        importlib.reload(r)
        param_m = {}
        param_m['data_file'] = path_to_use + 'monthly.csv'
        param_m['output_type'] = 'pandas'
        param_m['headers'] = True
        param_m['window'] = 100000000
        # param_m['first_row'] = 1
        # param_m['last_row'] = 100000
        import data_reader.reader.reader as r
        start_time = time.time()
        dm_data = d.multi_process(r.reader, param_m, 3)  # 2: 1890
        # dm_data = r.reader(param_m)  #2777
        elapsed_time = round(time.time() - start_time, 2)
        print('Elapsed time: ' + str(elapsed_time))
        print(dm_data.head())
        print(dm_data.shape)
        records = dm_data[['account_number', 'r']].groupby('account_number').agg(['count'])
        records.sort_index(inplace=True)
        rr = (np.squeeze(np.asarray(records[records.columns[0]])) - ad).sum()
        if rr != 0:
            ar = (np.squeeze(np.asarray(records[records.columns[0]])) - ad)
            i = ar > 0
            odd = records[i]
            print(odd.head())
        print(rr)
        self.assertEqual(rr, 0, 'record counts not correct')

        
