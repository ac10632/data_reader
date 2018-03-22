import numpy as np

class PopulateCBSAData(object):
    """
    This is an optional add-on class for reader.py.  It adds
    
    - CBSA (Core Based Statistical Area) code
    - optionally the CBSA name
    - optionally the boolean that is False if the zip and state passed in do not agree.
     
     to the data set.
    
    The mapping data is stored in data_reader/data.  To use this class, make the following specifications in
    the dictionary of parameters for reader:
    
    parameters['user_class'] = PopulateCBSAData
    parameters['user_class_init'] = {'check_state': True}  # checks that the state and zip agree
    parameters['user_method'] = 'cbsa_code'                # adds 'cbsa_code' to the output.
    
    """
    
    def __init__(self, check_state=False):
        """
        :param check_state:  if true, adds field 'zip_ok' if state and zip are in agreement.
        :type check_state: bool
        """
        import pkg_resources
        
        # this directory stores the built-in values (zips, states, etc)
        msa__path = pkg_resources.resource_filename('data_reader', 'data/') + 'zipCBSA.dat'
        fi = open(msa__path)
        
        # read in the data needed by this class.
        zip = []
        cbsa = []
        state = []
        cbsa_level = []
        cbsa_name = []
        while True:
            f = fi.readline()
            f = f.strip('\n').strip('\r').strip(' ')
            if not f:
                break
            fx = f.split('|')
            zip += [fx[0]]
            cbsa += [fx[1].strip(' ')]
            state += [fx[2].strip(' ')]
            cbsa_level += [fx[3].strip(' ')]
            cbsa_name += [fx[4].strip(' ')]
        fi.close()
        self.__zip = np.array(zip)
        self.__cbsa_code = np.array(cbsa)
        self.__state = np.array(state)
        self.__cbsa_level = np.array(cbsa_level)
        self.__cbsa_name = np.array(cbsa_name)
        self.__check_state = check_state
    
    def cbsa_code(self, fx):
        """
        This adds a new field to fx: cbsa_code, the CBSA code for this zip.  If the zip is not in a CBSA,
        then the state postal code is put in.
        
        :param fx: line of data from function reader. Must contain a field called 'zip', the 5-digit zip.
        :type fx: dict
        :return: fx['cbsa_code']
        :rtype: str
        """
        
        if self.__check_state:
            self.zip_state_agree(fx)
        
        # do this within a 'try' in case fx doesn't contain the fields we need or is some wrong data type.
        try:
            if fx['zip'] is not None:
                chk = np.searchsorted(self.__zip, fx['zip'])
                if (chk >= self.__zip.shape[0]) or (fx['zip'] != self.__zip[chk]):
                    fx['cbsa_code'] = None
                    fx['cbsa_name'] = None
                else:
                    fx['cbsa_code'] = self.__cbsa_code[chk]
            else:
                fx['cbsa_code'] = None
        except:
            fx['cbsa_code'] = None
        return True
    
    def cbsa_code_and_name(self, fx):
        """
        This adds two new fields to fx:
        
        - cbsa_code, the CBSA code for this zip.  If the zip is not in a CBSA,
          then the state postal code is put in.
        
        - cbsa_name, the name of the CBSA.
        
        :param fx: line of data from function reader. Must contain a field called 'zip', the 5-digit zip.
        :type fx: dict
        :return: fx['cbsa_code'], fx['cbsa_name']
        :rtype: str
        
        """
        
        if self.__check_state:
            self.zip_state_agree(fx)
        
        # do this within a 'try' in case fx doesn't contain the fields we need or is some wrong data type.
        try:
            if fx['zip'] is not None:
                chk = np.searchsorted(self.__zip, fx['zip'])
                if (chk >= self.__zip.shape[0]) or (fx['zip'] != self.__zip[chk]):
                    fx['cbsa_code'] = None
                    fx['cbsa_name'] = None
                else:
                    fx['cbsa_code'] = self.__cbsa_code[chk]
                    fx['cbsa_name'] = self.__cbsa_name[chk]
            else:
                fx['cbsa_code'] = None
                fx['cbsa_name'] = None
        except:
            fx['cbsa_code'] = None
            fx['cbsa_name'] = None
        return True
    
    def zip_state_agree(self, fx):
        """
        
        :param fx: line of data from function reader. Must contain a field called 'zip', the 5-digit zip.
        :type fx: dict
        :return: fx['zip_ok']
        :rtype: bool
        """
        
        # do this within a 'try' in case fx doesn't contain the fields we need or is some wrong data type.
        try:
            fx['zip_ok'] = True
            if (fx['zip'] is not None) and (fx['state'] is not None):
                chk = np.searchsorted(self.__zip, fx['zip'])
                if (chk >= self.__zip.shape[0]) or (fx['zip'] != self.__zip[chk]):
                    fx['zip_ok'] = False
                else:
                    if fx['state'] != self.__state[chk]:
                        fx['zip_ok'] = False
            else:
                fx['zip_ok'] = False
        except:
            fx['zip_ok'] = False


"""
  The routines in this module are create and run modules that read data from a file.
  
  Functionality provided:
  
  - Multiple file types.
  - Multiple data types.
  - Data validation.
  - Data error handling options.
  - Optional user-supplied function available during read.
  - Optional user-supplied class.
  - Multi-processing support.
  - Multiple output options.
  - Read any portion of a file.
  - Random sampling.
  
  

"""


class BuildDataDictionary(object):
    """
    This class builds a data dictionary of the type that is required by create_reader.
    Test test.
    
    """
    
    # resources shared by all instances. Data read only once.
    import pkg_resources
    
    # this directory stores the built-in values (zips, states, etc)
    __path = pkg_resources.resource_filename('data_reader', 'data/')
    
    def __init__(self):
        
        # __ddict is the data dictionary
        self.__ddict = {}
        # this will count the # of entries that have a field_width/field_start syntax
        self.__num_width_fields = 0
    
    @property
    def dictionary(self):
        """
        Return the data dictionary the user has created.  The key into the dictionary is the field number.  This
        starts at 0 and increments with each field the user defines.  The fields should be in the order that the file
        to be read has them in.  The exception is DELIM files that have headers, in which case any order is fine.
        
        :return: data dictionary.
        :rtype: dict
        """
        if len(self.__ddict) > 0:
            return self.__ddict
        else:
            return None
    
    def print(self, field_name=None):
        """
        prints element(s) in the data dictionary.
        field_name can be either the field_name of a field or a list of names.  If unspecified, the entire dictionary
        is printed.
        
        :param field_name: field_name
        :type field_name: str, list
        :return: <None>
        :rtype: <None>
        """
        
        if field_name is None:
            names = [self.__ddict[ind]['field_name'] for ind in range(len(self.__ddict))]
        else:
            if isinstance(field_name, str):
                names = [field_name]
            else:
                if isinstance(field_name, list):
                    names = field_name
                else:
                    raise ValueError('parameter to print must be: None, string or list of strings')
        for ind in range(len(self.__ddict)):
            if self.__ddict[ind]['field_name'] in names:
                tmp = self.__ddict[ind]
                print('Field:                            ' + tmp['field_name'])
                print('Type:                             ' + tmp['field_type'])
                print('Action:                           ' + tmp['action'])
                if tmp['field_format'] is not None:
                    print('Format:                           ' + tmp['field_format'])
                if tmp['minimum_value'] is not None:
                    print('Minimum:                          ' + str(tmp['minimum_value']))
                if tmp['action'] == 'FIX':
                    print('Minimum replacement value         ' + str(tmp['minimum_replacement_value']))
                if tmp['maximum_value'] is not None:
                    print('Maximum:                          ' + str(tmp['maximum_value']))
                if tmp['action'] == 'FIX':
                    print('Maximum replacement value         ' + str(tmp['maximum_replacement_value']))
                if tmp['legal_values'] is not None:
                    l = 'Legal values:                     '
                    for v in tmp['legal_values']:
                        if v != tmp['legal_values'][0]:
                            l += ', '
                        l += str(v)
                        if len(l) > 75:
                            l += '...'
                            break
                    print(l)
                if tmp['action'] == 'FIX':
                    print('illegal replacement value         ' + str(tmp['illegal_replacement_value']))
                print()
                print()
    
    def add_field(self, field_name, field_type='str', field_format=None,
                  field_width=None, field_start=None,
                  action='fix',
                  minimum_value=None, minimum_replacement_value=None,
                  maximum_value=None, maximum_replacement_value=None,
                  legal_values=None, illegal_replacement_value=None):
        
        """
        Adds a field to the data dictionary.
        
        :param field_name: name of the field
        :type field_name: str
        :param field_type: data type of the field
        :type field_type: str
        :param field_format: format of the field (date type only)
        :type field_format: str
        :param field_width: width, in the file, of the field (file type FLAT)
        :type field_width: int
        :param field_start: starting column, in the file, of the field (file type FLAT). First column is 1.
        :type field_start: int
        :param action: action to take if the field value fails validation, DROP, FIX, FATAL
        :type action: str
        :param minimum_value: optional minimum acceptable value for the field
        :type minimum_value: field_type (same type as the field)
        :param minimum_replacement_value: replacement value if action = FIX and value is below min
        :type minimum_replacement_value: field_type (same type as the field)
        :param maximum_value: optional maximum acceptable value for the field
        :type maximum_value: field_type (same type as the field)
        :param maximum_replacement_value: replacement value if action = FIX and value is above max
        :type maximum_replacement_value: field_type (same type as field)
        :param legal_values: optional set of legal values
        :type legal_values: numpy array, pandas series, list
        :param illegal_replacement_value: optional value to use if the value is the file is not legal
        :type illegal_replacement_value: field_type (same type as field)
        :return:
        :rtype:
        """
        
        def __get_static_data(file):
            """
            Read one of the standard files of legal values
            :param file: file name of legal values
            :type file: str
            :return: sorted numpy array of values
            :rtype: numpy array
            """
            try:
                f = open(file)
            except:
                raise FileNotFoundError('cannot open file: ' + str(file))
            data = []
            while True:
                line = f.readline().strip('\n')
                if not line:
                    break
                data += [line]
            data = np.array(data)
            data.sort()
            f.close()
            return data
        
        def __check_type(vartype, value):
            """
            Checks whether *value* is of type *vartype*.
            
            :param vartype: Type to check against.
            :type vartype: str
            :param value: Value to be checked.
            :type value: object
            :return: True if value is of type vartype
            :rtype: bool
            """
            if str(type(value)).upper().find(vartype) >= 0:
                return True
            else:
                if vartype == 'FLOAT':
                    if str(type(value)).upper().find('INT') >= 0:
                        return True
                if vartype == 'DATE':
                    if str(type(value)).upper().find('STR') >= 0:
                        ok = (value.upper()[0:5] == 'DATE(')
                        if not ok:
                            return False
                        tmp = value[5:].strip(')').split(',')
                        try:
                            yr = int(tmp[0])
                        except:
                            return False
                        if yr < 1900 or yr > 2200:
                            return False
                        try:
                            mo = int(tmp[1])
                        except:
                            return False
                        if mo < 1 or mo > 12:
                            return False
                        try:
                            day = int(tmp[2])
                        except:
                            return False
                        if day < 1 or day > 31:
                            return False
                        if (mo in (2, 4, 6, 9, 11)) and (day == 31):
                            return False
                        if (mo == 2) and (day == 30):
                            return False
                        if (mo == 2) and (yr % 4 != 0) and (day == 29):
                            return False
                        return True
                return False
        
        # check that this is legal
        action = action.upper()
        if action not in ('FIX', 'DROP', 'FATAL'):
            raise ValueError('action must be one of: FIX, DROP, FATAL')
        
        # check this is legal
        field_type = field_type.upper()
        if field_type not in ('FLOAT', 'INT', 'STR', 'BYTES', 'DATE', 'ZIP', 'STATE', 'STATETERR'):
            raise ValueError('field_type must be one of: FLOAT, INT, STR, BYTES, DATE, ZIP, STATE, STATETERR')
        
        # check...
        if field_format is not None:
            field_format = field_format.upper()
            if field_type != 'DATE':
                raise ValueError('only DATE field_type supports FORMAT at this time.')
            test = field_format.strip('E').strip('B')
            if test not in ('CCYYMMDD', 'CCYYMM', 'YYMM', 'MM/DD/CCYY', 'MM/DD/YY', 'MMDDCCYY','MM/CCYY', 'CCYY/MM/DD'):
                raise ValueError('supported date formats are: CCYYMMDD CCYYMM YYMM MM/DD/CCYY MM/DD/YY MMDDCCYY MM/CCYY CCYY/MM/DD <E>, <B>')
        
        if field_width is not None:
            self.__num_width_fields += 1
            if field_start is None:
                raise ValueError('field_start is None but field_width is not None')
            try:
                field_width = int(field_width)
            except:
                raise ValueError('field_width must be an integer')
            if field_width < 1:
                raise ValueError('field_width must be positive')
        
        if field_start is not None:
            if field_width is None:
                raise ValueError('field_width is None but field_start is not None.')
            try:
                field_start = int(field_start)
            except:
                raise ValueError('field_start must be positive')
            if field_start < 1:
                raise ValueError('field_start must be positive')
        
        if minimum_value is not None:
            if not __check_type(field_type, minimum_value):
                raise ValueError('minimum_value is not the right type')
        
        if maximum_value is not None:
            if not __check_type(field_type, maximum_value):
                raise ValueError('maximum_value is not the right type')
        
        if minimum_replacement_value is not None:
            if not __check_type(field_type, minimum_replacement_value):
                raise ValueError('minimum_replacement_value is not the right type')
        
        if maximum_replacement_value is not None:
            if not __check_type(field_type, maximum_replacement_value):
                raise ValueError('maximum_replacement_value is not the right type')
        
        # legal values can be specified as a list, pandas series, or numpy array.
        if legal_values is not None:
            if __check_type('LIST', legal_values):
                legal_values = np.array(legal_values)
            else:
                if __check_type('PANDAS.CORE.SERIES.SERIES', legal_values):
                    legal_values = np.array(np.squeeze(legal_values))
                else:
                    if not __check_type('NDARRAY', legal_values):
                        raise ValueError('legal values must be a numpy array, pandas series or list')
            legal_values.sort()
            # check the first few for type
            for (ind, val) in enumerate(legal_values):
                if not __check_type(field_type, val):
                    raise ValueError('legal values not correct type')
                if ind > 10:
                    break
        
        # special field
        if field_type == 'ZIP':
            legal_values = __get_static_data(self.__path + 'zips.dat')
        
        # special field
        if field_type == 'STATE':
            legal_values = __get_static_data(self.__path + 'states.dat')
        
        # special field
        if field_type == 'STATETERR':
            lv0 = __get_static_data(self.__path + 'states.dat')
            lv1 = __get_static_data(self.__path + 'territories.dat')
            legal_values = np.append(lv0, lv1)
            legal_values.sort()
        
        if action in ('FATAL', 'DROP'):
            if maximum_replacement_value is not None:
                raise Warning('maximum_replacement_value will not be used with action: ' + action)
            if minimum_replacement_value is not None:
                raise Warning('minimum_replacement_value will not be used with action: ' + action)
        
        dd = {}
        dd['field_name'] = field_name
        dd['field_type'] = field_type
        dd['field_format'] = field_format
        dd['field_width'] = field_width
        dd['field_start'] = field_start
        dd['action'] = action
        dd['minimum_value'] = minimum_value
        dd['minimum_replacement_value'] = minimum_replacement_value
        dd['maximum_value'] = maximum_value
        dd['maximum_replacement_value'] = maximum_replacement_value
        dd['legal_values'] = legal_values
        dd['illegal_replacement_value'] = illegal_replacement_value
        
        self.__ddict[len(self.__ddict)] = dd
        
        if (self.__num_width_fields > 0) and (self.__num_width_fields != len(self.__ddict)):
            raise ValueError('either no field may have field_start/field_width or all must have it')


def multi_process(reader, params, num_process):
    """
    Function to read a file in multi-process mode.
    
    :param reader: reader function created by create_reader
    :type reader: function
    :param params: parameters to reader function
    :type params: dict
    :param num_process: number of processes to use to read the file
    :type num_process: int
    :return: data read by reader, if not output to a file
    :rtype: list, numpy, pandas or None
    """
    import os
    import multiprocessing as mp
    
    # get the size of the file so it can be chunked up
    try:
        sz = float(os.stat(params['data_file']).st_size) / float(num_process)
    except:
        raise FileNotFoundError('cannot find file: ' + params['data_file'])
    
    # create a tuple of parameters for the num_process calls.  The differences between the parameters for each
    # call are:
    #    start_byte
    #    end_byte
    #    output_file, if the user has specified to write a file of the output.
    p = ()
    start_byte = 0
    end_byte = sz - 1
    version_string = 'abcdefghijklmnopqrstuvqxyz'
    if num_process > 25:
        raise ValueError('number of processes cannot exceed 26')
    for ind in range(num_process):
        px = params.copy()
        px['start_byte'] = start_byte
        px['end_byte'] = end_byte
        # honor any start_row for the first process.
        px['last_row'] = None
        # if the output are files, number them if there are more than 1.
        if (num_process > 1) and (params['output_type'].upper() in ['DELIM', 'TFRECORDS']):
            if px['output_file'].find('.') > 0:
                px['output_file'] = px['output_file'].replace('.', version_string[ind] + '.')
            else:
                px['output_file'] = px['output_file'] + str(ind)
        p += (px,)
        params['first_row'] = None
        start_byte = end_byte
        end_byte += sz
    
    # run reader as multi-process affair
    pool = mp.Pool()
    results = pool.map(reader, p)
    pool.close()
    
    # aggregate the data from the runs.  If the output is a file, there is no output here.
    if params['output_type'].upper() == 'PANDAS':
        output = results[0]
        for ind in (range(1, num_process)):
            output = output.append(results[ind])
        return output
    if params['output_type'].upper() == 'NUMPY':
        output = results[0]
        for r in (range(1, num_process)):
            output = np.append(output, results[ind], axis=0)
        return output
    if params['output_type'].upper() == 'LIST':
        output = []
        for r in results:
            output += r
        return output


def create_reader(data_dict, reader_path=None, file_format='DELIM', delimiter=',', lrecl=None, string_delim=None, \
                  remove_char=None, module_name='reader'):
    """
    Create a module 'reader' which reads in a file.  The output of this function is placed in the directory reader_path.
    If no path is specified, then the output is placed in the reader subdirectory of this module.
    
    The structure of the created module is:
    
    - reader_path/reader.py.  This is the module created here.
    - reader_path/data/data?.dat.  These data files contain the legal values for each variable if the user has
      specified legal values.  The ? increments according to the position of the variable in data_dict.  For example,
      if the first variable has legal values specified, those values are stored in data0.dat.
    
    readers can be created to read two file formats:
    
    - Delimited (file_format = 'DELIM').  Each value is separated by a single character specified by *delimiter*.
    - Flat (file_format = 'FLAT').  These files have a fixed record length as specified by *lrecel*.  The value of
      *lrecl* is the total length of each record including LF, and RET characters.
    
    Elements of data_dict.  Each element of data_dict defines a single field.  The key into data_dict is an
    integer: 0, 1, 2.  These define the order of the variables in the file if the file does not have headers.
    If the file has headers, then the order in data_dict does not matter.
    
    - field_name (str).  Name of the variable (column).
    
    - field_type (str).  Values: 'float', 'int', 'date', 'str', 'bytes', 'zip', 'state', 'stateterr'
    
    - format. For dates only.  Values: CCYYMMDD, CCYYMM, YYMM, MM/DD/CCYY, MM/DD/YY, MMDDCCYY, MM/CCYY, CCYY/MM/DD.
      If E is appened to the
      end, the the date is moved to the last day of the month.  If B is appended to the end, the date is set to the
      first day of the month.
    
    - minimum_value.  Minimum legal value. Default: None (i.e. Python 'None' value.)
    - maximum_value.  Maximum legal value. Default: None.
    - legal_values: numpy array of legal values.  Note that legal_values may be specified for any data type.
    
    - action.  Action to take if
    
        - the value fails to convert to the assigned type
        - is above the maximum specified.
        - is below the minimum specified.
        - is not in the array of legal values.
        
      Available actions:
    
        - fatal.  Abend the process.
        - drop. Drop the observation.
        - replace. Replace the value with the appropriate one of the next three.
        
    - illegal_replacement_value.  Value to use if action='replace'. Default: None.
    
    - minimum_replacement_value.  Value to use if action='replace'. Default: None.
    
    - maximum_replacement_value.  Value to use if action='replace'. Default: None.
    
    - field_start. For flat files, column number in which the field starts. (First column is 1).
    
    - field_width. For flat files, field_width of the field.
    
    Date values may be specified as strings of the form 'date(year, month, day)' or strings of the form
    'CCYY-MM-DD'.
    
    Files with headers may have extra fields but every field in data_dict must be in the file.
    Flat files may also have extra fields.
    
    :param data_dict: defines the data to be read in
    :type data_dict: BuildDataDictionary.dictionary
    :param reader_path: path in which to place the output
    :type reader_path: str
    :param file_format:  format of input file: 'DELIM' or 'FLAT'. Default is 'DELIM'.
    :type file_format: str
    :param delimiter: delimiter for file_format = 'DELIM'.  Default is ','.
    :type delimiter: str (character)
    :param string_delim: delimiter for strings. Default is *None*.
    :type string_delim: str
    :param lrecl: record length for file_format = 'FLAT'. Default is *None*.
    :type lrecl: int
    :param remove_char: character to be removed from any string variables
    :type remove_char: str
    :param module_name: name of the module to create the reader in (default=reader)
    :type module_name: str
    :return: No direct return
    :rtype: <none>

  
    The reader function created by this function:
    
    The dictionary of parameters has the following elements:
    
    - *data_file* (str).  Name of the file to read.
    
    - *module_path* (str).  The path to this module.  If this omitted, then it is assumed that this module is in the
      reader subdirectory of the data_reader module.  The *reader* function needs this path so that it can read legal
      values from the *data* subdirectory within the *reader* directory.
    
    - *output_type* (str).  How to output the data.Choices are:
    
        - list.  A list of lists where each sublist is a row of data.
        - numpy. A numpy matrix.
        - pandas. A pandas DataFrame. This is the default value.
        - delim. A delimited file.
        
    - *output_file* (str). The name of the output file. If 'delim' is chosen, then the data_file is output to
      output_file line by line so the entire dataset is never in memory.  Not needed unless *output_type* = 'delim'.
      
    - *output_delim* (str). The delimiter to separate the fields of *output_file*.  The default value is ','.
    
    - *output_headers* (bool). If *True*, output a header row. Default value is *True*.
    
    - *headers* (bool).  True means the input file has headers.  The default value is *False*.
    
    - *start_byte* (int).  The byte at which to start reading the file.  The default value is 0.  If the value is
      greater than 0, then reading begins at the next line ('\\n') after *start_byte*.
    
    - *end_byte* (int). The byte at which to stop reading the file.  The default value is *None* (read to the end of the
      file).  If a non-*None* value is specified, reading stops at the first line whose first byte is greater than
      end_byte.

    The above two will generally only be used for reading the file in multiprocessing mode.
    
    - *sample_rate* (float). The rate at which to sample the file.  The default value is 1.
    
    - *user_function* (function). A user-supplied function that is called as each row is processed.  It can take only
      one argument, a dictionary.  The dictionary entries have the form: 'field_name': value.  The function can
      modify or add values to the dictionary.
    
    -*user_class* (class), *user_class_init* (dict), *user_method* (str).  A user-supplied class.  It is initialized
      once and then the method is called as each row is processed.  The initialization can take only keyword arguments.
      These are supplied in the dict *user_class_init*.  The method is specified as a string containing the method
      name.
      
    - window (int). Window for mmap.  If *None* there is no window (fastest).
    
    
    - param params. A dictionary of parameters directing the reading of the file.
    - type dict
    - return list, numpy, pandas dataframe, or <none>.
    
    """
    
    import pkg_resources
    
    if (string_delim != None) and (delimiter != ','):
        raise ValueError('string_delim must also have a delim as a comma')
    
    if reader_path is None:
        reader_file = pkg_resources.resource_filename('data_reader', 'reader/') + 'reader.py'
    else:
        reader_file = reader_path + '/' + module_name + '.py'
    try:
        fo = open(reader_file, 'w')
    except:
        raise FileNotFoundError('could not find or open file: ' + reader_file)
    
    fo.write('import datetime\n')
    fo.write('from datetime import date\n')
    fo.write('import re\n')
    fo.write('import mmap\n')
    fo.write('import pkg_resources\n')
    fo.write('import numpy as np\n')
    fo.write('import pandas as pd\n')
    fo.write('import collections as co\n')
    fo.write('import os\n')
    fo.write('from subprocess import call\n')
    fo.write('import tensorflow as tf\n')
    fo.write('\n')

    
    if string_delim is not None:
        fo.write('def parse(in_str):\n')
        fo.write('    """\n')
        fo.write('    This function parses the input line if the user has specified a value for *str_delim*\n')
        fo.write('    This is slower than just using .split, so only incorporate if needed\n')
        fo.write('    This uses the Python module csv\n')
        fo.write('    param : in_str.  Line read from file being processed\n')
        fo.write('    type : bytes')
        fo.write('    param : regular expression that looks for instances of str_delimited strings\n')
        fo.write('    type : compiled regular expression')
        fo.write('    \n')
        fo.write('    return: list of strings that were separated by the delimiter outside of str_delim\n')
        fo.write('    rtype: list\n')
        fo.write('    \n')
        fo.write('    """\n')
        fo.write('    from csv import reader as r\n')
        fo.write('    for line in r([in_str.decode()]):\n')
        fo.write('        return line\n')
        fo.write('\n')

    fo.write('def to_end_of_month(dt):\n')
    fo.write('    """\n')
    fo.write('    Increment a date to the end of the month.\n')
    fo.write('    :param dt: date to operate on\n')
    fo.write('    :type dt: date\n')
    fo.write('    \n')
    fo.write('    :return: dt moved to last day of the month\n')
    fo.write('    :rtype: date\n')
    fo.write('    """\n')

    fo.write('    if dt.month == 12:\n')
    fo.write('        dt = datetime.date(dt.year+1,1,1)\n')
    fo.write('    else:\n')
    fo.write('        dt = dt.replace(day=1).replace(month=(dt.month + 1))\n')
    fo.write('    dt = dt + datetime.timedelta(-1)\n')
    fo.write('    return dt\n')
    fo.write('\n')
    fo.write('\n')

    fo.write('def make_opf(outfile, partition=None, split_number=None):\n')
    fo.write('    """\n')
    fo.write('    create the output file name for output_type="delim"\n')
    fo.write('\n')
    fo.write('    :param outfile: base filename.\n')
    fo.write('    :type str\n')
    fo.write('    :param partition: name of the partition subdirectory\n')
    fo.write('    :type partition: str\n')
    fo.write('    :param split_number: number to append to the file name for splitting files\n')
    fo.write('    :type split_number: int\n')
    fo.write('    """\n')
    fo.write('\n')
    fo.write('    import re\n')
    fo.write('    dot = outfile.rfind(".")\n')
    fo.write('    if dot < 0:\n')
    fo.write('        if outfile[-1] != "/":\n')
    fo.write('            outfile += "/"\n')
    fo.write('        outdir = outfile\n')
    fo.write('        dotpart = ""\n')
    fo.write('        namepart = ""\n')
    fo.write('    else:\n')
    fo.write('        slash = outfile.rfind("/")\n')
    fo.write('        outdir = outfile[0:slash + 1]\n')
    fo.write('        dotpart = outfile[dot:len(outfile)]\n')
    fo.write('        namepart = outfile[slash + 1:dot]\n')
    fo.write('    if partition is not None:\n')
    fo.write('        # check to see no weird charachters\n')
    fo.write("        x = re.findall('[^a-zA-Z[0-9][ \\t\\n\\r\\f\\v]\\\:/.=_-]', partition)\n")
    fo.write('        if len(x) > 0:\n')
    fo.write('            partition = "garbage"\n')
    fo.write('        opf = outdir + partition + "/"\n')
    fo.write('        if not os.path.isdir(opf):\n')
    fo.write('            try:\n')
    fo.write('                os.mkdir(opf)\n')
    fo.write('            except:\n')
    fo.write('                pass\n')
    fo.write('    else:\n')
    fo.write('        opf = outdir\n')
    fo.write('    opf += namepart\n')
    fo.write('    if split_number is not None:\n')
    fo.write('        opf += str(split_number)\n')
    fo.write('    opf += dotpart\n')
    fo.write('    return opf\n')

    # reader function
    fo.write('def reader(params):\n')
    fo.write('    """\n')
    fo.write('    Created by create_reader() of module data_reader.\n')
    fo.write('    \n')
    fo.write('    This is module specially designed to read a specific file type.\n')
    fo.write('    The dictionary of parameters has the following elements:\n')
    fo.write('    \n')
    fo.write('    - *data_file* (str).  Name of the file to read.\n')
    fo.write('    \n')
    fo.write('    - *module_path* (str).  The path to this module.  If this omitted, then it is assumed that\n')
    fo.write(
        '      this module is in the reader subdirectory of the data_reader module.  The *reader* function needs\n')
    fo.write('      this path so that it can read legal values from the *data* subdirectory within the *reader* \n')
    fo.write('      directory.\n')
    fo.write('    \n')
    fo.write('    - *output_type* (str).  How to output the data.Choices are:\n')
    fo.write('    \n')
    fo.write('        - list.  A list of lists where each sublist is a row of data.\n')
    fo.write('        - numpy. A numpy matrix.\n')
    fo.write('        - pandas. A pandas DataFrame. This is the default value.\n')
    fo.write('        - delim. A delimited file.\n')
    fo.write('        - tfrecords. A TensorFlow TFRecord file\n')
    fo.write('    \n')
    fo.write('    - *output_file* (str). The name of the output file. If "delim" is chosen, then the data_file is\n')
    fo.write('      output to output_file line by line so the entire dataset is never in memory.  Not needed unless\n')
    fo.write('      *output_type* = "delim".\n')
    fo.write('    \n')
    fo.write('    - *output_delim* (str). The delimiter for the *output_file*. Default value is ",".\n')
    fo.write('    \n')
    fo.write('    - *output_headers* (bool) If *True*, include headers in *output_file*. Default value is *True*.\n')
    fo.write('    \n')
    fo.write('    - *split_file* (int) If > 0, splits the file into sets of *split_file* rows. Must be at least 10\n')
    fo.write('    \n')
    fo.write('    - *partition* (str) If not None, name of field to partition the data on.\n')
    fo.write('    \n')
    fo.write('    - *gzip* (bool) If *True*, gzip *output_file*. Default value is *False*.\n')
    fo.write('    \n')
    fo.write('    - *headers* (bool).  True means the input file has headers.  The default value is *False*.\n')
    fo.write('    \n')
    fo.write('    - *first_row* (int). The first row of data to read.\n')
    fo.write('    \n')
    fo.write('    - *last_row* (int). The last row of the data to read.\n')
    fo.write('    \n')
    fo.write('      Note that *first_row* and *last_row* are ignored by function *multi_process*.\n')
    fo.write('    \n')
    fo.write('    - *start_byte* (int).  The byte at which to start reading the file.  The default value is 0.\n')
    fo.write('      If the value is greater than 0, then reading begins at the next line ("\\n") after *start_byte*.\n')
    fo.write('    \n')
    fo.write('    - *end_byte* (int). The byte at which to stop reading the file.  The default value is *None*\n')
    fo.write('      (read to the end of the file).  If a non-*None* value is specified, reading stops at the first\n')
    fo.write('      line whose first byte is greater than end_byte.\n')
    fo.write('    \n')
    fo.write('    The above two will generally only be used for reading the file in multiprocessing mode.\n')
    fo.write('    \n')
    fo.write('    - *sample_rate* (float). The rate at which to sample the file.  The default value is 1.\n')
    fo.write('    \n')
    fo.write('    - *user_function* (function). A user-supplied function that is called as each row is processed.\n')
    fo.write('      It can take only one argument, a dictionary.  The dictionary entries have the form: \n')
    fo.write('      "field_name": value.  The function can modify or add values to the dictionary.\n')
    fo.write('      It returns a type *bool*.  If *True*, the row is kept\n')
    fo.write('    \n')
    fo.write('    -*user_class* (class), *user_class_init* (dict), *user_method* (str).  A user-supplied class.\n')
    fo.write('      It is initialized once and then the method is called as each row is processed.  The \n')
    fo.write(
        '      initialization can take only keyword arguments. These are supplied in the dict *user_class_init*.\n')
    fo.write('      The method is specified as a string containing the method name.\n')
    fo.write('      The method returns a type *bool*.  If *True*, the row is kept\n')
    fo.write('    \n')
    fo.write('    - *window* (int). Window for mmap.  If *None* there is no window (fastest)\n')
    fo.write('    \n')
    fo.write('    :param params. A dictionary of parameters directing the reading of the file.\n')
    fo.write('    :type dict\n')
    fo.write('    :return list, numpy, pandas DataFrame, or None.\n')
    fo.write('    \n')
    fo.write('    """\n')
    
    fo.write('    # parse the dictionary of parameters\n')
    fo.write('    # if the parameter is a string, treat as file name to read and default the remaining parameters\n')
    fo.write('    if str(type(params)).find("str") >= 0:\n')
    fo.write('        output_type = "PANDAS"\n')
    fo.write('        data_file = params\n')
    fo.write('        module_path = None\n')
    fo.write('        output_file = None\n')
    fo.write('        start_byte = 0\n')
    fo.write('        end_byte = None\n')
    fo.write('        headers = False\n')
    fo.write('        user_function = None\n')
    fo.write('        user_class = None\n')
    fo.write('        user_class_init = None\n')
    fo.write('        first_row = None\n')
    fo.write('        last_row = None\n')
    fo.write('        output_delim = ","\n')
    fo.write('        output_headers = True\n')
    fo.write('        gzip = False\n')
    fo.write('        split_file = None\n')
    fo.write('        partition = None\n')
    fo.write('        window = None\n')
    fo.write('    # parse through the dictionary of parameters\n')
    fo.write('    else:\n')
    fo.write('        try:\n')
    fo.write('            data_file = params["data_file"]\n')
    fo.write('        except:\n')
    fo.write('            raise ValueError("must specify data_file")\n')
    fo.write('        try:\n')
    fo.write('            output_type = params["output_type"].upper()\n')
    fo.write('        except:\n')
    fo.write('            output_type = "PANDAS"\n')
    fo.write('        try:\n')
    fo.write('            module_path = params["module_path"]\n')
    fo.write('        except:\n')
    fo.write('            module_path = None\n')
    fo.write('        try:\n')
    fo.write('            start_byte = params["start_byte"]\n')
    fo.write('        except:\n')
    fo.write('            start_byte = 0\n')
    fo.write('        try:\n')
    fo.write('            end_byte = params["end_byte"]\n')
    fo.write('        except:\n')
    fo.write('            end_byte = None\n')
    fo.write('        try:\n')
    fo.write('            output_file = params["output_file"]\n')
    fo.write('        except:\n')
    fo.write('            output_file = None\n')
    fo.write('        try:\n')
    fo.write('            output_delim = params["output_delim"]\n')
    fo.write('        except:\n')
    fo.write('            output_delim = ","\n')
    fo.write('        try:\n')
    fo.write('            gzip = params["gzip"]\n')
    fo.write('        except:\n')
    fo.write('            gzip = False\n')
    fo.write('        try:\n')
    fo.write('            output_headers = params["output_headers"]\n')
    fo.write('        except:\n')
    fo.write('            output_headers = True\n')
    fo.write('        try:\n') # new
    fo.write('            split_file = params["split_file"]\n')
    fo.write('            if split_file <= 10:\n')
    fo.write('                split_file = None\n')
    fo.write('        except:\n') # new
    fo.write('            split_file = None\n')
    fo.write('        try:\n')
    fo.write('            partition = params["partition"]\n')
    fo.write('        except:\n')
    fo.write('            partition = None\n')
    fo.write('        try:\n')
    fo.write('            headers = params["headers"]\n')
    fo.write('        except:\n')
    fo.write('            headers = False\n')
    fo.write('        try:\n')
    fo.write('            window = params["window"]\n')
    fo.write('        except:\n')
    fo.write('            window = None\n')
    fo.write('        try:\n')
    fo.write('            sample_rate = params["sample_rate"]\n')
    fo.write('        except:\n')
    fo.write('            sample_rate = 1\n')
    fo.write('        try:\n')
    fo.write('            sample_rate = float(sample_rate)\n')
    fo.write('        except:\n')
    fo.write('            raise ValueError("sample_rate is a float")\n')
    fo.write('        if (sample_rate <= 0.0) or (sample_rate>1.0):\n')
    fo.write('            raise ValueError("sample_rate is >0 and <=1")\n')
    fo.write('        try:\n')
    fo.write('            user_function = params["user_function"]\n')
    fo.write('        except:\n')
    fo.write('            user_function = None\n')
    fo.write('        if (user_function is not None) and (str(type(user_function)).find("function") < 0):\n')
    fo.write('            raise ValueError("user_function is not a function")\n')
    fo.write('        try:\n')
    fo.write('            user_class = params["user_class"]\n')
    fo.write('        except:\n')
    fo.write('            user_class = None\n')
    fo.write('        try:\n')
    fo.write('            user_class_init = params["user_class_init"]\n')
    fo.write('        except:\n')
    fo.write('            user_class_init = None\n')
    fo.write('        try:\n')
    fo.write('            user_method = params["user_method"]\n')
    fo.write('        except:\n')
    fo.write('            user_method = None\n')
    fo.write('        try:\n')
    fo.write('            first_row = params["first_row"]\n')
    fo.write('        except:\n')
    fo.write('            first_row = None\n')
    fo.write('        if first_row is not None:\n')
    fo.write('            try:\n')
    fo.write('                first_row = int(first_row)\n')
    fo.write('            except:\n')
    fo.write('                raise ValueError("first_row must be an integer")\n')
    fo.write('            if first_row < 0:\n')
    fo.write('                raise ValueError("first row must be non-negative")\n')
    fo.write('        try:\n')
    fo.write('            last_row = params["last_row"]\n')
    fo.write('        except:\n')
    fo.write('            last_row = None\n')
    fo.write('        if last_row is not None:\n')
    fo.write('            try:\n')
    fo.write('                last_row = int(last_row)\n')
    fo.write('            except:\n')
    fo.write('                raise ValueError("last_row must be an integer")\n')
    fo.write('            if last_row <= 0:\n')
    fo.write('                raise ValueError("last_row must be positive")\n')
    fo.write('            if (first_row is not None) and (first_row > last_row):\n')
    fo.write('                raise ValueError("last_row cannot be less than first_row")\n')
    fo.write('    \n')
    fo.write('    last_place = start_byte\n')
    fo.write('    # initialize user_class if it has been provided\n')
    fo.write('    if user_class is not None:\n')
    fo.write('        try:\n')
    fo.write('            if user_class_init is not None:\n')
    fo.write('                uc = user_class(**user_class_init)\n')
    fo.write('            else:\n')
    fo.write('                uc = user_class()\n')
    fo.write('            user_methodx = uc.__getattribute__(user_method)\n')
    fo.write('        except:\n')
    fo.write('            raise ValueError("user_class or user_method not valid")\n')
    fo.write('    # regular expression to pull out strings within a delimiter\n')
    fo.write('    # regular expression to separate dates with / in them\n')
    fo.write('    if partition is not None:\n')
    fo.write('        outfile_dict = {}\n')
    fo.write("    parse_date_regexp = '([^/]+)'\n")
    fo.write('    parse_date = re.compile(parse_date_regexp)\n')
    fo.write('    file_count = 0\n') # new
    fo.write('    # open the file we are going to read\n')
    fo.write('    try:\n')
    fo.write('        fi = open(data_file, "r" )\n')
    fo.write('    except:\n')
    fo.write('        raise FileNotFoundError("cannot find/open file: " + data_file)\n')
    fo.write('    m = mmap.mmap(fi.fileno(), 0, access=mmap.ACCESS_READ)\n')
    fo.write('    if start_byte > 0:\n')
    fo.write('        st = m.find(b"\\n",int(start_byte))\n')
    fo.write('        offset = st+1\n')
    fo.write('    else:\n')
    fo.write('        offset = 0\n')
    fo.write('    if end_byte is not None:\n')
    fo.write('        eb = end_byte\n')
    fo.write('        end_byte = m.find(b"\\n", int(end_byte)) + 1\n')
    fo.write('        if end_byte == 0:\n')
    fo.write('            end_byte = eb\n')
    fo.write('    # output_data is a list of lists that holds what we are reading (unless writing to a file)\n')
    fo.write('    output_data = []\n')
    if file_format.upper() not in ['DELIM', 'FLAT']:
        raise ValueError("file format must be either DELIM or FLAT")
    if file_format.upper() == "DELIM":
        if lrecl is not None:
            raise ValueError("lrecl is not used with file_format = 'DELIM'")
        if str(type(delimiter)).find("bytes") > 0:
            delim = delimiter.decode()
        else:
            delim = delimiter
        fo.write("    d = b'" + delim + "'\n")
    else:
        if lrecl is None:
            raise ValueError("must specify lrecl with file_format = 'FLAT'")
    fo.write('    if module_path is None:\n')
    fo.write("        module_path = pkg_resources.resource_filename('data_reader', 'reader/')\n")
    fo.write('    else:\n')
    fo.write("        if module_path[-1] != '/':\n")
    fo.write("            module_path += '/'\n")
    fo.write('    cn = "\\n"')
    
    if reader_path is None:
        fname = pkg_resources.resource_filename('data_reader', 'reader/') + 'data/column_names.dat'
    else:
        fname = reader_path + '/data/column_names.dat'
    try:
        f = open(fname, 'w')
    except:
        raise FileNotFoundError('could not find or open file: ' + fname)
    for ind in range(len(data_dict)):
        f.write(data_dict[ind]['field_name'] + '\n')
    f.close()
    fo.write('    # read in the column names\n')
    fo.write('    # for a FLAT file or a DELIM file, the columns must be in this order (the order built by the user\n')
    fo.write('    # A file with headers can have the columns in a different order.  Indices below then will map\n')
    fo.write('    # the dictionary order to the file order\n')
    fo.write("    data_filename = module_path + '/data/column_names.dat'\n")
    fo.write('    try:\n')
    fo.write("        f = open(data_filename,'r')\n")
    fo.write('    except:\n')
    fo.write("        raise FileNotFoundError('cannot find/open file: ' + data_filename)\n")
    fo.write('    column_names = []\n')
    fo.write('    while True:\n')
    fo.write('        val = f.readline()\n')
    fo.write('        val = val.strip(" ").strip("\\n")\n')
    fo.write('        if not val:\n')
    fo.write('            break\n')
    fo.write('        column_names += [val]\n')
    fo.write('    f.close()\n')
    fo.write('    # legal_values holds the legal values for the fields, as specified by the user.\n')
    fo.write('    # legal_value files are in the same order as the fields in column_names\n')
    fo.write('    # each entry of legal_values is a sorted numpy array\n')
    fo.write('    legal_values = {}\n')
    
    for ind in range(len(data_dict)):
        if data_dict[ind]['legal_values'] is not None:
            # create data? files of legal values that the reader will read back me.
            if reader_path is None:
                fname = pkg_resources.resource_filename('data_reader', 'reader/') + 'data/data' + str(ind) + '.dat'
            else:
                fname = reader_path + '/data/data' + str(ind) + '.dat'
            try:
                f = open(fname, 'w')
            except:
                raise FileNotFoundError('could not find or open file: ' + fname)
            for val in data_dict[ind]['legal_values']:
                if str(type(val)).find('byte') >= 0:
                    f.write(val.decode() + '\n')
                else:
                    f.write(str(val) + '\n')
            f.close()
            fo.write("    data_filename = module_path + '/data/data" + str(ind) + ".dat'\n")
            fo.write('    try:\n')
            fo.write("        f = open(data_filename,'r')\n")
            fo.write('    except:\n')
            fo.write("        raise FileNotFoundError('cannot find/open file: ' + data_filename)\n")
            fo.write('    lv = []\n')
            fo.write('    while True:\n')
            fo.write('        val = f.readline()\n')
            fo.write('        val = val.strip(" ").strip("\\n")\n')
            fo.write('        if not val:\n')
            fo.write('            break\n')
            var_type = data_dict[ind]['field_type'].upper()
            if var_type == 'BYTES':
                fo.write('        val = val.encode()\n')
            if (var_type == 'INT') or (var_type == 'FLOAT') or (var_type == 'DATE'):
                fo.write('        try:\n')
                if var_type == 'INT':
                    fo.write('            val = int(float(val))\n')
                if var_type == 'FLOAT':
                    fo.write('            val = float(val)\n')
                if var_type == 'DATE':
                    fo.write("            val = datetime.datetime.strptime(val,'%Y-%m-%d').date()\n")
                fo.write('        except:\n')
                fo.write("            raise ValueError('Cannot interpret " + data_dict[ind]['field_name'] + \
                         " legal value of ' + str(val) + ' as " + var_type + "')\n")
            fo.write('        lv += [val]\n')
            fo.write('    lv = np.array(lv)\n')
            fo.write('    lv.sort()\n')
            fo.write('    f.close()\n')
            fo.write('    legal_values[' + str(ind) + '] = lv\n')
    
    fo.write('    # if the file to read is type DELIM, it might have headers\n')
    fo.write('    # and the columns can be in any order and there might be extra columns\n')
    if file_format.upper() == 'DELIM':
        fo.write('    if headers:\n')
        fo.write('        headers1 = m.readline().split(d)\n')
        fo.write('        headers1 = [h.decode().strip("\\n").strip("\\r").strip(" ") for h in headers1]\n')
        fo.write('        indices=[]\n')
        fo.write('        for cn in column_names:\n')
        fo.write('            for (ind,h) in enumerate(headers1):\n')
        fo.write('                if cn == h:\n')
        fo.write('                    indices += [ind]\n')
        fo.write('                    break\n')
        fo.write('            else:\n')
        fo.write("                raise ValueError('Column ' + cn + ' not in file')\n")
        fo.write('    else:\n')
        fo.write('        indices = [ind for ind in range(' + str(len(data_dict)) + ')]\n')
        fo.write('    if start_byte > 0:\n')
        fo.write('        m.seek(offset)\n')
    else:
        fo.write('    indices = [ind for ind in range(' + str(len(data_dict)) + ')]\n')
    
    fo.write('    # keep track of the row of the file with row_number\n')
    fo.write('    row_number = 0\n')
    fo.write('    # starting will be true until we find the first data row to keep\n')
    fo.write('    starting = True\n')
    fo.write('    # work through the file\n')
    fo.write('    while True:\n')
    fo.write('        # keep is True if we keep the obs\n')
    fo.write('        keepx = True\n')
    if file_format.upper() == 'DELIM':
        fo.write('        line = m.readline()\n')
        fo.write('        if not line:\n')
        fo.write('            break\n')
        if string_delim is None:
            fo.write('        fx = line.split(d)\n')
        else:
            fo.write('        fx = parse(line)\n')
    if file_format.upper() == 'FLAT':
        fo.write('        fx = []\n')
        fo.write('        if not m[offset:(1+offset)]:\n')
        fo.write('            break\n')
        fo.write('        if (end_byte is not None) and (offset >= end_byte):\n')
        fo.write('            break\n')
        for ind in range(len(data_dict)):
            fo.write('        start_offset = offset + ' + str(data_dict[ind]['field_start'] - 1) + '\n')
            fo.write('        end_offset = start_offset + ' + str(data_dict[ind]['field_width']) + '\n')
            fo.write('        fx += [m[start_offset:end_offset]]\n')
        fo.write('        offset += ' + str(lrecl) + '\n')
    fo.write('        # check to see if it is worth working on this row\n')
    fo.write('        if (sample_rate < 1) and (float(np.random.uniform(0,1,1)) > sample_rate):\n')
    fo.write('            keepx = False\n')
    fo.write('        row_number += 1\n')
    fo.write('        if first_row is not None:\n')
    fo.write('            keepx = keepx and (row_number >= first_row)\n')
    fo.write('        if last_row is not None:\n')
    fo.write('            if row_number > last_row:\n')
    fo.write('                break\n')
    fo.write('        if end_byte is not None:\n')
    fo.write('            if m.tell() > end_byte:\n')
    fo.write('                break\n')
    fo.write('        if keepx:\n')
    fo.write('            fx_out = co.OrderedDict()\n')
    if string_delim is None:
        decodeyn = '.decode()'
    else:
        decodeyn = ''
    for ind in range(len(data_dict)):
        sind = str(ind)
        sind = 'indices[' + sind + ']'
        var_type = data_dict[ind]['field_type'].upper()
        var_name = data_dict[ind]['field_name']
        min_value = data_dict[ind]['minimum_value']
        max_value = data_dict[ind]['maximum_value']
        var_format = data_dict[ind]['field_format']
        if var_type == 'STR':
            fo.write('            try:\n')
            fo.write(
                '                fx[' + sind + '] = fx[' + sind + ']' + decodeyn + ".strip('\\n').strip('\\r').strip(' ')\n")
            if remove_char is not None:
                fo.write(
                    '                fx[' + sind + '] = fx[' + sind + ']' + ".replace('" + remove_char + "','')\n")
            fo.write('            except:\n')
            fo.write('                fx[' + sind + '] = ""\n' )
        if var_type == 'ZIP':
            fo.write('            try:\n')
            fo.write(
                '                fx[' + sind + '] = fx[' + sind + ']' + decodeyn + ".strip('\\n').strip('\\r').strip(' ')\n")
            if remove_char is not None:
                fo.write(
                    '                fx[' + sind + '] = fx[' + sind + ']' + ".replace('" + remove_char + "','')\n")
            fo.write('            except:\n')
            fo.write('                fx[' + sind + '] = ""\n' )

            fo.write('            if len(fx[' + sind + ']) == 3:\n')
            fo.write('                fx[' + sind + '] = "00" + fx[' + sind + ']\n')
            fo.write('            if len(fx[' + sind + ']) == 4:\n')
            fo.write('                fx[' + sind + '] = "0" + fx[' + sind + ']\n')
            fo.write('            try:\n')
            fo.write('                tmp = int(fx[' + sind + '])\n')
            fo.write('            except:\n')
            fo.write('                raise ValueError("zip has non-numeric values")\n')
        if (var_type == 'STATE') or (var_type == 'STATETERR'):
            fo.write('            try:\n')
            fo.write(
                '                fx[' + sind + '] = fx[' + sind + ']' + decodeyn + ".strip('\\n').strip('\\r').strip(' ')\n")
            if remove_char is not None:
                fo.write(
                    '                fx[' + sind + '] = fx[' + sind + ']' + ".replace('" + remove_char + "','')\n")
            fo.write('            except:\n')
            fo.write('                fx[' + sind + '] = ""\n' )

        if var_type == 'DATE':
            fo.write('            try:\n')
            if remove_char is not None:
                fo.write(
                    '                fx[' + sind + '] = fx[' + sind + ']' + decodeyn + ".replace('" + remove_char + "','')\n")
            else:
                fo.write(
                    '                fx[' + sind + '] = fx[' + sind + ']' + decodeyn + '\n')
            if var_format.upper().find('CCYYMMDD') >= 0:
                fo.write('                yr = int(fx[' + sind + '][0:4])\n')
                fo.write('                mo = int(fx[' + sind + '][4:6])\n')
                fo.write('                day = int(fx[' + sind + '][6:8])\n')
                fo.write('                fx[' + sind + '] = datetime.date(yr, mo, day)\n')
            elif var_format.upper().find('CCYYMM') >= 0:
                fo.write('                yr = int(fx[' + sind + '][0:4])\n')
                fo.write('                mo = int(fx[' + sind + '][4:6])\n')
                fo.write('                fx[' + sind + '] = datetime.date(yr, mo, 1)\n')
            elif var_format.upper().find('YYMM') >= 0:
                fo.write('                yr = int(fx[' + sind + '][0:2])\n')
                fo.write('                mo = int(fx[' + sind + '][2:4])\n')
                fo.write('                fx[' + sind + '] = datetime.date(yr, mo, 1)\n')
            elif var_format.upper().find('MM/DD/YY') >= 0:
                fo.write('                dt = [int(x) for x in parse_date.findall(fx[' + sind + '])]\n')
                fo.write('                fx[' + sind + '] = datetime.date(dt[2], dt[0], dt[1])\n')
            elif var_format.upper().find('MMDDCCYY') >= 0:
                fo.write('                yr = int(fx[' + sind + '][4:8])\n')
                fo.write('                mo = int(fx[' + sind + '][0:2])\n')
                fo.write('                day = int(fx[' + sind + '][2:4])\n')
                fo.write('                fx[' + sind + '] = datetime.date(yr, mo, day)\n')
            elif var_format.upper().find('MM/CCYY') >= 0:
                fo.write('                yr = int(fx[' + sind + '][3:7])\n')
                fo.write('                mo = int(fx[' + sind + '][0:2])\n')
                fo.write('                fx[' + sind + '] = datetime.date(yr, mo, 1)\n')
            elif var_format.upper().find('CCYY/MM/DD') >= 0:
                fo.write('                yr = int(fx[' + sind + '][0:4])\n')
                fo.write('                mo = int(fx[' + sind + '][5:7])\n')
                fo.write('                day = int(fx[' + sind + '][8:10])\n')
                fo.write('                fx[' + sind + '] = datetime.date(yr, mo, day)\n')
            else:
                fo.write('                dt = [int(x) for x in parse_date.findall(fx[' + sind + '])]\n')
                fo.write('                fx[' + sind + '] = datetime.date(dt[2], dt[0], dt[1])\n')
            if var_format[-1] == 'E':
                fo.write('                fx[' + sind + '] = to_end_of_month(fx[' + sind + '])\n')
            if var_format[-1] == 'B':
                fo.write('                fx[' + sind + '] = fx[' + sind + '].replace(day=1)\n')
            
            fo.write('            except:\n')
            
            if data_dict[ind]['action'].upper() == 'FATAL':
                fo.write(
                    "                raise ValueError('type conversion error. Field:  " + var_name + \
                    ", Value: '  + str(fx[" + sind + "]) + ' is not " + data_dict[ind]['field_type'] + "')\n")
            else:
                if data_dict[ind]['action'].upper() == 'DROP':
                    fo.write('                keepx = False\n')
                else:
                    fo.write('                fx[' + sind + '] = ' + str(data_dict[ind]['illegal_replacement_value'])
                             + '\n')
        if (var_type == 'INT') or (var_type == 'FLOAT'):
            fo.write('            try:\n')
            if remove_char is not None:
                fo.write(
                    '                fx[' + sind + '] = fx[' + sind + ']' + decodeyn + ".replace('" + remove_char + "','')\n")
            if var_type == 'INT':
                # this will truncate a float..dropping *float* will produce an error for '3.2'
                fo.write('                fx[' + sind + '] = int(float(fx[' + sind + ']))\n')
            else:
                fo.write('                fx[' + sind + '] = float(fx[' + sind + '])\n')
            fo.write('            except ValueError:\n')
            if data_dict[ind]['action'].upper() == 'FATAL':
                fo.write(
                    "                raise ValueError('type conversion error. Field:  " + var_name + \
                    ", Value: '  + str(fx[" + sind + "]) + ' is not " + data_dict[ind]['field_type'] + "')\n")
            else:
                if data_dict[ind]['action'].upper() == 'DROP':
                    fo.write('                keepx = False\n')
                else:
                    fo.write('                fx[' + sind + '] = ' + str(data_dict[ind]['illegal_replacement_value'])
                             + '\n')
        if min_value is not None:
            fo.write('            # check vs. min value\n')
            fo.write('            if (fx[' + sind + '] is not None) and (fx[' + sind + '] < ' + str(min_value) + '):\n')
            if data_dict[ind]['action'].upper() == 'FATAL':
                fo.write("                raise ValueError('value of " + var_name + " below minimum of " + \
                         str(min_value) + "')\n")
            else:
                if data_dict[ind]['action'].upper() == 'DROP':
                    fo.write('                keepx = False\n')
                else:
                    fo.write(
                        '                fx[' + sind + '] = ' + str(data_dict[ind]['minimum_replacement_value']) + '\n')
        if max_value is not None:
            fo.write('            # check vs. max value\n')
            fo.write('            if (fx[' + sind + '] is not None) and (fx[' + sind + '] > ' + str(max_value) + '):\n')
            if data_dict[ind]['action'].upper() == 'FATAL':
                fo.write("                raise ValueError('value of " + var_name + " above maximum of " + \
                         str(max_value) + "')\n")
            else:
                if data_dict[ind]['action'].upper() == 'DROP':
                    fo.write('                keepx = False\n')
                else:
                    fo.write(
                        '                fx[' + sind + '] = ' + str(data_dict[ind]['maximum_replacement_value']) + '\n')
        #
        if data_dict[ind]['legal_values'] is not None:
            fo.write('            # check vs. legal values\n')
            fo.write('            chk = np.searchsorted(legal_values[' + str(ind) + '], fx[' + sind + '])\n')
            fo.write('            if (chk == legal_values[' + str(ind) + '].size) or ' +
                     '(fx[' + sind + '] != legal_values[' + str(ind) + '][chk]):\n')
            if data_dict[ind]['action'].upper() == 'FATAL':
                fo.write("                raise ValueError('value of " + data_dict[ind]['field_name'] + \
                         " of ' + str(fx[" + sind + "]) + ' is not legal')\n")
            else:
                if data_dict[ind]['action'].upper() == 'DROP':
                    fo.write('                keepx = False\n')
                else:
                    fo.write(
                        '                fx[' + sind + '] = ' + str(data_dict[ind]['illegal_replacement_value']) + '\n')
        fo.write('            fx_out[column_names[' + str(ind) + ']] = fx[' + sind + ']\n')
    fo.write('            if keepx:\n')
    fo.write('                if user_function is not None:\n')
    fo.write('                    keepx = user_function(fx_out)\n')
    fo.write('                if keepx and (user_class is not None):\n')
    fo.write('                    keepx = user_methodx(fx_out)\n')
    fo.write('                if keepx:\n')
    fo.write('                    if starting:\n')
    fo.write('                        out_names = list(fx_out.keys())\n')
    fo.write('                        if partition is not None:\n')
    fo.write('                            if partition not in fx_out.keys():\n')
    fo.write('                                raise ValueError("partition variable not in output file")\n')
    fo.write('                            out_names = [r for r in out_names if r != partition]\n')
    fo.write("                    if output_type == 'DELIM':\n")
    fo.write('                        if partition is None:\n')
    fo.write('                            if starting:\n')
    fo.write('                                row_count = 0\n')
    fo.write('                                try:\n')
    fo.write('                                    if split_file is not None:\n')
    fo.write('                                        opf = make_opf(output_file, None, file_count)\n')
    fo.write('                                        fo = open(opf,"w")\n')
    fo.write('                                        file_count += 1\n')
    fo.write('                                    else:\n')
    fo.write('                                        opf = output_file\n')
    fo.write('                                        fo = open(opf,"w")\n') # (new) indented
    fo.write('                                except:\n')
    fo.write('                                    raise FileNotFoundError("cannot open file: " + output_file)\n')
    fo.write('                                starting = False\n')
    fo.write('                                if output_headers:\n')
    fo.write('                                    for (index,field) in enumerate(out_names):\n')
    fo.write('                                        fo.write(field)\n')
    fo.write('                                        if index < len(out_names) - 1:\n')
    fo.write('                                            fo.write(output_delim)\n')
    fo.write('                                        else:\n')
    fo.write('                                            fo.write(cn)\n')
    fo.write('                            row_count += 1\n')
    fo.write('                            for (index,field) in enumerate(out_names):\n')
    fo.write('                                fo.write(str(fx_out[field]))\n')
    fo.write('                                if index < len(out_names)-1:\n')
    fo.write('                                    fo.write(output_delim)\n')
    fo.write('                                else:\n')
    fo.write('                                    fo.write(cn)\n')
    fo.write('                            if split_file is not None and row_count > split_file:\n')
    fo.write('                                fo.close()\n')
    fo.write('                                starting = True\n')
    fo.write('                                if gzip:\n')
    fo.write("                                    call(['gzip', opf])\n")
    fo.write('                        else:\n')
    fo.write('                            if fx_out[partition] in outfile_dict.keys():\n')
    fo.write('                                if outfile_dict[fx_out[partition]][2] < 0:\n')
    fo.write('                                    fc = outfile_dict[fx_out[partition]][3]\n')
    fo.write('                                    outfile_dict[fx_out[partition]][0] = make_opf(output_file, partition + "=" + str(fx_out[partition]), fc)\n')
    fo.write('                                    outfile_dict[fx_out[partition]][1] = open(outfile_dict[fx_out[partition]][0],"a")\n')
    fo.write('                                    outfile_dict[fx_out[partition]][2] = 0\n')
    fo.write('                                fo = outfile_dict[fx_out[partition]][1]\n')
    fo.write('                            else:\n')
    fo.write('                                if split_file is not None:\n')
    fo.write('                                    opf = make_opf(output_file, partition + "=" + str(fx_out[partition]), 0)\n')
    fo.write('                                else:\n')
    fo.write('                                    opf = make_opf(output_file, partition + "=" + str(fx_out[partition]))\n')
    fo.write('                                fo = open(opf,"a")\n')
    fo.write('                                outfile_dict[fx_out[partition]] = [opf, fo, 0, 0]\n')
    fo.write('                            outfile_dict[fx_out[partition]][2] += 1\n')
    fo.write('                            for (index,field) in enumerate(out_names):\n')
    fo.write('                                fo.write(str(fx_out[field]))\n')
    fo.write('                                if index < len(out_names)-1:\n')
    fo.write('                                    fo.write(output_delim)\n')
    fo.write('                                else:\n')
    fo.write('                                    fo.write(cn)\n')

    fo.write('                            if split_file is not None and outfile_dict[fx_out[partition]][2] > split_file:\n')
    fo.write('                                fo.close()\n')
    fo.write('                                if gzip:\n')
    fo.write("                                    call(['gzip', outfile_dict[fx_out[partition]][0]])\n")
    fo.write('                                outfile_dict[fx_out[partition]][3] += 1\n')
    fo.write('                                outfile_dict[fx_out[partition]][2] = -1\n')

    fo.write("                    elif output_type == 'TFRECORDS':\n")
    fo.write('                        if starting:\n')
    fo.write('                            row_count = 0\n')
    fo.write('                            try:\n')
    fo.write('                                writer = tf.python_io.TFRecordWriter(output_file)\n')
    fo.write('                            except:\n')
    fo.write('                                raise FileNotFoundError("cannot open file: " + output_file)\n')
    fo.write('                            starting = False\n')
    
    fo.write('                        feature = {}\n')
    fo.write('                        for (index,field) in enumerate(out_names):\n')
    fo.write('                            field_type = str(type(fx_out[field]))\n')
    fo.write("                            if field_type.find('float') >= 0:\n")
    fo.write('                                f = tf.train.Feature(float_list=tf.train.FloatList(value=[fx_out[field]]))\n')
    fo.write("                            elif field_type.find('str') >= 0 or field_type.find('zip') >= 0 or field_type.find('state') >= 0:\n")
    fo.write('                                xf = [a.encode() for a in fx_out[field]]\n')
    fo.write('                                f = tf.train.Feature(bytes_list=tf.train.BytesList(value=xf))\n')
    fo.write("                            elif field_type.find('int') >= 0:\n")
    fo.write('                                f = tf.train.Feature(int64_list=tf.train.Int64List(value=[fx_out[field]]))\n')
    fo.write("                            elif field_type.find('bytes') >= 0: \n")
    fo.write('                                xf = [a for a in fx_out[field]]\n')
    fo.write('                                f = tf.train.Feature(bytes_list=tf.train.BytesList(value=xf))\n')
    fo.write("                            elif field_type.find('date') >= 0: \n")
#    fo.write('                                dt = 10000*fx_out[field].year + 100*fx_out[field].month + fx_out[field].day\n')
    fo.write('                                dt = [fx_out[field].year, fx_out[field].month, fx_out[field].day]\n')
    fo.write('                                f = tf.train.Feature(int64_list=tf.train.Int64List(value=dt))\n')
    fo.write('                            feature[field] = f\n')
    fo.write('                        features = tf.train.Features(feature=feature)\n')
    fo.write('                        example = tf.train.Example(features=features)\n')
    fo.write('                        writer.write(example.SerializeToString())\n')

    fo.write('                    else:\n')
    fo.write('                        fx_out = [fx_out[cc] for cc in list(fx_out.keys())]\n')
    fo.write('                        output_data += [fx_out]\n')
    fo.write('                    if window is not None:\n')
    fo.write('                        place = m.tell()\n')
    fo.write('                        if int(place / window) > int(last_place / window):\n')
    fo.write('                            m.close()\n')
    fo.write('                            m = mmap.mmap(fi.fileno(), 0, access=mmap.ACCESS_READ)\n')
    fo.write('                            m.seek(place)\n')
    fo.write('                            last_place = place\n')
    fo.write('    m.close()\n')
    fo.write('    fi.close()\n')
    fo.write('    # select output type and we are done.\n')
    fo.write("    if output_type == 'LIST':\n")
    fo.write('        return output_data\n')
    fo.write("    elif output_type == 'NUMPY':\n")
    fo.write('        return np.matrix(output_data)\n')
    fo.write("    elif output_type == 'PANDAS':\n")
    fo.write('        return pd.DataFrame(output_data, columns=out_names)\n')
    fo.write("    elif output_type == 'DELIM':\n")
    fo.write('        if partition is None:\n')
    fo.write('            fo.close()\n')
    fo.write('            if gzip:\n')
    fo.write("                call(['gzip', opf])\n")
    fo.write('        else:\n')
    fo.write('             for key in outfile_dict.keys():\n')
    fo.write('                 outfile_dict[key][1].close()\n')
    fo.write('                 if gzip:\n')
    fo.write("                     call(['gzip', outfile_dict[key][0]])\n")
    fo.write("    elif output_type == 'TFRECORDS':\n")
    fo.write('        writer.close()\n')
    fo.close()


def make_input_fn(data_dict, module_file, dep_var=None, dates='CCYYMMDD'):
    """
    
    This function creates an input function, 'input_fn' for the TensorFlow estimator class.
    
    if dep_var is specified, then it is popped off the features dictionary and returned separately. In this case,
    the return is
    
    - a dictionary of tensors that are the features of the analysis.
    - a tensor that is the dependent variable.
    
    If it is not specified, then there is a single return: a dictionary of tensors.
    
    TensorFlow does not have a dates data type. data_reader stores dates as a 3-tuple (year, month, day) in the
    TFRecord file.  The dates option determines how these are treated.  In each case, the length is reduced from 3
    to 1 and the return is an Int64.  The options are:
    
    - CCYYMMDD
    - CCYYMM
    - CCYY
    - MM
    - DD

    There is a single option: all dates will be processed the same way.
    If something else is needed, a new field should be created in the 'reader'.
    
    :param data_dict: data dictionary of data to be used
    :type data_dict: BuildDataDictionary.dictionary
    :param module_file: file to create with function 'model_columns'
    :type module_file: str
    :param dep_var: name of the tensor that will be the dependent variable in the analysis.
    :type dep_var: str
    :param dates: How to treat dates
    :type dates: str
    :return: <none>
    :rtype: <none>
    """
    fo = open(module_file, 'w')
    fo.write('import tensorflow as tf\n\n')
    fo.write('def input_fn(files, batch_size=1, shuffle=0, skip=0, take=0, num_epochs=1, parallel_calls=4, include_columns=None):\n')
    fo.write('    """\n')
    fo.write('    :param files: file (or list of files) to read\n')
    fo.write('    :type file: str or list of str\n')
    fo.write('    :param batch_size: number of records to read at each shot\n')
    fo.write('    :type batch_size: int\n')
    fo.write('    :param shuffle: number of records to shuffle (0=none)\n')
    fo.write('    :type shuffle: int\n')
    fo.write('    :param take: number of records to read before EOF is issued\n')
    fo.write('    :type take: int\n')
    fo.write('    :param num_epochs: number of times to repeat through the data set\n')
    fo.write('    :type num_echochs: int\n')
    fo.write('    :param parallel_calls: number of threads to use\n')
    fo.write('    :type parallel_calls: int\n')
    fo.write('    :param include_columns: same as for *model_columns*: includes output type for ints and dates\n')
    fo.write('    :type include_columns: dict\n')
    fo.write('\n')
    fo.write('    For fields of type INT or DATE, the *include_columns* dictionary directs how to treat the output.\n')
    fo.write('    The dictionary should have a key for the field that points to a dictionary.  That dictionary\n')
    fo.write('    may have a key "type" that is either FLOAT or STR to treat the output as numeric or string.\n')
    fo.write('    If *include_columns* is not passed or it does not have a key for a given INT/DATE, the result\n')
    fo.write('    is treated as numeric.\n')
    fo.write('\n')
    fo.write('    """\n')
    fo.write('    def parse_input(proto):\n')
    fo.write('        shape_np = 1\n')
    fo.write('        keys_to_features = {}\n')
    for ind in range(len(data_dict)):
        field_name = data_dict[ind]['field_name']
        field_type = data_dict[ind]['field_type'].upper()
        line = '        '
        if field_type == 'FLOAT':
            line += "keys_to_features['" + field_name + "'] = tf.FixedLenFeature((shape_np), tf.float32)\n"
        elif field_type == 'INT':
            line += "keys_to_features['" + field_name + "'] = tf.FixedLenFeature((shape_np), tf.int64)\n"
        elif field_type == 'DATE':
            line += "keys_to_features['" + field_name + "'] = tf.FixedLenFeature((shape_np, 3), tf.int64)\n"
        elif field_type in ('STR', 'BYTES', 'STATE', 'ZIP', 'STATETERR'):
            line += "keys_to_features['" + field_name + "'] = tf.VarLenFeature(tf.string)\n"
        fo.write(line)
    fo.write('        parsed_features = tf.parse_single_example(proto, keys_to_features)\n')
    if dep_var is None:
        fo.write('        return parsed_features\n')
    else:
        fo.write("        dep_var = parsed_features.pop('" + dep_var + "')\n")
        fo.write('        return parsed_features, dep_var\n')
    fo.write('\n')
    fo.write('    ds = tf.data.TFRecordDataset(files).skip(skip)\n')
    fo.write('    if take > 0:\n')
    fo.write('        ds = ds.take(take)\n')
    fo.write('    if shuffle > 0:\n')
    fo.write('        ds = ds.suffle(buffer_size=shuffle)\n')
    fo.write('    ds = ds.map(parse_input, num_parallel_calls=parallel_calls)\n')
    fo.write('    if num_epochs > 1:\n')
    fo.write('        ds = ds.repeat(num_epochs)\n')
    fo.write('    ds = ds.batch(batch_size)\n')
    fo.write('    iter = ds.make_one_shot_iterator()\n')
    if dep_var is None:
        fo.write('    x = iter.get_next()\n')
    else:
        fo.write('    (features, dep_var) = iter.get_next()\n')
    for ind in range(len(data_dict)):
        field_name = data_dict[ind]['field_name']
        field_type = data_dict[ind]['field_type'].upper()
        if field_name != dep_var:
            if field_type in ('STR', 'BYTES', 'STATE', 'ZIP', 'STATETERR'):
                fo.write("    e = features.pop('" + field_name + "')\n")
                fo.write('    cols = e.dense_shape[1]\n')
                fo.write("    d = tf.sparse_to_dense(e.indices, (batch_size, cols), e.values, '')\n")
                fo.write('    h = tf.reduce_join(d,1)\n')
                fo.write("    features['" + field_name + "'] = h\n")
            if data_dict[ind]['field_type'].upper() == 'DATE':
                dates = dates.upper()
                if dates == 'CCYY':
                    fo.write("    a = features['" + field_name + "']\n")
                    fo.write('    yr = tf.as_string(tf.slice(a, [0, 0, 0], [batch_size, 1, 1]))\n')
                    fo.write("    features['" + field_name +"'] = yr\n")
                elif dates == 'MM':
                    fo.write("    a = features['" + field_name + "']\n")
                    fo.write('    mon = tf.as_string(tf.slice(a, [0, 0, 1], [batch_size, 1, 1]))\n')
                    fo.write("    features['" + field_name +"'] = mon\n")
                elif dates == 'DD':
                    fo.write("    a = features['" + field_name + "']\n")
                    fo.write('    day = tf.as_string(tf.slice(a, [0, 0, 2], [batch_size, 1, 1]))\n')
                    fo.write("    features['" + field_name + "'] = day\n")
                elif dates == 'CCYYMM':
                    fo.write("    a = features['" + field_name + "']\n")
                    fo.write('    yr = tf.slice(a, [0, 0, 0], [batch_size, 1, 1])\n')
                    fo.write('    mon = tf.slice(a, [0, 0, 1], [batch_size, 1, 1])\n')
                    fo.write('    yrmon = tf.as_string(tf.add(tf.multiply(yr,100),mon))\n')
                    fo.write("    features['" + field_name + "'] = yrmon\n")
                elif dates == 'CCYYMMDD':
                    fo.write("    a = features['" + field_name + "']\n")
                    fo.write('    yr = tf.slice(a, [0, 0, 0], [batch_size, 1, 1])\n')
                    fo.write('    mon = tf.slice(a, [0, 0, 1], [batch_size, 1, 1])\n')
                    fo.write('    day = tf.slice(a, [0, 0, 2], [batch_size, 1, 1])\n')
                    fo.write('    yrmonday = tf.as_string(tf.add(tf.add(tf.multiply(yr,10000),tf.multiply(mon,100)),day))\n')
                    fo.write("    features['" + field_name + "'] = yrmonday\n")
            if field_type in ('INT', 'DATE'):
                fo.write("    if include_columns is not None and '" + field_name + "' in include_columns.keys():\n")
                fo.write("        if include_columns['" + field_name + "']['type'].upper() == 'STR':\n")
                fo.write("            e = features.pop('" + field_name + "')\n")
                fo.write('            estr = tf.as_string(e)\n')
                fo.write("            features['" + field_name + "'] = estr\n")

    if dep_var is None:
        fo.write('    return features\n')
    else:
        fo.write('    return features, dep_var\n')
    fo.write('\n')
    fo.close()
    return


def make_model_columns(data_dict, module_file):
    """
    Creates a function 'model_columns' in the file 'module_file'.
    The idea is:
    
    1. Create a data dictionary using BuildDataDictionary to be used to process the data.
    2. Use that same dictionary that allows the user to specify to TensorFlow the models in the column
    
    The function model_columns takes a single input: a dictionary 'include_columns'. The keys to the
    dictionary are the features to include in the model.  These features must be in data_dict.
    
    The dictionary entries are:
    
    - For float columns:
        - Any value (e.g. 'yes') to include the column as-as
        - A list of boundary values to bucketize the column.
    - for a str, bytes, state, zip, int: embed_size:
        - 0 if the column is to be included as one-hot encoded.
        - > 0 to included an embedded layer of 'embed_size' elements.
    
    For str and bytes fields, if legal_values is specified, then TensorFlow is given this list.  If an
    illegal_replacement_value is given, that is included.  If these values are not in data_dict, TensorFlow uses
    a hash function with a size of 1,000,000.
    
    TensorFlow uses a list of legal values for state and zip variables with a missing value of XX and 00000,
    respecively.

    :param data_dict: data dictionary of data to be used
    :type data_dict: BuildDataDictionary.dictionary
    :param module_file: file to create with function 'model_columns'
    :type module_file: str
    :return: <none>
    :rtype: <none>
    """
    import pkg_resources
    
    fo = open(module_file, 'w')
    fo.write('import tensorflow as tf\n\n')
    fo.write('def model_columns(include_columns):\n')
    fo.write('    """"\n')
    fo.write('    :param include_columns: features in the model\n')
    fo.write('    :type include_columns: dict\n')
    fo.write('\n')
    fo.write('    *include_columns* has a key for each feature to include in the model. The keys is the feature name\n')
    fo.write('    The dict entry is also a dict that specifies the options for handling the feature.\n')
    fo.write('\n')
    fo.write('    Handling for string, bytes (and DATE/INT when key "type" is STR): \n')
    fo.write('    - if the data dictionary specifies legal values, then category_column_with_vocabulary_list is used.\n')
    fo.write('    - if the dict has an entry *vocab_list* then that vocabularly list is used. Note: the *vocab_list*\n')
    fo.write('        must be of type str.\n')
    fo.write('    - if the dict has an entry *hash_size* then a hash list of that size is used\n')
    fo.write('    - if there are none of the above, then a hash list of 1000 is used\n')
    fo.write('    - if the dict has an entry *n_oov* then there are this many out-of-value catch-all buckets\n')
    fo.write('    - if the dict has an entry *embed_size* then an embedded layer of that size is used\n')
    fo.write('\n')
    fo.write('    Handling for float (and DATE/INT when key "type" is FLOAT): \n')
    fo.write('    - if nothing is specified, then it is treated as numeric_column\n')
    fo.write('    - if the dict has a key *boundaries* with a list of boundary values, then bucketized_column is used\n')
    fo.write('\n')
    fo.write('    """\n')
    fo.write('    columns = []\n')
    for ind in range(len(data_dict)):
        field_name = data_dict[ind]['field_name']
        field_type = data_dict[ind]['field_type'].upper()
        fo.write("    if '" + field_name + "' in include_columns.keys():\n")
        
        if field_type == 'FLOAT':
            fo.write('        try:\n')
            fo.write("            boundaries = include_columns['" + field_name + "']['boundaries']\n")
            line = '            '
            line += "tmp_field = tf.feature_column.numeric_column('" + field_name + "')\n"
            fo.write(line)
            line = '            '
            line += field_name + ' = tf.feature_column.bucketized_column(tmp_field, boundaries)\n'
            fo.write(line)
            fo.write('        except:\n')
            line = '            '
            line += field_name + " = tf.feature_column.numeric_column('" + field_name + "')\n"
            fo.write(line)
        elif field_type in ('STR', 'BYTES'):
            if data_dict[ind]['legal_values'] is None:
                fo.write('        try:\n')
                fo.write("            vocab_list = include_columns['" + field_name + "']['vocab_list']\n")
                fo.write('            try:\n')
                fo.write("                n_oov = include_columns['" + field_name + "']['n_oov']\n")
                fo.write('            except:\n')
                fo.write('                n_oov = 0\n')
                line = '            '
                line += "tmp_field = tf.feature_column.categorical_column_with_vocabulary_list("
                line += "'" + field_name + "', vocabulary_list=vocab_list, num_oov_buckets=n_oov)\n"
                fo.write(line)
                fo.write('        except:\n')
                fo.write('            try:\n')
                fo.write("                hash_size = include_columns['" + field_name + "']['hash_size']\n")
                fo.write('            except:\n')
                fo.write('                hash_size = 1000\n')
                line = '            '
                line += "tmp_field = tf.feature_column.categorical_column_with_hash_bucket("
                line += "'" + field_name + "', hash_bucket_size=hash_size)\n"
                fo.write(line)
            else:
                fo.write('        vocab = []\n')
                for v in data_dict[ind]['legal_values']:
                    fo.write("        vocab += ['" + v + "']\n")
                if data_dict[ind]['illegal_replacement_value'] is not None:
                    fo.write("        vocab += ['" + data_dict[ind]['illegal_replacement_value'] + "']\n")
                    fo.write('        n_oov = 0\n')
                else:
                    fo.write('        n_oov = 1\n')
                line = '        '
                line += "tmp_field = tf.feature_column.categorical_column_with_vocabulary_list("
                line += "'" + field_name + "', vocab, num_oov_buckets=n_oov)\n"
                fo.write(line)
        elif field_type == 'STATE':
            vocab_file = pkg_resources.resource_filename('data_reader', 'data/') + 'states.dat'
            line = '        '
            line += 'tmp_field = tf.feature_column.categorical_column_with_vocabulary_file('
            line += "'" + field_name + "', '" + vocab_file + "', default_value='XX')\n"
            fo.write(line)
        elif field_type == 'ZIP':
            vocab_file = pkg_resources.resource_filename('data_reader', 'data/') + 'zips.dat'
            line = '        '
            line += 'tmp_field = tf.feature_column.categorical_column_with_vocabulary_file('
            line += data_dict[ind]['field_name'] + ", '" + vocab_file + "', default_value='00000')\n"
            fo.write(line)
        elif field_type in ('INT', 'DATE'):
            fo.write("        if include_columns['" + field_name + "']['type'].upper() == 'FLOAT':\n")
            fo.write('            try:\n')
            fo.write("                boundaries =  include_columns['" + field_name + "']['boundaries']\n")
            line = '                '
            line += "tmp_field = tf.feature_column.numeric_column('" + field_name + "')\n"
            fo.write(line)
            line = '                '
            line += field_name + ' = tf.feature_column.bucketized_column(tmp_field, boundaries)\n'
            fo.write(line)
            fo.write('            except:\n')
            line = '                '
            line += field_name + " = tf.feature_column.numeric_column('" + field_name + "')\n"
            fo.write(line)
            fo.write('        else:\n')
            fo.write('            try:\n')
            fo.write("                vocab_list = include_columns['" + field_name + "']['vocab_list']\n")
            fo.write('                try:\n')
            fo.write("                    n_oov = include_columns['" + field_name + "']['n_oov']\n")
            fo.write('                except:\n')
            fo.write('                    n_oov = 0\n')
            line = '                '
            line += "tmp_field = tf.feature_column.categorical_column_with_vocabulary_list("
            line += "'" + field_name + "', vocabulary_list=vocab_list, num_oov_buckets=n_oov)\n"
            fo.write(line)
            fo.write('            except:\n')
            fo.write('                try:\n')
            fo.write("                    hash_size = include_columns['" + field_name + "']['hash_size']\n")
            fo.write('                except:\n')
            fo.write('                    hash_size = 1000\n')
            line = '                '
            line += "tmp_field = tf.feature_column.categorical_column_with_hash_bucket("
            line += "'" + field_name + "', hash_bucket_size=hash_size)\n"
            fo.write(line)
            fo.write('            try:\n')
            fo.write("                embed_size = include_columns['" + field_name + "']['embed_size']\n")
            fo.write('            except:\n')
            fo.write('                embed_size = 0\n')
            fo.write('            if embed_size > 0:\n')
            line = '                '
            line += field_name + ' = tf.feature_column.embedding_column(tmp_field, embed_size)\n'
            fo.write(line)
            fo.write('            else:\n')
            line = '                '
            line += field_name + ' = tf.feature_column.indicator_column(tmp_field)\n'
            fo.write(line)

        if field_type in ('STR', 'BYTES', 'STATE', 'ZIP'):
            fo.write('        try:\n')
            fo.write("            embed_size = include_columns['" + field_name + "']['embed_size']\n")
            fo.write('        except:\n')
            fo.write('            embed_size = 0\n')
            fo.write('        if embed_size > 0:\n')
            line = '            '
            line += field_name + ' = tf.feature_column.embedding_column(tmp_field, embed_size)\n'
            fo.write(line)
            fo.write('        else:\n')
            line = '            '
            line += field_name + ' = tf.feature_column.indicator_column(tmp_field)\n'
            fo.write(line)
        fo.write('        columns += [' + field_name + ']\n')
    fo.write('    return columns\n')
    fo.close()
