import datetime
from datetime import date
import re
import mmap
import pkg_resources
import numpy as np
import pandas as pd
import collections as co
import os
from subprocess import call
import tensorflow as tf

# test


def parse(in_str):
    """
    This function parses the input line if the user has specified a value for *str_delim*
    This is slower than just using .split, so only incorporate if needed
    This uses the Python module csv
    param : in_str.  Line read from file being processed
    type : bytes    param : regular expression that looks for instances of str_delimited strings
    type : compiled regular expression    
    return: list of strings that were separated by the delimiter outside of str_delim
    rtype: list
    
    """
    from csv import reader as r
    for line in r([in_str.decode()]):
        return line

def to_end_of_month(dt):
    """
    Increment a date to the end of the month.
    :param dt: date to operate on
    :type dt: date
    
    :return: dt moved to last day of the month
    :rtype: date
    """
    if dt.month == 12:
        dt = datetime.date(dt.year+1,1,1)
    else:
        dt = dt.replace(day=1).replace(month=(dt.month + 1))
    dt = dt + datetime.timedelta(-1)
    return dt


def make_opf(outfile, partition=None, split_number=None):
    """
    create the output file name for output_type="delim"

    :param outfile: base filename.
    :type str
    :param partition: name of the partition subdirectory
    :type partition: str
    :param split_number: number to append to the file name for splitting files
    :type split_number: int
    """

    import re
    dot = outfile.rfind(".")
    if dot < 0:
        if outfile[-1] != "/":
            outfile += "/"
        outdir = outfile
        dotpart = ""
        namepart = ""
    else:
        slash = outfile.rfind("/")
        outdir = outfile[0:slash + 1]
        dotpart = outfile[dot:len(outfile)]
        namepart = outfile[slash + 1:dot]
    if partition is not None:
        # check to see no weird charachters
        x = re.findall('[^a-zA-Z[0-9][ \t\n\r\f\v]\\:/.=_-]', partition)
        if len(x) > 0:
            partition = "garbage"
        opf = outdir + partition + "/"
        if not os.path.isdir(opf):
            try:
                os.mkdir(opf)
            except:
                pass
    else:
        opf = outdir
    opf += namepart
    if split_number is not None:
        opf += str(split_number)
    opf += dotpart
    return opf
def reader(params):
    """
    Created by create_reader() of module data_reader.
    
    This is module specially designed to read a specific file type.
    The dictionary of parameters has the following elements:
    
    - *data_file* (str).  Name of the file to read.
    
    - *module_path* (str).  The path to this module.  If this omitted, then it is assumed that
      this module is in the reader subdirectory of the data_reader module.  The *reader* function needs
      this path so that it can read legal values from the *data* subdirectory within the *reader* 
      directory.
    
    - *output_type* (str).  How to output the data.Choices are:
    
        - list.  A list of lists where each sublist is a row of data.
        - numpy. A numpy matrix.
        - pandas. A pandas DataFrame. This is the default value.
        - delim. A delimited file.
        - tfrecords. A TensorFlow TFRecord file
    
    - *output_file* (str). The name of the output file. If "delim" is chosen, then the data_file is
      output to output_file line by line so the entire dataset is never in memory.  Not needed unless
      *output_type* = "delim".
    
    - *output_delim* (str). The delimiter for the *output_file*. Default value is ",".
    
    - *output_headers* (bool) If *True*, include headers in *output_file*. Default value is *True*.
    
    - *split_file* (int) If > 0, splits the file into sets of *split_file* rows. Must be at least 10
    
    - *partition* (str) If not None, name of field to partition the data on.
    
    - *gzip* (bool) If *True*, gzip *output_file*. Default value is *False*.
    
    - *headers* (bool).  True means the input file has headers.  The default value is *False*.
    
    - *first_row* (int). The first row of data to read.
    
    - *last_row* (int). The last row of the data to read.
    
      Note that *first_row* and *last_row* are ignored by function *multi_process*.
    
    - *start_byte* (int).  The byte at which to start reading the file.  The default value is 0.
      If the value is greater than 0, then reading begins at the next line ("\n") after *start_byte*.
    
    - *end_byte* (int). The byte at which to stop reading the file.  The default value is *None*
      (read to the end of the file).  If a non-*None* value is specified, reading stops at the first
      line whose first byte is greater than end_byte.
    
    The above two will generally only be used for reading the file in multiprocessing mode.
    
    - *sample_rate* (float). The rate at which to sample the file.  The default value is 1.
    
    - *user_function* (function). A user-supplied function that is called as each row is processed.
      It can take only one argument, a dictionary.  The dictionary entries have the form: 
      "field_name": value.  The function can modify or add values to the dictionary.
      It returns a type *bool*.  If *True*, the row is kept
    
    -*user_class* (class), *user_class_init* (dict), *user_method* (str).  A user-supplied class.
      It is initialized once and then the method is called as each row is processed.  The 
      initialization can take only keyword arguments. These are supplied in the dict *user_class_init*.
      The method is specified as a string containing the method name.
      The method returns a type *bool*.  If *True*, the row is kept
    
    - *window* (int). Window for mmap.  If *None* there is no window (fastest)
    
    :param params. A dictionary of parameters directing the reading of the file.
    :type dict
    :return list, numpy, pandas DataFrame, or None.
    
    """
    # parse the dictionary of parameters
    # if the parameter is a string, treat as file name to read and default the remaining parameters
    if str(type(params)).find("str") >= 0:
        output_type = "PANDAS"
        data_file = params
        module_path = None
        output_file = None
        start_byte = 0
        end_byte = None
        headers = False
        user_function = None
        user_class = None
        user_class_init = None
        first_row = None
        last_row = None
        output_delim = ","
        output_headers = True
        gzip = False
        split_file = None
        partition = None
        window = None
    # parse through the dictionary of parameters
    else:
        try:
            data_file = params["data_file"]
        except:
            raise ValueError("must specify data_file")
        try:
            output_type = params["output_type"].upper()
        except:
            output_type = "PANDAS"
        try:
            module_path = params["module_path"]
        except:
            module_path = None
        try:
            start_byte = params["start_byte"]
        except:
            start_byte = 0
        try:
            end_byte = params["end_byte"]
        except:
            end_byte = None
        try:
            output_file = params["output_file"]
        except:
            output_file = None
        try:
            output_delim = params["output_delim"]
        except:
            output_delim = ","
        try:
            gzip = params["gzip"]
        except:
            gzip = False
        try:
            output_headers = params["output_headers"]
        except:
            output_headers = True
        try:
            split_file = params["split_file"]
            if split_file <= 10:
                split_file = None
        except:
            split_file = None
        try:
            partition = params["partition"]
        except:
            partition = None
        try:
            headers = params["headers"]
        except:
            headers = False
        try:
            window = params["window"]
        except:
            window = None
        try:
            sample_rate = params["sample_rate"]
        except:
            sample_rate = 1
        try:
            sample_rate = float(sample_rate)
        except:
            raise ValueError("sample_rate is a float")
        if (sample_rate <= 0.0) or (sample_rate>1.0):
            raise ValueError("sample_rate is >0 and <=1")
        try:
            user_function = params["user_function"]
        except:
            user_function = None
        if (user_function is not None) and (str(type(user_function)).find("function") < 0):
            raise ValueError("user_function is not a function")
        try:
            user_class = params["user_class"]
        except:
            user_class = None
        try:
            user_class_init = params["user_class_init"]
        except:
            user_class_init = None
        try:
            user_method = params["user_method"]
        except:
            user_method = None
        try:
            first_row = params["first_row"]
        except:
            first_row = None
        if first_row is not None:
            try:
                first_row = int(first_row)
            except:
                raise ValueError("first_row must be an integer")
            if first_row < 0:
                raise ValueError("first row must be non-negative")
        try:
            last_row = params["last_row"]
        except:
            last_row = None
        if last_row is not None:
            try:
                last_row = int(last_row)
            except:
                raise ValueError("last_row must be an integer")
            if last_row <= 0:
                raise ValueError("last_row must be positive")
            if (first_row is not None) and (first_row > last_row):
                raise ValueError("last_row cannot be less than first_row")
    
    last_place = start_byte
    # initialize user_class if it has been provided
    if user_class is not None:
        try:
            if user_class_init is not None:
                uc = user_class(**user_class_init)
            else:
                uc = user_class()
            user_methodx = uc.__getattribute__(user_method)
        except:
            raise ValueError("user_class or user_method not valid")
    # regular expression to pull out strings within a delimiter
    # regular expression to separate dates with / in them
    if partition is not None:
        outfile_dict = {}
    parse_date_regexp = '([^/]+)'
    parse_date = re.compile(parse_date_regexp)
    file_count = 0
    # open the file we are going to read
    try:
        fi = open(data_file, "r" )
    except:
        raise FileNotFoundError("cannot find/open file: " + data_file)
    m = mmap.mmap(fi.fileno(), 0, access=mmap.ACCESS_READ)
    if start_byte > 0:
        st = m.find(b"\n",int(start_byte))
        offset = st+1
    else:
        offset = 0
    if end_byte is not None:
        eb = end_byte
        end_byte = m.find(b"\n", int(end_byte)) + 1
        if end_byte == 0:
            end_byte = eb
    # output_data is a list of lists that holds what we are reading (unless writing to a file)
    output_data = []
    d = b','
    if module_path is None:
        module_path = pkg_resources.resource_filename('data_reader', 'reader/')
    else:
        if module_path[-1] != '/':
            module_path += '/'
    cn = "\n"    # read in the column names
    # for a FLAT file or a DELIM file, the columns must be in this order (the order built by the user
    # A file with headers can have the columns in a different order.  Indices below then will map
    # the dictionary order to the file order
    data_filename = module_path + '/data/column_names.dat'
    try:
        f = open(data_filename,'r')
    except:
        raise FileNotFoundError('cannot find/open file: ' + data_filename)
    column_names = []
    while True:
        val = f.readline()
        val = val.strip(" ").strip("\n")
        if not val:
            break
        column_names += [val]
    f.close()
    # legal_values holds the legal values for the fields, as specified by the user.
    # legal_value files are in the same order as the fields in column_names
    # each entry of legal_values is a sorted numpy array
    legal_values = {}
    # if the file to read is type DELIM, it might have headers
    # and the columns can be in any order and there might be extra columns
    if headers:
        headers1 = m.readline().split(d)
        headers1 = [h.decode().strip("\n").strip("\r").strip(" ") for h in headers1]
        indices=[]
        for cn in column_names:
            for (ind,h) in enumerate(headers1):
                if cn == h:
                    indices += [ind]
                    break
            else:
                raise ValueError('Column ' + cn + ' not in file')
    else:
        indices = [ind for ind in range(2)]
    if start_byte > 0:
        m.seek(offset)
    # keep track of the row of the file with row_number
    row_number = 0
    # starting will be true until we find the first data row to keep
    starting = True
    # work through the file
    while True:
        # keep is True if we keep the obs
        keepx = True
        line = m.readline()
        if not line:
            break
        fx = parse(line)
        # check to see if it is worth working on this row
        if (sample_rate < 1) and (float(np.random.uniform(0,1,1)) > sample_rate):
            keepx = False
        row_number += 1
        if first_row is not None:
            keepx = keepx and (row_number >= first_row)
        if last_row is not None:
            if row_number > last_row:
                break
        if end_byte is not None:
            if m.tell() > end_byte:
                break
        if keepx:
            fx_out = co.OrderedDict()
            try:
                fx[indices[0]] = fx[indices[0]].strip('\n').strip('\r').strip(' ')
            except:
                fx[indices[0]] = ""
            fx_out[column_names[0]] = fx[indices[0]]
            try:
                fx[indices[1]] = int(float(fx[indices[1]]))
            except ValueError:
                fx[indices[1]] = None
            fx_out[column_names[1]] = fx[indices[1]]
            if keepx:
                if user_function is not None:
                    keepx = user_function(fx_out)
                if keepx and (user_class is not None):
                    keepx = user_methodx(fx_out)
                if keepx:
                    if starting:
                        out_names = list(fx_out.keys())
                        if partition is not None:
                            if partition not in fx_out.keys():
                                raise ValueError("partition variable not in output file")
                            out_names = [r for r in out_names if r != partition]
                    if output_type == 'DELIM':
                        if partition is None:
                            if starting:
                                row_count = 0
                                try:
                                    if split_file is not None:
                                        opf = make_opf(output_file, None, file_count)
                                        fo = open(opf,"w")
                                        file_count += 1
                                    else:
                                        opf = output_file
                                        fo = open(opf,"w")
                                except:
                                    raise FileNotFoundError("cannot open file: " + output_file)
                                starting = False
                                if output_headers:
                                    for (index,field) in enumerate(out_names):
                                        fo.write(field)
                                        if index < len(out_names) - 1:
                                            fo.write(output_delim)
                                        else:
                                            fo.write(cn)
                            row_count += 1
                            for (index,field) in enumerate(out_names):
                                fo.write(str(fx_out[field]))
                                if index < len(out_names)-1:
                                    fo.write(output_delim)
                                else:
                                    fo.write(cn)
                            if split_file is not None and row_count > split_file:
                                fo.close()
                                starting = True
                                if gzip:
                                    call(['gzip', opf])
                        else:
                            if fx_out[partition] in outfile_dict.keys():
                                if outfile_dict[fx_out[partition]][2] < 0:
                                    fc = outfile_dict[fx_out[partition]][3]
                                    outfile_dict[fx_out[partition]][0] = make_opf(output_file, partition + "=" + str(fx_out[partition]), fc)
                                    outfile_dict[fx_out[partition]][1] = open(outfile_dict[fx_out[partition]][0],"a")
                                    outfile_dict[fx_out[partition]][2] = 0
                                fo = outfile_dict[fx_out[partition]][1]
                            else:
                                if split_file is not None:
                                    opf = make_opf(output_file, partition + "=" + str(fx_out[partition]), 0)
                                else:
                                    opf = make_opf(output_file, partition + "=" + str(fx_out[partition]))
                                fo = open(opf,"a")
                                outfile_dict[fx_out[partition]] = [opf, fo, 0, 0]
                            outfile_dict[fx_out[partition]][2] += 1
                            for (index,field) in enumerate(out_names):
                                fo.write(str(fx_out[field]))
                                if index < len(out_names)-1:
                                    fo.write(output_delim)
                                else:
                                    fo.write(cn)
                            if split_file is not None and outfile_dict[fx_out[partition]][2] > split_file:
                                fo.close()
                                if gzip:
                                    call(['gzip', outfile_dict[fx_out[partition]][0]])
                                outfile_dict[fx_out[partition]][3] += 1
                                outfile_dict[fx_out[partition]][2] = -1
                    elif output_type == 'TFRECORDS':
                        if starting:
                            row_count = 0
                            try:
                                writer = tf.python_io.TFRecordWriter(output_file)
                            except:
                                raise FileNotFoundError("cannot open file: " + output_file)
                            starting = False
                        feature = {}
                        for (index,field) in enumerate(out_names):
                            field_type = str(type(fx_out[field]))
                            if field_type.find('float') >= 0:
                                f = tf.train.Feature(float_list=tf.train.FloatList(value=[fx_out[field]]))
                            elif field_type.find('str') >= 0 or field_type.find('zip') >= 0 or field_type.find('state') >= 0:
                                xf = [a.encode() for a in fx_out[field]]
                                f = tf.train.Feature(bytes_list=tf.train.BytesList(value=xf))
                            elif field_type.find('int') >= 0:
                                f = tf.train.Feature(int64_list=tf.train.Int64List(value=[fx_out[field]]))
                            elif field_type.find('bytes') >= 0: 
                                xf = [a for a in fx_out[field]]
                                f = tf.train.Feature(bytes_list=tf.train.BytesList(value=xf))
                            elif field_type.find('date') >= 0: 
                                dt = [fx_out[field].year, fx_out[field].month, fx_out[field].day]
                                f = tf.train.Feature(int64_list=tf.train.Int64List(value=dt))
                            feature[field] = f
                        features = tf.train.Features(feature=feature)
                        example = tf.train.Example(features=features)
                        writer.write(example.SerializeToString())
                    else:
                        fx_out = [fx_out[cc] for cc in list(fx_out.keys())]
                        output_data += [fx_out]
                    if window is not None:
                        place = m.tell()
                        if int(place / window) > int(last_place / window):
                            m.close()
                            m = mmap.mmap(fi.fileno(), 0, access=mmap.ACCESS_READ)
                            m.seek(place)
                            last_place = place
    m.close()
    fi.close()
    # select output type and we are done.
    if output_type == 'LIST':
        return output_data
    elif output_type == 'NUMPY':
        return np.matrix(output_data)
    elif output_type == 'PANDAS':
        return pd.DataFrame(output_data, columns=out_names)
    elif output_type == 'DELIM':
        if partition is None:
            fo.close()
            if gzip:
                call(['gzip', opf])
        else:
             for key in outfile_dict.keys():
                 outfile_dict[key][1].close()
                 if gzip:
                     call(['gzip', outfile_dict[key][0]])
    elif output_type == 'TFRECORDS':
        writer.close()
