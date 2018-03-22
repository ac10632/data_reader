import tensorflow as tf
import math
import numpy as np
from modeling.functions import ks_calculate, decile_plot
import time
import shutil
import pkg_resources
import data_reader as d


def make_tf_fns():
    data_path = pkg_resources.resource_filename('data_reader', 'data/')
    test_data_path = pkg_resources.resource_filename('data_reader', 'test_data/')
    
    da = d.BuildDataDictionary()
    # da.add_field('originator', field_type='str', legal_values=['a','b','c'])
    # da.add_field('orignum', field_type='state')
    da.add_field('x1', field_type='float')
    da.add_field('x2', field_type='float')
    da.add_field('x3', field_type='float')
#    da.add_field('xd', field_type='str', legal_values=['now now', 'then then', 'now then', 'then now'],
#                 illegal_replacement_value='X')
    da.add_field('xd', field_type='str')
    da.add_field('x5', field_type='date', field_format='CCYYMMDD')
    da.add_field('bad', field_type='int')
    da.add_field('xe','int',minimum_value=0, maximum_value=3, minimum_replacement_value=-1, maximum_replacement_value=-1)

    from data_reader import make_input_fn
    from data_reader import make_model_columns
    
    make_input_fn(da.dictionary, '/home/will/tmp/inp.py', dep_var='bad', dates='ccyymmdd')
    make_model_columns(da.dictionary, '/home/will/tmp/col.py')


def build_estimator(model_dir, include_columns):
    columns = model_columns(include_columns)
    
    # Create a tf.estimator.RunConfig to ensure the model is run on CPU, which
    # trains faster than GPU for this model.
    run_config = tf.estimator.RunConfig().replace(
        session_config=tf.ConfigProto(device_count={'GPU': 0}))
    
    #model = tf.estimator.LinearClassifier(
    #    model_dir=model_dir, feature_columns=columns,
    #    optimizer=tf.train.FtrlOptimizer(
    #        learning_rate=0.1,
    #        l1_regularization_strength=1.0,
    #        l2_regularization_strength=1.0))

    hidden_units = [50, 25]  # [5, 4] #[50, 25]

    model = tf.estimator.DNNClassifier(
        model_dir=model_dir,
        feature_columns=columns,
        hidden_units=hidden_units,
        optimizer=tf.train.FtrlOptimizer(
            learning_rate=0.1,
            l2_regularization_strength=1.0,
            l1_regularization_strength=1.0
        )
    )
    
    return model


def read_tfr(tfrecord_filename):
    with tf.Session() as sess:
        (a, b) = input_fn(tfrecord_filename, batch_size=10)
        e = a['x5']
        r = a['xd']
        # print(tf.shape(e))
        # h = tf.reshape(e, (10,3))
        # f = tf.slice(h,[0,0], [10,1])
        n = 1
        while True:
            n += 1
            if n > 2:
                break
            try:
                x = sess.run(e)
                print('here')
                print(x)
                # print(tf.shape(x))
            except:
                break


def write_tfr(tfrecord_filename, n):
    writer = tf.python_io.TFRecordWriter(tfrecord_filename)
    
    for rows in range(0, n):
        x1 = np.random.normal(0, 1, 1)
        x2 = np.random.normal(0, 1, 1)
        x3 = np.random.normal(0, 1, 1)
        x4 = np.random.uniform(0, 1, 1)
        yr = int(np.random.uniform(0, 1, 1) * 20) + 1999
        mo = int(np.random.uniform(0, 1, 1) * 12) + 1
        day = 1
        x5 = [yr, mo, day]
        pl = -1 + x1 * 1 - x2 + 0.5 * x3 + x3 * x3 + math.sin(math.pi * x2) + x2 * x2 + x2 * x3
        
        xd = 'now now'  # 'now now', 'then then', 'now then', 'then now'
        xe = 0
        if x4 < 0.75 and x4 >= 0.5:
            xd = 'then then'
            xe = 1
            pl -= 1.0
        if x4 < 0.5 and x4 >= 0.25:
            xd = 'now then'
            xe = 2
            pl += 2.0
        if x4 < 0.25:
            xd = 'then now'
            xe = 3
            pl -= 2.0
        prob = math.exp(pl) / (1.0 + math.exp(pl))
        
        if np.random.uniform(0, 1, 1) < prob:
            bad = [1]
        else:
            bad = [0]
        # xx = [s.encode() for s in xd]
        # f4 = tf.train.Feature(bytes_list=tf.train.BytesList(value=xx))
        xf = [a.encode() for a in xd]
        xe = [xe]
        
        f1 = tf.train.Feature(float_list=tf.train.FloatList(value=x1))
        f2 = tf.train.Feature(float_list=tf.train.FloatList(value=x2))
        f3 = tf.train.Feature(float_list=tf.train.FloatList(value=x3))
        f5 = tf.train.Feature(int64_list=tf.train.Int64List(value=xe))
        f6 = tf.train.Feature(int64_list=tf.train.Int64List(value=x5))
        f4 = tf.train.Feature(bytes_list=tf.train.BytesList(value=xf))
        i1 = tf.train.Feature(int64_list=tf.train.Int64List(value=bad))
        feature = {'bad': i1, 'x1': f1, 'x2': f2, 'x3': f3, 'xd': f4, 'xe': f5, 'x5': f6}
        
        features = tf.train.Features(feature=feature)
        example = tf.train.Example(features=features)
        writer.write(example.SerializeToString())
    
    writer.close()


def bev(model_dir, data_file, include_columns, train_epochs=10, batch_size=100, build_take=1000, val_skip=0,
        val_take=1000, steps=10):
    # Clean up the model directory if present
    shutil.rmtree(model_dir, ignore_errors=True)
    model = build_estimator(model_dir, include_columns)
    
    # experiment with skip and shuffle and nobs
    # does steps in tensorboard equate?
    
    epochs_per_eval = 1
    start_time = time.time()
    for n in range(train_epochs // epochs_per_eval):
        print('training step ' + str(n))
        # steps is the number of batches to draw for the training epoch.
        # It corresponds to the 'steps' axis on TensorBoard.
        model.train(input_fn=lambda: input_fn(data_file, batch_size=batch_size, take=build_take), steps=steps)
        model.evaluate(input_fn=lambda: input_fn(data_file, batch_size=batch_size, take=build_take), steps=steps)
    
    # print(model.get_variable_names())
    for name in model.get_variable_names():
        # skip Follow The Regularized Leader values
        if name.upper().find('FTRL') < 0:
            print(name)
            print(model.get_variable_value(name))
    
    elapsed_time = time.time() - start_time
    print('elapsed time: {:.3f} seconds'.format(elapsed_time))
    
    val_batch = val_take - val_skip
    yh = list(model.predict(input_fn=lambda: input_fn(data_file, batch_size=val_batch, skip=val_skip, take=val_take)))
    print(len(yh))
    pr = np.array([x['probabilities'][1] for x in yh])
    print('train')
    with tf.Session() as sess:
        val_data_iter = input_fn(data_file, batch_size=val_batch, skip=val_skip, take=val_take)
        val_data = sess.run(
            val_data_iter)  # cd is a tuple. First entry is a dict of features, second a numpy array of labels
        y = val_data[1]
    
    print(pr.shape)
    print(pr.mean())
    
    # p tensorboard.main --logdir='/tmp/model'
    # to load labels for the projector for embeddings, create a tab separated file like:
    # index\t name
    # 0\t hi
    
    pr = pr.squeeze()
    y = y.squeeze()
    ks_calculate(pr, y, True, False)
    decile_plot(pr, y, wait=False)


tfrecord_filename = test_data_path = pkg_resources.resource_filename('data_reader', 'test_data/') + 'testxy.tfr'

model_dir = '/tmp/model'
n = 100000

make_tf_fns()
import sys

sys.path.append('/home/will/tmp')
from inp import input_fn
from col import model_columns

#write_tfr(tfrecord_filename,n)
#read_tfr(tfrecord_filename)
include_columns = {}
include_columns['x1'] = 'yes'
include_columns['x2'] = 'yes'
include_columns['x3'] = 'yes'
# add check for date part
include_columns['x5'] = {'embed_size': 0, 'hash_size': 100, 'type': 'str'}
include_columns['xd'] = {'embed_size': 0, 'type': 'str'}
bev(model_dir, tfrecord_filename, include_columns, train_epochs=10, batch_size=100, build_take=75000, val_skip=75000, val_take=100000,
    steps=1000)



