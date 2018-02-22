# -*- coding: utf-8 -*-

__author__ = "Lucas Miguel S Ponce"
__email__ = "lucasmsp@gmail.com"

from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_wait_on
import hdfs_pycompss.hdfsConnector as hdfs
import time


def getSuffixes(length):
    import itertools
    import math
    alpha = 'abcdefghijklmnopqrstuvwxyz'
    r = int(math.ceil(float(length)/len(alpha)))
    product = list(itertools.product(alpha, repeat=r))[:length]
    return sorted(["".join(x) for x in product])

def test01_mkdir_and_write():
    """Test: create a new folder and write splitted data."""
    numFrag = 4
    hdfs_ops = dict()
    hdfs_ops['host'] = 'default'
    hdfs_ops['port'] = 0
    hdfs_ops['path'] = '/test_Integration'
    hdfs.mkdir(hdfs_ops)

    suffixes = getSuffixes(numFrag)
    hdfs_ops['path'] = '/test_Integration/file'

    start = time.time()
    info = [writeData(suff, hdfs_ops) for suff in suffixes]
    info = compss_wait_on(info)
    print info
    end = time.time()
    print "Time to write: {}".format(end-start)


def test02_merge():

    hdfs_ops = dict()
    hdfs_ops['host'] = 'default'
    hdfs_ops['port'] = 0
    hdfs_ops['path'] = '/test_Integration/file\*'
    hdfs_ops['dst'] = '/test_Integration/test02_merged'
    hdfs_ops['wait'] = True
    hdfs_ops['rm'] = False  # True
    start = time.time()
    info = hdfs.mergeFiles(hdfs_ops)
    info = compss_wait_on(info)
    print info

    end = time.time()
    print "Time to merge: {}".format(end-start)


def test03_read():
    """Test: read a file in HDFS."""
    numFrag = 4
    hdfs_ops = dict()
    hdfs_ops['host'] = 'default'
    hdfs_ops['port'] = 0
    hdfs_ops['path'] = '/test_Integration/test02_merged'
    start = time.time()

    HDFS_BLOCKS = hdfs.findNBlocks(hdfs_ops, numFrag)
    print "[INFO] - Number of Blocks: {}".format(len(HDFS_BLOCKS))
    for b in HDFS_BLOCKS:
        print "[INFO] - Block: {}".format(b)

    data = [readData(block) for block in HDFS_BLOCKS]
    from pycompss.api.api import compss_wait_on
    data = compss_wait_on(data)
    end = time.time()

    for e, frag in enumerate(data):
        print "frag: #", e
        for line in frag:
            print (">"+line),

    # or:
    # for frag in data:
    #    print frag.getvalue()

    print "Time to read: {}".format(end-start)


@task(returns=list)
def readData(block):
    return hdfs.readBlock(block)


@task(returns=int)
def writeData(id_block, txt_options):
    """Create a DataFrame."""
    txt_options['path'] = '{}_{}'.format(txt_options['path'], id_block)
    msg = "Lorem ipsum dolor sit amet, "\
          "consectetur adipiscing elit.\nIn a mi eget libero"\
          "volutpat efficitur.\n"

    return hdfs.writeBlock(txt_options, msg)



if __name__ == "__main__":
    test01_mkdir_and_write()
    test02_merge()
    test03_read()
